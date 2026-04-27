#!/usr/bin/env python3
"""
OpenList-strm WebDAV sync engine.
Generates and maintains .strm files by syncing WebDAV directory trees.

Features:
  - Watch Mode: Background continuous monitoring for new files
  - Auto-Repair: Detect and refresh expired/broken STRM URLs
  - Incremental Sync: Only process new/changed files
  - Batch Processing: Configurable batch size with progress tracking
  - API Rate Limiting: Prevent overwhelming the alist server

Usage:
    python main.py <config_id> [--full] [--watch] [--repair] [--config-id N]
"""
import os
import sys
import time
import json
import hashlib
import requests
import logging
import random
import threading
from datetime import datetime
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
# WebDAV Client
# ============================================================================

# Global rate limiter state
_last_request_time = 0
_api_rate_limit_ms = 500
_lock = threading.Lock()


def rate_limit():
    """Enforce API rate limiting (call before each request)."""
    global _last_request_time
    with _lock:
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < (_api_rate_limit_ms / 1000.0):
            time.sleep((_api_rate_limit_ms / 1000.0) - elapsed)
        _last_request_time = time.time()


def get_jwt_token(url, username, password, logger, retries=3):
    """Fetch JWT token from alist with retry logic."""
    api_url = f"{url}/api/auth/login"
    payload = {"username": username, "password": password}

    for attempt in range(retries):
        rate_limit()
        try:
            response = requests.post(api_url, json=payload, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 200 and data.get('data', {}).get('token'):
                    return data['data']['token']
                else:
                    logger.warning(f"Get Token failed (attempt {attempt+1}/{retries}): {data.get('message')}")
            else:
                logger.warning(f"Get Token HTTP error (attempt {attempt+1}/{retries}): {response.status_code}")
        except Exception as e:
            logger.warning(f"Request Token error (attempt {attempt+1}/{retries}): {e}")

        if attempt < retries - 1:
            time.sleep(2)

    logger.error(f"Get JWT Token failed after {retries} retries")
    return None


def get_direct_url(jwt_token, file_path, logger, config_url='http://localhost:5244'):
    """Get direct download URL for a file path via alist API."""
    global _api_rate_limit_ms
    api_url = f"{config_url}/api/fs/get"
    headers = {
        "Authorization": jwt_token,
        "Content-Type": "application/json"
    }
    payload = {
        "path": file_path,
        "password": "",
        "opt": "/"
    }
    try:
        rate_limit()
        response = requests.post(api_url, json=payload, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == 200 and data.get('data', {}).get('raw_url'):
                return data['data']['raw_url']
    except Exception as e:
        logger.error(f"Failed to get direct URL: {e}")
    return None


def check_url_alive(url, logger):
    """Check if a STRM URL is still accessible (HEAD request)."""
    try:
        rate_limit()
        resp = requests.head(url, timeout=10, allow_redirects=True)
        if resp.status_code == 200:
            return True, None
        return False, f"HTTP {resp.status_code}"
    except requests.exceptions.Timeout:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)


class SimpleWebDAV:
    """Minimal WebDAV client using alist as proxy."""

    def __init__(self, base_url, username, password, config_id, logger):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.config_id = config_id
        self.logger = logger
        self.token = None
        self._authenticate()

    def _authenticate(self):
        self.token = get_jwt_token(self.base_url, self.username, self.password, self.logger)
        if not self.token:
            raise ValueError(f"Failed to authenticate with {self.base_url}")

    def list_directory(self, path):
        """List directory contents via alist directory API."""
        api_url = f"{self.base_url}/api/fs/list"
        headers = {"Authorization": self.token, "Content-Type": "application/json"}
        payload = {"path": path, "password": "", "page": 1, "per_page": 0, "refresh": False}

        rate_limit()
        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=60)
        except Exception:
            self._authenticate()
            headers["Authorization"] = self.token
            rate_limit()
            response = requests.post(api_url, json=payload, headers=headers, timeout=60)

        if response.status_code != 200:
            self.logger.error(f"List failed for {path}: {response.status_code}")
            return []

        data = response.json()
        if data.get('code') != 200:
            self._authenticate()
            headers["Authorization"] = self.token
            rate_limit()
            response = requests.post(api_url, json=payload, headers=headers, timeout=60)
            if response.status_code != 200:
                return []

        content = data.get('data', {}).get('content', [])
        return content or []

    def get_file_url(self, path):
        """Get direct URL for a file."""
        url = get_direct_url(self.token, path, self.logger, self.base_url)
        return url or f"{self.base_url}{path}"

    def get_download_url(self, path):
        """Alias for get_file_url."""
        return self.get_file_url(path)


# ============================================================================
# STRM Generation
# ============================================================================

total_download_file_counter = 0


def process_with_cache(webdav, config, script_config, config_id, size_threshold, logger,
                        min_interval, max_interval, local_tree, visited=None,
                        progress_callback=None, repair_mode=False, db_handler=None,
                        batch_size=10):
    """
    Recursively process directory tree and generate .strm files.

    Features:
      - Incremental Sync: Skip existing STRM files (only create new ones)
      - Auto-Repair: Check and refresh broken STRM URLs (repair_mode=True)
      - Batch Processing: Process in batches with configurable size
      - Progress Tracking: Report progress via callback

    Args:
        webdav: SimpleWebDAV client
        config: dict with WebDAV config
        script_config: tuple with script settings (size_threshold, local_tree_path, ...)
        config_id: int
        size_threshold: minimum file size to create strm (bytes)
        logger: logger instance
        min_interval, max_interval: download interval range (seconds)
        local_tree: path to local tree cache file
        visited: set of visited paths to avoid loops
        progress_callback: callable(state_dict) for progress updates
        repair_mode: if True, also check and repair existing STRM files
        db_handler: DBHandler instance for history tracking
        batch_size: number of files to process per batch (feature #4)
    """
    global total_download_file_counter
    if visited is None:
        visited = set()

    rootpath = config.get('rootpath', '/')
    if not rootpath.startswith('/dav/'):
        rootpath = '/dav/' + rootpath.lstrip('/')

    target_dir = config.get('target_directory', '')

    # Find video files from tree
    video_files = _find_video_files_in_tree(local_tree, rootpath, size_threshold, logger)

    # In repair mode, also find existing STRM files to check
    to_repair = []
    if repair_mode:
        to_repair = _find_existing_strms(target_dir, logger)
        logger.info(f"[Auto-Repair] Found {len(to_repair)} existing STRM files to check")

    all_items = video_files  # list of (file_path, file_size)
    total_items = len(all_items) + len(to_repair)
    processed = 0
    created = 0
    repaired = 0
    failed = 0
    deleted = 0

    if progress_callback:
        progress_callback({
            'total': total_items,
            'processed': 0,
            'created': 0,
            'repaired': 0,
            'failed': 0,
            'status': 'running'
        })

    logger.info(f"Found {len(video_files)} video files to process ({'repair mode' if repair_mode else 'normal mode'})")

    # --- Batch processing: new files ---
    batch = []
    for file_path, file_size in all_items:
        if file_path in visited:
            continue
        visited.add(file_path)

        rel_path = file_path[len(rootpath):].lstrip('/')
        strm_dir = os.path.join(target_dir, os.path.dirname(rel_path))
        strm_file = os.path.join(target_dir, rel_path.rsplit('.', 1)[0] + '.strm')

        batch.append((file_path, strm_file, strm_dir))

        # Process in batches
        if len(batch) >= batch_size:
            batch_created, batch_failed = _process_batch(
                batch, webdav, config, min_interval, max_interval, logger
            )
            created += batch_created
            failed += batch_failed
            processed += len(batch)

            if progress_callback:
                progress_callback({
                    'total': total_items,
                    'processed': processed,
                    'created': created,
                    'repaired': repaired,
                    'failed': failed,
                    'status': 'running'
                })

            if db_handler:
                db_handler.update_sync_history(
                    getattr(progress_callback, '_history_id', None),
                    files_total=total_items, files_created=created,
                    files_failed=failed, files_repaired=repaired
                )
            batch = []

    # Remaining batch
    if batch:
        batch_created, batch_failed = _process_batch(
            batch, webdav, config, min_interval, max_interval, logger
        )
        created += batch_created
        failed += batch_failed
        processed += len(batch)

    # --- Batch processing: repair existing STRMs ---
    repair_batch = []
    for strm_file in to_repair:
        repair_batch.append(strm_file)

        if len(repair_batch) >= batch_size:
            batch_repaired, batch_failed_rep = _repair_batch(
                repair_batch, webdav, config, logger
            )
            repaired += batch_repaired
            failed += batch_failed_rep
            processed += len(repair_batch)

            if progress_callback:
                progress_callback({
                    'total': total_items,
                    'processed': processed,
                    'created': created,
                    'repaired': repaired,
                    'failed': failed,
                    'status': 'running'
                })
            repair_batch = []

    if repair_batch:
        batch_repaired, batch_failed_rep = _repair_batch(
            repair_batch, webdav, config, logger
        )
        repaired += batch_repaired
        failed += batch_failed_rep
        processed += len(repair_batch)

    total_download_file_counter += created

    if progress_callback:
        progress_callback({
            'total': total_items,
            'processed': processed,
            'created': created,
            'repaired': repaired,
            'failed': failed,
            'status': 'done'
        })

    logger.info(f"Done. Created: {created}, Repaired: {repaired}, Failed: {failed}")
    return created, repaired, failed


def _process_batch(batch, webdav, config, min_interval, max_interval, logger):
    """Process a batch of new files, creating STRM entries."""
    created = 0
    failed = 0
    for file_path, strm_file, strm_dir in batch:
        os.makedirs(strm_dir, exist_ok=True)

        # Incremental: skip if already exists
        if os.path.exists(strm_file):
            continue

        download_url = webdav.get_download_url(file_path)

        if download_url:
            with open(strm_file, 'w') as f:
                f.write(download_url)
            created += 1
            logger.info(f"Created: {strm_file}")

            # Respect interval
            interval = random.uniform(min_interval, max_interval)
            time.sleep(interval)
        else:
            failed += 1
            logger.warning(f"Failed to get URL for: {file_path}")

    return created, failed


def _repair_batch(strm_files, webdav, config, logger):
    """Check and repair a batch of existing STRM files."""
    repaired = 0
    failed = 0
    for strm_file in strm_files:
        if not os.path.exists(strm_file):
            continue

        try:
            with open(strm_file, 'r') as f:
                old_url = f.read().strip()

            if not old_url:
                logger.warning(f"[Repair] Empty STRM: {strm_file}")
                continue

            # Check if URL is still alive
            alive, error = check_url_alive(old_url, logger)
            if alive:
                continue  # OK, skip

            logger.info(f"[Auto-Repair] Broken STRM detected: {strm_file} ({error})")

            # Extract original file path from the URL or strm filename
            # strm filename pattern: /path/to/video.mp4.strm
            # The strm is named after the video file
            strm_basename = os.path.basename(strm_file)  # e.g., movie.mp4.strm
            # The original video extension was replaced with .strm
            # We need to figure out the original path from the directory structure
            # Strategy: reconstruct from relative path of strm inside target_dir
            rel_path = strm_file.replace('.strm', '')  # strip extension to get potential video path
            # Actually, the strm file is stored at video_path.strm, the video is at video_path.{ext}
            # We need the original file path. Since we don't have it stored in the strm,
            # we'll try to find it from the directory structure.
            # Better approach: parse the strm filename to find the video
            video_name = os.path.basename(strm_file)[:-5]  # remove .strm
            video_dir = os.path.dirname(strm_file)
            # Try to find the actual video file extension
            video_extensions = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts', '.m2ts', '.iso']
            found_path = None
            for ext in video_extensions:
                potential = os.path.join(video_dir, video_name + ext)
                if os.path.exists(potential):
                    found_path = potential
                    break

            if not found_path:
                logger.warning(f"[Auto-Repair] Cannot find video for: {strm_file}")
                failed += 1
                continue

            # Get fresh URL
            new_url = webdav.get_download_url(found_path)
            if new_url:
                with open(strm_file, 'w') as f:
                    f.write(new_url)
                logger.info(f"[Auto-Repair] Repaired: {strm_file}")
                repaired += 1
            else:
                logger.warning(f"[Auto-Repair] Failed to get new URL for: {strm_file}")
                failed += 1

        except Exception as e:
            logger.error(f"[Auto-Repair] Error repairing {strm_file}: {e}")
            failed += 1

    return repaired, failed


def _find_video_files_in_tree(local_tree_json, rootpath, size_threshold, logger):
    """Find video files from local tree JSON cache."""
    video_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts', '.m2ts', '.iso'}
    results = []

    if not os.path.exists(local_tree_json):
        logger.warning(f"Local tree not found: {local_tree_json}")
        return results

    try:
        with open(local_tree_json, 'r') as f:
            tree = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load tree: {e}")
        return results

    def walk(node, path=''):
        if not isinstance(node, dict):
            return
        for name, content in node.items():
            node_path = f"{path}/{name}" if path else f"/{name}"
            if isinstance(content, dict) and '_is_file' not in content:
                walk(content, node_path)
            else:
                ext = os.path.splitext(name)[1].lower()
                if ext in video_extensions:
                    size = content.get('size', 0) if isinstance(content, dict) else 0
                    if size >= size_threshold:
                        results.append((node_path, size))

    walk(tree, '')
    return results


def _find_existing_strms(target_dir, logger):
    """Find all existing STRM files in target directory."""
    results = []
    if not os.path.exists(target_dir):
        return results
    for root, _, files in os.walk(target_dir):
        for f in files:
            if f.endswith('.strm'):
                results.append(os.path.join(root, f))
    return results


def _build_local_tree(webdav, rootpath, output_path, logger):
    """Recursively fetch and cache full directory tree."""
    logger.info(f"Building local tree cache at {output_path}")
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

    tree = {}

    def fetch_recursive(path, depth=0):
        if depth > 10:
            return
        items = webdav.list_directory(path)
        subtree = {}
        for item in items:
            item_path = item.get('path', '')
            item_name = os.path.basename(item_path)
            if item.get('is_dir', False):
                subtree[item_name] = {'_is_dir': True}
                if depth < 8:
                    sub = fetch_recursive(item_path, depth + 1)
                    if sub:
                        subtree[item_name] = sub
            else:
                subtree[item_name] = {'size': item.get('size', 0), '_is_file': True}
        return subtree

    tree = fetch_recursive(rootpath) or {}

    with open(output_path, 'w') as f:
        json.dump(tree, f)

    return output_path


# ============================================================================
# Watch Mode — Background Monitoring
# ============================================================================

_watch_scheduler = None
_watch_running = False
_watch_threads = {}  # config_id -> daemon thread


def _watch_loop(config_id, interval_seconds, db_handler):
    """Background watch loop for a single config."""
    global _watch_running
    import logging as _log
    _log.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    _logger = _log.getLogger(f'watch_config_{config_id}')

    while _watch_running:
        try:
            _logger.info(f"[Watch Mode] Checking config {config_id}...")
            generate_strm_for_config(config_id, full_sync=False, repair_mode=True,
                                    db_handler=db_handler, watch_callback=None)
            _logger.info(f"[Watch Mode] Config {config_id} check complete. Next in {interval_seconds}s")
        except Exception as e:
            _logger.error(f"[Watch Mode] Error for config {config_id}: {e}")

        # Sleep in increments to allow quick shutdown
        for _ in range(interval_seconds):
            if not _watch_running:
                break
            time.sleep(1)


def start_watch_mode(db_handler):
    """Start watch mode for all enabled configs."""
    global _watch_running, _watch_scheduler
    if _watch_running:
        return

    _watch_running = True
    import logging as _log
    _log.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    watch_configs = db_handler.get_all_watch_configs()
    if not watch_configs:
        _log.getLogger('watch').info("No watch configs enabled")
        return

    for wc in watch_configs:
        cid = wc['config_id']
        interval = wc.get('interval_seconds', 300)
        t = threading.Thread(target=_watch_loop, args=(cid, interval, db_handler), daemon=True)
        t.start()
        _watch_threads[cid] = t
        _log.getLogger('watch').info(f"Started watch thread for config {cid}, interval={interval}s")

    _log.getLogger('watch').info(f"Watch mode started for {len(watch_configs)} configs")


def stop_watch_mode():
    """Stop all watch mode threads."""
    global _watch_running, _watch_threads
    _watch_running = False
    for t in _watch_threads.values():
        t.join(timeout=5)
    _watch_threads.clear()


def is_watch_running():
    return _watch_running


# ============================================================================
# Progress Store (for SSE endpoint)
# ============================================================================

_progress_store = {}  # config_id -> latest progress dict


def _make_progress_callback(config_id, history_id, db_handler):
    """Factory: create a progress callback for a specific config run."""
    def callback(state):
        state['config_id'] = config_id
        state['history_id'] = history_id
        _progress_store[config_id] = state
        if db_handler and history_id:
            db_handler.update_sync_history(
                history_id,
                files_total=state.get('total'),
                files_created=state.get('created'),
                files_repaired=state.get('repaired'),
                files_failed=state.get('failed')
            )
    return callback


# ============================================================================
# Main Entry Point
# ============================================================================

def generate_strm_for_config(config_id, full_sync=False, repair_mode=False,
                               db_handler=None, watch_callback=None):
    """Main entry point: generate strm files for a given config ID."""
    global _api_rate_limit_ms

    # Setup logger
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    logger, _ = setup_logger(f'config_{config_id}', log_dir)

    # Lazy import to avoid circular
    sys.path.insert(0, os.path.dirname(__file__))
    if db_handler is None:
        from db_handler import DBHandler
        db_handler = DBHandler()

    config_row = db_handler.get_webdav_config(config_id)

    if not config_row:
        logger.error(f"Config {config_id} not found")
        return

    config = {
        'url': config_row[2],
        'username': config_row[3],
        'password': config_row[4],
        'rootpath': config_row[5],
        'target_directory': config_row[6],
    }

    script_config_row = db_handler.get_script_config()
    if not script_config_row:
        logger.error("Script config not found")
        return

    size_threshold = script_config_row[0] or 104857600  # 100MB default
    local_tree_path = script_config_row[1] or './local_tree'
    os.makedirs(local_tree_path, exist_ok=True)
    local_tree_file = os.path.join(local_tree_path, f'config_{config_id}_tree.json')

    # Feature #5: batch size from config
    batch_size = script_config_row[5] if len(script_config_row) > 5 else 10
    batch_size = max(1, min(batch_size, 100))  # clamp 1-100

    # Feature #6: API rate limit from config
    _api_rate_limit_ms = script_config_row[4] if len(script_config_row) > 4 else 500
    _api_rate_limit_ms = max(100, _api_rate_limit_ms)  # minimum 100ms

    # Build WebDAV client
    webdav = SimpleWebDAV(
        config['url'],
        config['username'],
        config['password'],
        config_id,
        logger
    )

    # Build or load tree (incremental: reuse cached tree unless full_sync)
    if full_sync or not os.path.exists(local_tree_file):
        _build_local_tree(webdav, config['rootpath'], local_tree_file, logger)
    else:
        logger.info(f"Using cached tree: {local_tree_file}")

    # Get interval range
    download_interval = config.get('download_interval_range', '1-3')
    if isinstance(download_interval, tuple):
        min_interval, max_interval = download_interval
    else:
        parts = download_interval.split('-')
        min_interval, max_interval = int(parts[0]), int(parts[1])

    # Start history record
    history_id = db_handler.start_sync_history(config_id)

    # Progress callback
    progress_cb = _make_progress_callback(config_id, history_id, db_handler)

    # Process
    created, repaired, failed = process_with_cache(
        webdav, config, script_config_row,
        config_id, size_threshold, logger,
        min_interval, max_interval,
        local_tree_file,
        progress_callback=progress_cb,
        repair_mode=repair_mode,
        db_handler=db_handler,
        batch_size=batch_size
    )

    # Finish history
    db_handler.finish_sync_history(
        history_id,
        status='done' if failed == 0 else 'partial',
        log=f"Created={created}, Repaired={repaired}, Failed={failed}"
    )

    logger.info(f"Done. Total strm files created: {created}, repaired: {repaired}, failed: {failed}")
    return created, repaired, failed


def setup_logger(name, log_dir):
    """Setup logger with file + console handlers."""
    import logging
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'{name}.log')

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger, log_file

    fh = logging.FileHandler(log_file)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger, log_file


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python main.py <config_id> [--full] [--watch] [--repair]")
        sys.exit(1)

    config_id = int(sys.argv[1])
    full_sync = '--full' in sys.argv
    repair_mode = '--repair' in sys.argv
    watch_mode = '--watch' in sys.argv

    if watch_mode:
        from db_handler import DBHandler
        db = DBHandler()
        start_watch_mode(db)
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            stop_watch_mode()
    else:
        generate_strm_for_config(config_id, full_sync=full_sync, repair_mode=repair_mode)
