#!/usr/bin/env python3.9
"""
OpenList-strm WebDAV sync engine.
Generates and maintains .strm files by syncing WebDAV directory trees.

Usage:
    python main.py <config_id> [--full]
"""
import os
import sys
import time
import json
import hashlib
import requests
import logging
from datetime import datetime

# ============================================================================
# WebDAV Client
# ============================================================================

def get_jwt_token(url, username, password, logger, retries=3):
    """Fetch JWT token from alist with retry logic."""
    import time
    api_url = f"{url}/api/auth/login"
    payload = {"username": username, "password": password}

    for attempt in range(retries):
        try:
            response = requests.post(api_url, json=payload, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if data.get('code') == 200 and data.get('data') and data['data'].get('token'):
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


def get_direct_url(jwt_token, file_path, logger):
    """Get direct download URL for a file path via alist API."""
    api_url = f"http://localhost:5244/api/fs/get"
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
        response = requests.post(api_url, json=payload, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == 200 and data.get('data', {}).get('raw_url'):
                return data['data']['raw_url']
    except Exception as e:
        logger.error(f"Failed to get direct URL: {e}")
    return None


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

        response = requests.post(api_url, json=payload, headers=headers, timeout=60)
        if response.status_code != 200:
            self.logger.error(f"List failed for {path}: {response.status_code}")
            return []

        data = response.json()
        if data.get('code') != 200:
            # Try re-auth
            self._authenticate()
            headers["Authorization"] = self.token
            response = requests.post(api_url, json=payload, headers=headers, timeout=60)
            if response.status_code != 200:
                return []

        content = data.get('data', {}).get('content', [])
        return content or []

    def get_file_url(self, path):
        """Get direct URL for a file."""
        url = get_direct_url(self.token, path, self.logger)
        return url or f"{self.base_url}{path}"

    def get_download_url(self, path):
        """Alias for get_file_url."""
        return self.get_file_url(path)


# ============================================================================
# STRM Generation
# ============================================================================

total_download_file_counter = 0


def process_with_cache(webdav, config, script_config, config_id, size_threshold, logger,
                        min_interval, max_interval, local_tree, visited=None):
    """
    Recursively process directory tree and generate .strm files.
    
    Args:
        webdav: SimpleWebDAV client
        config: dict with WebDAV config
        script_config: dict with script settings
        config_id: int
        size_threshold: minimum file size to create strm (bytes)
        logger: logger instance
        min_interval, max_interval: download interval range (seconds)
        local_tree: path to local tree cache file
        visited: set of visited paths to avoid loops
    """
    global total_download_file_counter
    if visited is None:
        visited = set()

    rootpath = config.get('rootpath', '/')
    if not rootpath.startswith('/dav/'):
        rootpath = '/dav/' + rootpath.lstrip('/')

    target_dir = config.get('target_directory', '')
    video_files = _find_video_files_in_tree(local_tree, rootpath, size_threshold, logger)

    logger.info(f"Found {len(video_files)} video files to process")

    for file_path, file_size in video_files:
        if file_path in visited:
            continue
        visited.add(file_path)

        rel_path = file_path[len(rootpath):].lstrip('/')
        strm_dir = os.path.join(target_dir, os.path.dirname(rel_path))
        strm_file = os.path.join(target_dir, rel_path.rsplit('.', 1)[0] + '.strm')

        os.makedirs(strm_dir, exist_ok=True)

        if os.path.exists(strm_file):
            continue

        download_url = webdav.get_download_url(file_path)

        if download_url:
            with open(strm_file, 'w') as f:
                f.write(download_url)
            total_download_file_counter += 1
            logger.info(f"Created: {strm_file}")

            # Respect interval
            interval = random.uniform(min_interval, max_interval)
            time.sleep(interval)


def _find_video_files_in_tree(local_tree_json, rootpath, size_threshold, logger):
    """Find video files from local tree JSON cache."""
    video_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.ts'}
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


def generate_strm_for_config(config_id, full_sync=False):
    """Main entry point: generate strm files for a given config ID."""
    import random

    # Setup logger
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    logger, _ = setup_logger(f'config_{config_id}', log_dir)

    # Load config from environment / env file
    sys.path.insert(0, os.path.dirname(__file__))
    from db_handler import DBHandler
    from logger import setup_logger

    db = DBHandler()
    config_row = db.get_webdav_config(config_id)

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

    script_config_row = db.get_script_config()
    if not script_config_row:
        logger.error("Script config not found")
        return

    size_threshold = script_config_row[0] or 104857600  # 100MB default
    local_tree_path = script_config_row[1] or './local_tree'
    os.makedirs(local_tree_path, exist_ok=True)
    local_tree_file = os.path.join(local_tree_path, f'config_{config_id}_tree.json')

    # Build WebDAV client
    webdav = SimpleWebDAV(
        config['url'],
        config['username'],
        config['password'],
        config_id,
        logger
    )

    # Build or load tree
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

    # Process and generate STRM files
    process_with_cache(
        webdav, config, script_config_row,
        config_id, size_threshold, logger,
        min_interval, max_interval,
        local_tree_file
    )

    logger.info(f"Done. Total strm files created: {total_download_file_counter}")


# ============================================================================
# CLI Entry Point
# ============================================================================

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python main.py <config_id> [--full]")
        sys.exit(1)

    config_id = int(sys.argv[1])
    full_sync = '--full' in sys.argv

    generate_strm_for_config(config_id, full_sync)
