"""
OpenList-strm Database Handler
SQLite wrapper for configuration and user management.
"""
import sqlite3
import os


class DBHandler:
    def __init__(self, db_path='data.db'):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self._init_tables()

    def _init_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS user (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS config (
                config_id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_name TEXT,
                url TEXT,
                username TEXT,
                password TEXT,
                rootpath TEXT,
                target_directory TEXT,
                download_enabled INTEGER DEFAULT 1,
                update_mode TEXT DEFAULT 'incremental',
                download_interval_range TEXT DEFAULT '1-3'
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS script_config (
                id INTEGER PRIMARY KEY,
                size_threshold INTEGER DEFAULT 104857600,
                local_tree_path TEXT DEFAULT './local_tree',
                auto_delete INTEGER DEFAULT 0,
                parallel_tasks INTEGER DEFAULT 1,
                api_rate_limit_ms INTEGER DEFAULT 500,
                batch_size INTEGER DEFAULT 10
            )
        ''')
        # Watch mode configs
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS watch_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id INTEGER UNIQUE,
                enabled INTEGER DEFAULT 0,
                interval_seconds INTEGER DEFAULT 300,
                last_check INTEGER DEFAULT 0,
                FOREIGN KEY (config_id) REFERENCES config(config_id)
            )
        ''')
        # Sync history / broken files tracking
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS sync_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id INTEGER,
                started_at INTEGER,
                finished_at INTEGER,
                files_total INTEGER DEFAULT 0,
                files_created INTEGER DEFAULT 0,
                files_deleted INTEGER DEFAULT 0,
                files_repaired INTEGER DEFAULT 0,
                files_failed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                log TEXT,
                FOREIGN KEY (config_id) REFERENCES config(config_id)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS broken_strms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id INTEGER,
                strm_path TEXT,
                error_msg TEXT,
                detected_at INTEGER,
                FOREIGN KEY (config_id) REFERENCES config(config_id)
            )
        ''')
        self.conn.commit()
        # Ensure script_config has a default row
        self.cursor.execute('SELECT COUNT(*) FROM script_config')
        if self.cursor.fetchone()[0] == 0:
            self.cursor.execute(
                'INSERT INTO script_config (id) VALUES (1)'
            )
            self.conn.commit()

    def get_user_credentials(self):
        self.cursor.execute('SELECT username, password_hash FROM user LIMIT 1')
        row = self.cursor.fetchone()
        if row:
            return row[0], row[1]
        return None, None

    def set_user_credentials(self, username, password_hash):
        self.cursor.execute('DELETE FROM user')
        self.cursor.execute(
            'INSERT INTO user (username, password_hash) VALUES (?, ?)',
            (username, password_hash)
        )
        self.conn.commit()

    def get_all_configurations(self):
        self.cursor.execute(
            'SELECT config_id, config_name, url, username, rootpath, target_directory '
            'FROM config'
        )
        return self.cursor.fetchall()

    def get_webdav_config(self, config_id):
        self.cursor.execute(
            'SELECT config_id, config_name, url, username, password, rootpath, '
            'target_directory, download_enabled, update_mode, download_interval_range '
            'FROM config WHERE config_id = ?',
            (config_id,)
        )
        return self.cursor.fetchone()

    def get_script_config(self):
        self.cursor.execute(
            'SELECT size_threshold, local_tree_path, auto_delete, parallel_tasks, '
            'api_rate_limit_ms, batch_size '
            'FROM script_config WHERE id = 1'
        )
        return self.cursor.fetchone()

    # ---- Watch Mode ----
    def get_watch_config(self, config_id):
        self.cursor.execute(
            'SELECT * FROM watch_configs WHERE config_id = ?',
            (config_id,)
        )
        return self.cursor.fetchone()

    def upsert_watch_config(self, config_id, enabled, interval_seconds):
        self.cursor.execute(
            'INSERT INTO watch_configs (config_id, enabled, interval_seconds) '
            'VALUES (?, ?, ?) '
            'ON CONFLICT(config_id) DO UPDATE SET enabled=?, interval_seconds=?',
            (config_id, enabled, interval_seconds, enabled, interval_seconds)
        )
        self.conn.commit()

    def update_watch_last_check(self, config_id, timestamp):
        self.cursor.execute(
            'UPDATE watch_configs SET last_check = ? WHERE config_id = ?',
            (timestamp, config_id)
        )
        self.conn.commit()

    def get_all_watch_configs(self):
        self.cursor.execute('SELECT * FROM watch_configs WHERE enabled = 1')
        return self.cursor.fetchall()

    # ---- Sync History ----
    def start_sync_history(self, config_id):
        import time
        started = int(time.time())
        self.cursor.execute(
            'INSERT INTO sync_history (config_id, started_at, status) VALUES (?, ?, ?)',
            (config_id, started, 'running')
        )
        self.conn.commit()
        return self.cursor.lastrowid

    def update_sync_history(self, history_id, files_total=None, files_created=None,
                            files_deleted=None, files_repaired=None, files_failed=None):
        self.cursor.execute(
            'UPDATE sync_history SET files_total=COALESCE(?,files_total),'
            'files_created=COALESCE(?,files_created),'
            'files_deleted=COALESCE(?,files_deleted),'
            'files_repaired=COALESCE(?,files_repaired),'
            'files_failed=COALESCE(?,files_failed) '
            'WHERE id=?',
            (files_total, files_created, files_deleted, files_repaired, files_failed, history_id)
        )
        self.conn.commit()

    def finish_sync_history(self, history_id, status='success', log=None):
        import time
        self.cursor.execute(
            'UPDATE sync_history SET finished_at=?, status=?, log=? WHERE id=?',
            (int(time.time()), status, log, history_id)
        )
        self.conn.commit()

    def get_recent_sync_history(self, config_id, limit=5):
        self.cursor.execute(
            'SELECT * FROM sync_history WHERE config_id=? ORDER BY id DESC LIMIT ?',
            (config_id, limit)
        )
        return self.cursor.fetchall()

    # ---- Broken STRMs ----
    def add_broken_strm(self, config_id, strm_path, error_msg):
        import time
        self.cursor.execute(
            'INSERT INTO broken_strms (config_id, strm_path, error_msg, detected_at) '
            'VALUES (?, ?, ?, ?)',
            (config_id, strm_path, error_msg, int(time.time()))
        )
        self.conn.commit()

    def clear_broken_strms(self, config_id):
        self.cursor.execute(
            'DELETE FROM broken_strms WHERE config_id = ?', (config_id,)
        )
        self.conn.commit()

    def get_broken_strms(self, config_id):
        self.cursor.execute(
            'SELECT * FROM broken_strms WHERE config_id=? ORDER BY detected_at DESC',
            (config_id,)
        )
        return self.cursor.fetchall()

    def get_all_broken_strms(self):
        self.cursor.execute(
            'SELECT b.*, c.config_name FROM broken_strms b '
            'JOIN config c ON b.config_id=c.config_id ORDER BY b.detected_at DESC'
        )
        return self.cursor.fetchall()

    def remove_broken_strm(self, broken_id):
        self.cursor.execute('DELETE FROM broken_strms WHERE id=?', (broken_id,))
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()
