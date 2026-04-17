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
                parallel_tasks INTEGER DEFAULT 1
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
            'SELECT size_threshold, local_tree_path, auto_delete, parallel_tasks '
            'FROM script_config WHERE id = 1'
        )
        return self.cursor.fetchone()

    def close(self):
        self.cursor.close()
        self.conn.close()
