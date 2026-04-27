import os
import re
import sys
import random
import glob
import json
import subprocess
import zipfile
import requests
import time
import datetime
from typing import Dict, Tuple
from flask import Flask, render_template, request, redirect, url_for, flash, send_from_directory, session, g, abort, jsonify
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from db_handler import DBHandler
from logger import setup_logger
from task_scheduler import add_tasks_to_cron, update_tasks_in_cron, delete_tasks_from_cron, list_tasks_in_cron, convert_to_cron_time, run_task_immediately
from main import (
    generate_strm_for_config, start_watch_mode, stop_watch_mode,
    is_watch_running, _progress_store
)
import threading

# Global watch control flag (per-process)
_watch_thread = None
_watch_stop_event = threading.Event()



# Initialize logger for module-level use (compatible with gunicorn workers)
logger, log_file = setup_logger('app')

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'openlist-strm-fixed-secret-key-2026')


# ===== 新功能配置 =====
# Feature 1: Watch Mode - 后台持续监控
_watch_thread = None
_watch_stop_event = threading.Event()


# ===== 暴力破解防护配置 =====
MAX_ATTEMPTS = int(os.environ.get('LOGIN_MAX_ATTEMPTS', '5'))
BAN_MINUTES = int(os.environ.get('LOGIN_BAN_MINUTES', '30'))
# 格式: { ip_or_user: { "count": N, "first_failure": timestamp } }
_login_failures = {}


def get_login_failures_key(username, ip):
    return f"{ip}:{username}"


def is_banned(username, ip):
    key = get_login_failures_key(username, ip)
    record = _login_failures.get(key)
    if not record:
        return False
    elapsed = time.time() - record['first_failure']
    if elapsed > BAN_MINUTES * 60:
        # 已过封禁期，清除记录
        _login_failures.pop(key, None)
        return False
    remaining = int(BAN_MINUTES * 60 - elapsed)
    return remaining > 0


def get_ban_remaining_seconds(username, ip):
    key = get_login_failures_key(username, ip)
    record = _login_failures.get(key)
    if not record:
        return 0
    elapsed = time.time() - record['first_failure']
    remaining = BAN_MINUTES * 60 - elapsed
    return max(0, int(remaining))


def record_failed_login(username, ip):
    key = get_login_failures_key(username, ip)
    record = _login_failures.get(key, {'count': 0, 'first_failure': time.time()})
    record['count'] += 1
    record['first_failure'] = time.time()
    _login_failures[key] = record


def clear_failed_logins(username, ip):
    key = get_login_failures_key(username, ip)
    _login_failures.pop(key, None)


# 定义图片文件夹路径
IMAGE_FOLDER = 'static/images'


db_handler = DBHandler()


# ---- Jinja2 template filters ----
import datetime as _dt

@app.template_filter('timestamp_to_str')
def _ts(ts):
    """Convert Unix timestamp to readable string."""
    if not ts:
        return '-'
    try:
        return _dt.datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return str(ts)




CRON_BACKUP_FILE = "/config/cron.bak"
ENV_FILE = "/app/config/app.env"

# Version (read from environment, defaults to '6.0.9-fixed')
VERSION = os.environ.get('APP_VERSION', '6.0.9-fixed')






@app.before_request
def check_user_config():
    # 跳过以下端点的检查
    if request.endpoint in ['login', 'register', 'static', 'random_image', 'forgot_password']:
        return

    # 确保 user_config 表中有用户名和密码
    username, password = db_handler.get_user_credentials()
    if not username or not password:
        # 重定向到注册页面
        return redirect(url_for('register'))

    # 检查用户是否已登录
    if 'logged_in' not in session:
        return redirect(url_for('login'))

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            flash('请先登录', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        # 获取表单数据
        username = request.form['username']
        password = request.form['password']
        # 对密码进行哈希处理
        password_hash = generate_password_hash(password)

        # 存储用户凭证
        db_handler.set_user_credentials(username, password_hash)

        flash('注册成功，请登录', 'success')
        return redirect(url_for('login'))

    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    client_ip = request.remote_addr or request.headers.get('X-Forwarded-For', 'unknown').split(',')[0].strip()

    if request.method == 'POST':
        # 获取表单数据
        username = request.form['username']
        password = request.form['password']

        # 获取存储的用户凭证
        stored_username, stored_password_hash = db_handler.get_user_credentials()

        # —— 暴力破解防护检查 ——
        if is_banned(username, client_ip):
            remaining = get_ban_remaining_seconds(username, client_ip)
            flash(f'登录失败次数过多，请 {remaining} 秒后重试', 'error')
            return render_template('login.html')

        # 检查用户名和密码
        if username == stored_username and check_password_hash(stored_password_hash, password):
            # 登录成功，清除失败记录
            clear_failed_logins(username, client_ip)
            session['logged_in'] = True
            session['username'] = username
            flash('登录成功', 'success')
            return redirect(url_for('index'))
        else:
            # 登录失败，记录
            record_failed_login(username, client_ip)
            remaining = get_ban_remaining_seconds(username, client_ip)
            if remaining > 0:
                flash(f'登录失败次数过多，请 {remaining} 秒后重试', 'error')
            else:
                flash('用户名或密码错误', 'error')

    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('您已退出登录', 'success')
    return redirect(url_for('login'))


# ===== 紧急密码重置 =====
EMERGENCY_RESET_CODE = os.environ.get('EMERGENCY_RESET_CODE', '')
_logger = setup_logger(__name__)
MAX_EMERGENCY_RESET_ATTEMPTS = 3       # 重置码输错3次后封禁
EMERGENCY_RESET_BAN_HOURS = 1         # 封禁时长1小时
_emergency_reset_attempts: Dict[str, Tuple[int, float]] = {}  # ip → (失败次数, 首次失败时间戳)


def is_emergency_reset_banned(ip: str) -> bool:
    """检查IP是否被禁止使用紧急重置"""
    if ip not in _emergency_reset_attempts:
        return False
    count, first_attempt = _emergency_reset_attempts[ip]
    if count < MAX_EMERGENCY_RESET_ATTEMPTS:
        return False
    elapsed = time.time() - first_attempt
    if elapsed > EMERGENCY_RESET_BAN_HOURS * 3600:
        del _emergency_reset_attempts[ip]
        return False
    return True


def get_emergency_reset_ban_remaining(ip: str) -> int:
    """返回剩余封禁秒数"""
    if ip not in _emergency_reset_attempts:
        return 0
    count, first_attempt = _emergency_reset_attempts[ip]
    elapsed = time.time() - first_attempt
    remaining = EMERGENCY_RESET_BAN_HOURS * 3600 - elapsed
    return max(0, int(remaining))


def record_emergency_reset_attempt(ip: str):
    """记录一次重置码失败"""
    if ip not in _emergency_reset_attempts:
        _emergency_reset_attempts[ip] = (1, time.time())
    else:
        count, first_attempt = _emergency_reset_attempts[ip]
        _emergency_reset_attempts[ip] = (count + 1, first_attempt)


def clear_emergency_reset_attempts(ip: str):
    """清除重置失败记录（成功后调用）"""
    if ip in _emergency_reset_attempts:
        del _emergency_reset_attempts[ip]


@app.route('/emergency-reset', methods=['GET', 'POST'])
def emergency_reset():
    """紧急密码重置：需要正确的 EMERGENCY_RESET_CODE 环境变量才能使用"""
    client_ip = request.remote_addr or request.headers.get('X-Forwarded-For', 'unknown').split(',')[0].strip()

    # —— 防暴力：检查 IP 是否被封禁 ——
    if is_emergency_reset_banned(client_ip):
        remaining = get_emergency_reset_ban_remaining(client_ip)
        minutes = remaining // 60 + 1
        flash(f'操作过于频繁，请 {minutes} 分钟后再试', 'error')
        return redirect(url_for('login'))

    if not EMERGENCY_RESET_CODE:
        flash('紧急重置功能未启用（未设置 EMERGENCY_RESET_CODE 环境变量）', 'error')
        return redirect(url_for('login'))

    if request.method == 'POST':
        code = request.form.get('code', '').strip()
        new_password = request.form.get('new_password', '').strip()
        confirm_password = request.form.get('confirm_password', '').strip()

        if code != EMERGENCY_RESET_CODE:
            record_emergency_reset_attempt(client_ip)
            remaining = get_emergency_reset_ban_remaining(client_ip)
            _logger.warning(f'[EMERGENCY_RESET] 错误重置码，IP: {client_ip}，剩余尝试: {MAX_EMERGENCY_RESET_ATTEMPTS - (_emergency_reset_attempts.get(client_ip, (0, 0))[0])}')
            if is_emergency_reset_banned(client_ip):
                minutes = get_emergency_reset_ban_remaining(client_ip) // 60 + 1
                flash(f'输错次数过多，请 {minutes} 分钟后再试', 'error')
                return redirect(url_for('login'))
            flash('重置码错误', 'error')
            return render_template('emergency_reset.html')

        if not new_password or len(new_password) < 4:
            flash('新密码长度至少 4 位', 'error')
            return render_template('emergency_reset.html')

        if new_password != confirm_password:
            flash('两次输入的密码不一致', 'error')
            return render_template('emergency_reset.html')

        # 设置新密码
        clear_emergency_reset_attempts(client_ip)
        password_hash = generate_password_hash(new_password)
        db_handler.set_user_credentials(None, password_hash)

        _logger.warning(
            f'[EMERGENCY_RESET] 密码重置成功，IP: {client_ip}，'
            f'时间: {datetime.datetime.now().isoformat()}'
        )
        flash('密码已重置，请使用新密码登录', 'success')
        return redirect(url_for('login'))

    return render_template('emergency_reset.html')


# 首页
@app.route('/')
@login_required
def index():
    invalid_file_trees = []
    invalid_tree_dir = 'invalid_file_trees'

    if os.path.exists(invalid_tree_dir):
        for json_file in os.listdir(invalid_tree_dir):
            if json_file.endswith('.json'):
                with open(os.path.join(invalid_tree_dir, json_file), 'r', encoding='utf-8') as file:
                    invalid_file_trees.append({
                        'name': json_file,  # 保留完整的文件名，包括 .json
                        'structure': json.load(file)
                    })

    return render_template('home.html', invalid_file_trees=invalid_file_trees)

@app.route('/view_invalid_directory/<path:directory_name>', methods=['GET'])
def view_invalid_directory(directory_name):
    try:
        invalid_file_tree_path = os.path.join('invalid_file_trees', f'{directory_name}.json')
        if not os.path.exists(invalid_file_tree_path):
            return jsonify({"error": "未找到目录树"}), 404

        with open(invalid_file_tree_path, 'r', encoding='utf-8') as file:
            directory_structure = json.load(file)

        # 将目录树返回给前端
        return jsonify({"structure": json.dumps(directory_structure, ensure_ascii=False, indent=4)})
    except Exception as e:
        logger.error(f"查看目录树时出错: {e}")
        return jsonify({"error": "查看目录树时出错"}), 500

def get_target_directory_by_config_id(config_id):
    """
    根据 config_id 从数据库获取 target_directory
    """
    config = db_handler.get_webdav_config(config_id)
    if config:
        return config['target_directory']
    return None

@app.route('/delete_invalid_directory/<path:json_filename>', methods=['POST'])
def delete_invalid_directory(json_filename):
    try:
        # 确保文件名以 'invalid_file_trees_' 开头并以 '.json' 结尾
        if not json_filename.startswith('invalid_file_trees_') or not json_filename.endswith('.json'):
            return jsonify({"error": "无效的文件名"}), 400

        # 从文件名中提取 config_id
        config_id_str = json_filename.replace('invalid_file_trees_', '').replace('.json', '')
        if not config_id_str.isdigit():
            return jsonify({"error": "无效的配置 ID"}), 400

        config_id = int(config_id_str)

        # 从数据库中获取 target_directory
        target_directory = get_target_directory_by_config_id(config_id)
        if not target_directory:
            return jsonify({"error": "未找到对应的配置"}), 404

        # 构建 JSON 文件的路径
        json_file_path = os.path.join('invalid_file_trees', json_filename)
        if not os.path.exists(json_file_path):
            return jsonify({"error": "未找到指定的 JSON 文件"}), 404

        # 读取 JSON 文件，获取目录树
        with open(json_file_path, 'r', encoding='utf-8') as file:
            directory_tree = json.load(file)

        # 遍历目录树，删除所有列出的 .strm 文件
        def delete_strm_files(base_path, tree):
            for name, content in tree.items():
                current_path = os.path.join(base_path, name)
                if isinstance(content, dict):
                    # 如果是目录，递归遍历
                    delete_strm_files(current_path, content)
                    # 删除空目录
                    if os.path.exists(current_path) and not os.listdir(current_path):
                        os.rmdir(current_path)
                        logger.info(f"删除空目录: {current_path}")
                elif content == "invalid" and name.endswith('.strm'):
                    # 删除文件
                    if os.path.exists(current_path):
                        os.remove(current_path)
                        logger.info(f"删除文件: {current_path}")

        # 开始删除
        delete_strm_files(target_directory, directory_tree)

        # 删除对应的失效目录树 JSON 文件
        os.remove(json_file_path)
        logger.info(f"删除失效目录树 JSON 文件: {json_file_path}")

        flash('目录及其 .strm 文件已成功删除！', 'success')
        return jsonify({"message": "目录和失效目录树已成功删除"}), 200

    except Exception as e:
        logger.error(f"删除目录时出错: {e}")
        return jsonify({"error": "删除目录时出错"}), 500

@app.route('/invalid_file_trees')
def invalid_file_trees():
    invalid_file_trees = []
    invalid_tree_dir = 'invalid_file_trees'

    if os.path.exists(invalid_tree_dir):
        for json_file in os.listdir(invalid_tree_dir):
            if json_file.endswith('.json'):
                invalid_file_trees.append({
                    'name': json_file,  # 保留完整的文件名，包括 .json
                })

    return render_template('invalid_file_trees.html', invalid_file_trees=invalid_file_trees)

@app.route('/get_invalid_file_tree/<path:json_filename>', methods=['GET'])
def get_invalid_file_tree(json_filename):
    try:
        # 构建 JSON 文件的路径
        json_file_path = os.path.join('invalid_file_trees', json_filename)
        if not os.path.exists(json_file_path):
            return jsonify({"error": "未找到指定的 JSON 文件"}), 404

        # 读取 JSON 文件，获取目录树
        with open(json_file_path, 'r', encoding='utf-8') as file:
            directory_tree = json.load(file)

        # 返回目录树结构
        return jsonify({"structure": directory_tree}), 200

    except Exception as e:
        logger.error(f"获取目录树时出错: {e}")
        return jsonify({"error": "获取目录树时出错"}), 500


# 配置文件页面
@app.route('/configs')
@login_required
def configs():
    try:
        # 查询数据库
        db_handler.cursor.execute("SELECT config_id, config_name, url, username, rootpath, target_directory FROM config")
        configs = db_handler.cursor.fetchall()

        # 调试输出
        print(f"从数据库中读取的配置: {configs}")

        return render_template('configs.html', configs=configs)
    except Exception as e:
        flash(f"加载配置时出错: {e}", 'error')
        return render_template('configs.html', configs=[])

@app.route('/random_image')
def random_image():
    # 获取目录中的所有图片文件
    images = os.listdir(IMAGE_FOLDER)
    # 随机选择一张图片
    random_image = random.choice(images)
    # 返回该图片
    return send_from_directory(IMAGE_FOLDER, random_image)

@app.before_request
def before_request():
    g.local_version = VERSION  # 动态获取版本号的逻辑


@app.route('/edit/<int:config_id>', methods=['GET', 'POST'])
def edit_config(config_id):
    try:
        if request.method == 'POST':
            # 打印表单数据，调试用途
            print(f"收到的表单数据: {request.form}")

            config_name = request.form['config_name']
            url = request.form['url']
            username = request.form['username']
            password = request.form['password']
            rootpath = request.form['rootpath']
            target_directory = request.form['target_directory']
            download_interval_range = request.form.get('download_interval_range', '1-3')  # 保持为字符串
            download_enabled = int(request.form.get('download_enabled', 0))  # 获取是否启用下载功能，默认0（禁用）

            # 前端验证已经做过，这里做后端验证
            if not validate_download_interval_range(download_interval_range):
                flash("下载间隔范围无效。请使用 'min-max' 格式，且 min <= max。", 'error')
                return redirect(url_for('new_config'))

            # 自动为 rootpath 添加 /dav/ 前缀（如果没有）
            if not rootpath.startswith('/dav/'):
                rootpath = '/dav/' + rootpath.lstrip('/')

            # 更新配置，包括下载启用状态、更新模式和大小阈值
            db_handler.cursor.execute('''
                UPDATE config SET 
                    config_name = ?, url = ?, username = ?, password = ?,
                    rootpath = ?, target_directory = ?,
                    download_interval_range = ?, download_enabled = ?
                WHERE config_id = ?
            ''', (config_name, url, username, password, rootpath, target_directory,
                  download_interval_range, download_enabled, config_id))
            db_handler.conn.commit()

            flash('配置已成功更新！', 'success')
            return redirect(url_for('configs'))

        # GET 请求时，获取并显示现有的配置项
        db_handler.cursor.execute('''
            SELECT * FROM config 
            WHERE config_id = ?
        ''', (config_id,))
        config = db_handler.cursor.fetchone()

        if config and config[8] is None:
            config = list(config)  # 转换为列表以进行修改
            config[8] = '1-3'  # 默认值为字符串 '1-3'

        return render_template('edit_config.html', config=config)
    except Exception as e:
        flash(f"编辑配置时出错: {e}", 'error')
        return redirect(url_for('configs'))







@app.route('/new', methods=['GET', 'POST'])
def new_config():
    if request.method == 'POST':
        try:
            # 从表单中获取用户输入的数据
            config_name = request.form['config_name']
            url = request.form['url']
            username = request.form['username']
            password = request.form['password']
            rootpath = request.form['rootpath']
            target_directory = request.form['target_directory']
            download_interval_range = request.form.get('download_interval_range', '1-3')  # 保持为字符串
            download_enabled = int(request.form.get('download_enabled', 0))  # 获取是否启用下载功能，默认0（禁用）

            # 前端验证已经做过，这里做后端验证
            if not validate_download_interval_range(download_interval_range):
                flash("下载间隔范围无效。请使用 'min-max' 格式，且 min <= max。", 'error')
                return redirect(url_for('new_config'))

            # 自动为 rootpath 添加 /dav/ 前缀（如果没有）
            if not rootpath.startswith('/dav/'):
                rootpath = '/dav/' + rootpath.lstrip('/')

            # 插入新配置到数据库，确保所有字段都被插入
            db_handler.cursor.execute('''
                INSERT INTO config (config_name, url, username, password, rootpath, target_directory, download_enabled, download_interval_range)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (config_name, url, username, password, rootpath, target_directory, download_enabled, download_interval_range))
            db_handler.conn.commit()

            flash('新配置已成功添加！', 'success')
            return redirect(url_for('configs'))
        except Exception as e:
            flash(f"添加新配置时出错: {e}", 'error')

    return render_template('new_config.html')




@app.route('/copy_config/<int:config_id>', methods=['GET'])
def copy_config(config_id):
    try:
        # 查询要复制的配置
        db_handler.cursor.execute('SELECT config_name, url, username, password, rootpath, target_directory, download_enabled, update_mode, download_interval_range FROM config WHERE config_id = ?', (config_id,))
        config = db_handler.cursor.fetchone()

        if not config:
            flash(f"未找到配置 ID 为 {config_id} 的配置文件。", 'error')
            return render_template('404.html'), 404  # 返回404页面

        # 生成新名称，确保唯一性
        new_name = config[0] + " - 复制"

        db_handler.cursor.execute('''
            INSERT INTO config (config_name, url, username, password, rootpath, target_directory, download_enabled, download_interval_range)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (new_name, config[1], config[2], config[3], config[4], config[5], config[6], config[8]))

        # 提交事务
        db_handler.conn.commit()

        # 添加日志输出，确认插入成功
        print(f"新配置已插入数据库: {new_name}")
        flash(f"配置已成功复制！", 'success')

    except Exception as e:
        flash(f"复制配置时出错: {e}", 'error')
        return render_template('500.html'), 500  # 返回500错误

    return redirect(url_for('configs'))



@app.route('/delete/<int:config_id>', methods=['GET', 'POST'])
def delete_config(config_id):
    try:
        db_handler.cursor.execute("DELETE FROM config WHERE config_id = ?", (config_id,))
        db_handler.conn.commit()
        flash('配置已成功删除！', 'success')
    except Exception as e:
        flash(f"删除配置时出错: {e}", 'error')
        return render_template('500.html'), 500  # 返回500错误

    return redirect(url_for('configs'))


def validate_download_interval_range(interval_range):
    pattern = re.compile(r'^(\d+)-(\d+)$')
    match = pattern.match(interval_range)
    if not match:
        return False
    min_val, max_val = int(match.group(1)), int(match.group(2))
    return min_val <= max_val


# 设置页面 (增强版：含批量大小 & API限速配置)
@app.route('/settings', methods=['GET', 'POST'])
def settings():
    if request.method == 'POST':
        try:
            video_formats = request.form.get('video_formats', 'mp4,mkv,avi,mov,flv,wmv,ts,m2ts,iso')
            subtitle_formats = request.form.get('subtitle_formats', 'srt,ass,sub')
            image_formats = request.form.get('image_formats', 'jpg,png,bmp')
            metadata_formats = request.form.get('metadata_formats', 'nfo')
            size_threshold = int(request.form.get('size_threshold', 50)) * 1024 * 1024
            auto_delete = int(request.form.get('auto_delete', 0))
            parallel_tasks = int(request.form.get('parallel_tasks', 1))
            batch_size = int(request.form.get('batch_size', 10))
            api_rate_limit_ms = int(request.form.get('api_rate_limit_ms', 500))
            # Validate
            batch_size = max(1, min(batch_size, 100))
            api_rate_limit_ms = max(100, min(api_rate_limit_ms, 5000))
            db_handler.cursor.execute('''
                UPDATE script_config SET
                    size_threshold=?, auto_delete=?, parallel_tasks=?,
                    batch_size=?, api_rate_limit_ms=?
                WHERE id=1
            ''', (size_threshold, auto_delete, parallel_tasks, batch_size, api_rate_limit_ms))
            db_handler.conn.commit()
            flash('设置已保存', 'success')
        except Exception as e:
            flash(f'保存设置失败: {e}', 'error')
        return redirect(url_for('settings'))
    script_config = db_handler.get_script_config()
    if script_config:
        script_config = dict(script_config)
    else:
        script_config = {}
    return render_template('settings.html', script_config=script_config)


@app.route('/logs/<int:config_id>')
def logs(config_id):
    log_dir = os.path.join(os.getcwd(), 'logs')

    # 获取指定 config_id 的所有日志文件（以 config_id 为前缀）
    log_files = [f for f in os.listdir(log_dir) if f.startswith(f'config_{config_id}') and f.endswith('.log')]

    if not log_files:
        # 如果没有找到相关日志文件，返回 404 错误
        abort(404, description=f"没有找到与配置 ID {config_id} 相关的日志文件")

    # 按修改时间倒序排列，获取最新的日志文件
    latest_log_file = max(log_files, key=lambda f: os.path.getmtime(os.path.join(log_dir, f)))
    log_file_path = os.path.join(log_dir, latest_log_file)

    # 分页参数
    page = int(request.args.get('page', 1))  # 获取当前页码，默认为第一页
    per_page = 100  # 每页显示100行日志
    start = (page - 1) * per_page
    end = start + per_page

    # 读取日志文件并按行倒序排列
    with open(log_file_path, 'r', encoding='utf-8') as log_file:
        log_lines = log_file.readlines()

    # 计算总页数
    total_lines = len(log_lines)
    total_pages = (total_lines // per_page) + (1 if total_lines % per_page > 0 else 0)

    # 确保页码合法
    if page < 1:
        page = 1
    elif page > total_pages:
        page = total_pages

    # 获取当前页的日志内容，并反转顺序（最新的日志行在顶部）
    current_page_lines = log_lines[start:end][::-1]

    # 将当前页面的日志行转换成字符串，确保每行用 <br> 换行
    log_content = '<br>'.join(current_page_lines)

    # 渲染模板并传递分页信息
    return render_template(
        'logs_single.html',
        log_content=log_content,
        config_id=config_id,
        page=page,
        total_pages=total_pages
    )


def _run_config_impl(config_id):
    """内部实现：启动 main.py 生成 strm（无返回值）"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    main_script_path = os.path.join(current_dir, 'main.py')
    if os.path.exists(main_script_path):
        command = f"/usr/local/bin/python3.9 {main_script_path} {config_id}"
        logger.info(f"手动运行配置ID: {config_id}")
        subprocess.Popen(command, shell=True)
        return True
    else:
        logger.error(f"无法找到 main.py: {main_script_path}")
        return False

@app.route('/run_config/<int:config_id>')
def run_config(config_id):
    """手动运行指定配置（配置文件页立即执行按钮）"""
    ok = _run_config_impl(config_id)
    if ok:
        flash(f'配置已开始后台运行，可在日志页查看进度', 'success')
    else:
        flash('无法找到 main.py 文件', 'error')
    return redirect(url_for('configs'))

@app.route('/run_selected_configs', methods=['POST'])
def run_selected_configs():
    selected_configs = request.form.getlist('selected_configs')
    action = request.form.get('action')

    if not selected_configs:
        flash('请选择至少一个配置', 'error')
        return redirect(url_for('configs'))

    if action == 'copy_selected':
        # 处理复制选定配置
        for config_id in selected_configs:
            copy_config(int(config_id))  # 你可以直接调用之前定义的 `copy_config` 函数
        flash('选定的配置已成功复制！', 'success')

    elif action == 'delete_selected':
        # 处理删除选定配置
        for config_id in selected_configs:
            db_handler.cursor.execute('DELETE FROM config WHERE config_id = ?', (config_id,))
        db_handler.conn.commit()
        flash('选定的配置已成功删除！', 'success')

    elif action == 'run_selected':
        for config_id in selected_configs:
            _run_config_impl(int(config_id))  # 调用 `run_config` 函数来运行 main.py
        flash('选定的配置已开始运行！', 'success')

    return redirect(url_for('configs'))

@app.route('/scheduled_tasks')
def scheduled_tasks():
    try:
        # 从定时任务模块中获取所有定时任务
        tasks = list_tasks_in_cron()  # 调用 task_scheduler.py 的 list_tasks_in_cron 方法
        return render_template('scheduled_tasks.html', tasks=tasks)
    except Exception as e:
        flash(f'获取定时任务时出错: {e}', 'error')
        return redirect(url_for('index'))
@app.route('/new_task', methods=['GET', 'POST'])
def new_task():
    if request.method == 'POST':
        task_name = request.form['task_name']
        config_ids = request.form.getlist('config_ids')  # 获取选择的配置文件 ID，列表形式
        interval_type = request.form['interval_type']
        interval_value = request.form['interval_value']
        task_mode = request.form['task_mode']
        is_enabled = request.form['is_enabled'] == '1'  # 将字符串转换为布尔值

        # 验证间隔值
        try:
            interval_value_int = int(interval_value)
            if interval_type == 'minute' and not (1 <= interval_value_int <= 59):
                raise ValueError('分钟间隔值必须在 1 到 59 之间')
            elif interval_type == 'hourly' and not (1 <= interval_value_int <= 23):
                raise ValueError('小时间隔值必须在 1 到 23 之间')
            elif interval_type == 'daily' and not (1 <= interval_value_int <= 31):
                raise ValueError('天数间隔值必须在 1 到 31 之间')
            elif interval_type == 'weekly' and not (0 <= interval_value_int <= 6):
                raise ValueError('星期值必须在 0（周日）到 6（周六）之间')
            elif interval_type == 'monthly' and not (1 <= interval_value_int <= 12):
                raise ValueError('月份间隔值必须在 1 到 12 之间')
        except ValueError as ve:
            flash(str(ve), 'error')
            return redirect(url_for('new_task'))

        # 将间隔类型和间隔值转换为 cron 时间格式
        cron_time = convert_to_cron_time(interval_type, interval_value)

        # 调用定时任务模块的函数添加任务
        task_ids = add_tasks_to_cron(
            task_name=task_name,
            cron_time=cron_time,
            config_ids=config_ids,
            task_mode=task_mode,
            is_enabled=is_enabled
        )

        flash('任务已成功添加！', 'success')
        return redirect(url_for('scheduled_tasks'))

    # 从数据库中读取配置文件列表
    configs = db_handler.get_all_configurations()
    return render_template('new_task.html', configs=configs)

def update_task(task_id):
    if request.method == 'POST':
        task_name = request.form['task_name']
        config_ids = request.form.getlist('config_ids')
        interval_type = request.form['interval_type']
        interval_value = request.form['interval_value']
        task_mode = request.form['task_mode']
        is_enabled = request.form['is_enabled'] == '1'

        # 验证间隔值
        try:
            interval_value_int = int(interval_value)
            if interval_type == 'minute' and not (1 <= interval_value_int <= 59):
                raise ValueError('分钟间隔值必须在 1 到 59 之间')
            elif interval_type == 'hourly' and not (1 <= interval_value_int <= 23):
                raise ValueError('小时间隔值必须在 1 到 23 之间')
            elif interval_type == 'daily' and not (1 <= interval_value_int <= 31):
                raise ValueError('天数间隔值必须在 1 到 31 之间')
            elif interval_type == 'weekly' and not (0 <= interval_value_int <= 6):
                raise ValueError('星期值必须在 0（周日）到 6（周六）之间')
            elif interval_type == 'monthly' and not (1 <= interval_value_int <= 12):
                raise ValueError('月份间隔值必须在 1 到 12 之间')
        except ValueError as ve:
            flash(str(ve), 'error')
            return redirect(url_for('update_task', task_id=task_id))

        # 将间隔类型和间隔值转换为 cron 时间格式
        cron_time = convert_to_cron_time(interval_type, interval_value)

        # 更新任务信息
        update_tasks_in_cron(
            task_ids=[task_id],
            cron_time=cron_time,
            config_ids=config_ids,
            task_mode=task_mode,
            task_name=task_name,
            is_enabled=is_enabled
        )

        flash('任务已成功更新！', 'success')
        return redirect(url_for('scheduled_tasks'))

    # GET 请求时，加载任务信息
    tasks = list_tasks_in_cron()  # 调用 task_scheduler.py 的 list_tasks_in_cron 方法
    task = next((t for t in tasks if t.get('task_id') == task_id), None)
    configs = db_handler.get_all_configurations()

    if not task:
        flash('未找到指定的任务', 'error')
        return redirect(url_for('scheduled_tasks'))

    # 获取已有的配置文件 ID，并确保它们是字符串
    selected_config_ids = [str(task.get('config_id'))]
    app.logger.debug(f"Selected Config IDs: {selected_config_ids}")

    return render_template('edit_task.html', task=task, configs=configs, selected_config_ids=selected_config_ids)


@app.route('/delete_task/<task_id>', methods=['POST'])
def delete_task(task_id):
    try:
        # 删除定时任务
        delete_tasks_from_cron([task_id])  # 调用 task_scheduler.py 的 delete_tasks_from_cron 方法

        flash('任务已成功删除！', 'success')
    except Exception as e:
        flash(f"删除任务时出错: {e}", 'error')
        print(f"删除任务时出现错误: {e}")

    return redirect(url_for('scheduled_tasks'))

@app.route('/delete_selected_tasks', methods=['POST'])
def delete_selected_tasks():
    try:
        data = request.get_json()
        task_ids = data.get('task_ids', [])
        if not task_ids:
            return jsonify({'success': False, 'error': '未提供任务ID'})

        delete_tasks_from_cron(task_ids)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/view_logs/<task_id>')
def view_logs(task_id):
    # 日志文件的路径
    log_dir = os.path.join(os.getcwd(), 'logs')
    # 构建日志文件的搜索模式
    log_pattern = os.path.join(log_dir, f'task_{task_id}_*.log')
    log_files = glob.glob(log_pattern)

    if log_files:
        # 按照文件修改时间排序，最新的文件排在第一个
        log_files.sort(key=os.path.getmtime, reverse=True)

        # 只读取最新的日志文件
        latest_log_file = log_files[0]
        with open(latest_log_file, 'r', encoding='utf-8') as f:
            content = f.read()
        log_contents = [{
            'filename': os.path.basename(latest_log_file),
            'content': content
        }]
    else:
        log_contents = None

    return render_template('view_logs.html', log_contents=log_contents, task_id=task_id)


def restart_app():
    print("重启应用...")
    try:
        subprocess.run(['supervisorctl', 'restart', 'flask'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"重启失败: {e}")

def download_and_extract(url, extract_to='.'):
    try:
        # 下载文件
        local_filename = url.split('/')[-1]
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        # 解压缩文件
        if local_filename.endswith('.zip'):
            with zipfile.ZipFile(local_filename, 'r') as zip_ref:
                zip_ref.extractall(extract_to)

        # 删除压缩包
        os.remove(local_filename)

        return True
    except Exception as e:
        print(f"下载或解压时出错: {e}")
        return False

def check_for_updates(source, channel):
    """检查更新 — 已禁用，始终返回无更新"""
    return {"new_version": False}





@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('500.html'), 500

@app.errorhandler(400)
def bad_request_error(e):
    return render_template('400.html'), 400




@app.route('/other', methods=['GET', 'POST'])
@login_required  # 如果需要登录才能访问
def other():
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'edit':
            # 获取用户输入的变量
            target_directory = request.form.get('target_directory')
            old_domain = request.form.get('old_domain')
            new_domain = request.form.get('new_domain')
            # 保存变量到会话
            session['script_params'] = {
                'target_directory': target_directory,
                'old_domain': old_domain,
                'new_domain': new_domain
            }
            flash('参数已保存。', 'success')
            return redirect(url_for('other'))
        elif action == 'run':
            # 从会话中获取变量
            script_params = session.get('script_params')
            if not script_params:
                flash('请先设置脚本参数。', 'error')
                return redirect(url_for('other'))
            # 运行脚本
            result = run_replace_domain_script(
                script_params['target_directory'],
                script_params['old_domain'],
                script_params['new_domain']
            )
            if result:
                flash('脚本已启动！请查看日志。', 'success')
            else:
                flash('脚本启动失败。', 'error')
            return redirect(url_for('other'))
    else:
        # GET 请求，渲染页面并传递日志内容
        script_params = session.get('script_params', {})
        log_content = get_script_log()  # 获取日志内容
        return render_template('other.html',
                               script_params=script_params,
                               log_content=log_content)


def run_task_immediately(task_id):
    # 获取所有任务
    tasks = list_tasks_in_cron()

    # 查找指定 task_id 对应的任务
    task_to_run = next((task for task in tasks if task.get('task_id') == task_id), None)

    if task_to_run:
        # 获取任务命令
        command = task_to_run.get('command')
        if not command:
            raise ValueError('找不到该任务的命令，无法运行。')

        try:
            # 使用 subprocess 来运行任务的命令
            subprocess.Popen(command, shell=True)
            print(f"任务 {task_id} 已立即运行。")
        except Exception as e:
            print(f"运行任务 {task_id} 时发生错误: {e}")
    else:
        raise ValueError(f"找不到 task_id 为 {task_id} 的任务。")

@app.route('/run_task_now/<task_id>', methods=['POST'])
def run_task_now(task_id):
    try:
        # 调用立即运行任务的函数
        run_task_immediately(task_id)
        flash(f"任务 {task_id} 已成功运行！", 'success')
    except Exception as e:
        flash(f"运行任务 {task_id} 时出错: {e}", 'error')

    return redirect(url_for('scheduled_tasks'))




# 辅助函数：运行脚本
def run_replace_domain_script(target_directory, old_domain, new_domain):
    script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'replace_domain.py')
    try:
        # 构建命令
        command = [
            'python3',
            script_path,
            target_directory,
            old_domain,
            new_domain
        ]
        # 后台运行脚本
        subprocess.Popen(command)
        app.logger.info(f"已启动脚本: {' '.join(command)}")
        return True
    except Exception as e:
        app.logger.error(f"运行脚本时出错：{e}")
        return False

def get_script_log():
    log_dir = os.path.join(os.getcwd(), 'logs')
    log_file_name = 'replace_domain.log'
    log_file = os.path.join(log_dir, log_file_name)
    if os.path.exists(log_file):
        with open(log_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            # 只返回最后 1000 行
            return ''.join(lines[-1000:])
    else:
        return '日志文件不存在。'

@app.route('/about', methods=['GET', 'POST'])
def about():
    if request.method == 'POST':
        source = request.form.get('source', 'github')
        channel = request.form.get('channel', 'stable')

        # 检查更新
        update_info = check_for_updates(source, channel)

        if "error" in update_info:
            return jsonify(error=update_info["error"])
        elif update_info.get("new_version"):
            return jsonify(new_version=True,
                           changelog=update_info.get('changelog'))
        else:
            return jsonify(new_version=False)

    return render_template('about.html')



def update_version():
    source = request.form.get('source', 'github')
    channel = request.form.get('channel', 'stable')

    # 检查更新
    update_info = check_for_updates(source, channel)

    if update_info.get("new_version"):
        download_url = update_info.get('download_url')

        # 下载并解压新版本
        success = download_and_extract(download_url)

        if success:
            return jsonify(message="新版本下载并安装成功！应用即将重启。")
        else:
            return jsonify(message="更新失败，下载或解压时出错。")
    else:
        return jsonify(message="当前已是最新版本，无需更新。")


# 在您的 Flask 应用中，确保已经导入了必要的模块


# 修改 forgot_password 路由
@app.route('/forgot_password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        security_code = request.form['security_code']
        new_password = request.form['new_password']
        confirm_password = request.form['confirm_password']

        # 获取安全码（强烈建议通过环境变量设置）
        stored_security_code = os.getenv('SECURITY_CODE', os.urandom(8).hex())
        if os.path.exists(ENV_FILE):
            with open(ENV_FILE, 'r') as f:
                for line in f:
                    if line.startswith('SECURITY_CODE='):
                        stored_security_code = line.split('=')[1].strip()

        # 验证安全码
        if stored_security_code != security_code:
            flash('安全码不正确', 'error')
            return redirect(url_for('forgot_password'))

        # 验证密码
        if new_password != confirm_password:
            flash('两次输入的密码不一致', 'error')
            return redirect(url_for('forgot_password'))

        # 获取存储的用户名
        stored_username, _ = db_handler.get_user_credentials()

        # 更新密码哈希
        new_password_hash = generate_password_hash(new_password)
        db_handler.set_user_credentials(username=stored_username, password_hash=new_password_hash)

        # 将用户名存储在会话中
        session['reset_username'] = stored_username

        # 重定向到同一页面，以便显示提示框
        return redirect(url_for('forgot_password'))

    else:
        # 处理 GET 请求，获取并弹出用户名
        reset_username = session.pop('reset_username', None)
        return render_template('forgot_password.html', reset_username=reset_username)



def check_and_apply_updates():
    # 根据本地版本号选择通道
    if "beta" in VERSION:
        channel = 'beta'
    else:
        channel = 'stable'

    # 先检查国内源

    # 检查更新
    update_info = check_for_updates(source, channel)

    # 如果国内源检查失败，切换到 GitHub 源
    if update_info.get("error"):
        print(f"国内源检查更新失败，切换到 GitHub 源：{update_info['error']}")
        source = 'github'
        update_info = check_for_updates(source, channel)

    # 如果有新版本，下载并更新
    if update_info.get("new_version"):
        download_url = update_info.get('download_url')

        # 下载并解压新版本
        success = download_and_extract(download_url)

        if success:
            print("新版本下载并安装成功！应用即将重启。")
            restart_app()  # 重启应用
        else:
            print("更新失败，下载或解压时出错。")
            sys.exit(1)  # 停止启动应用，等用户手动修复问题




def sync_cron_with_backup():
    """同步 crontab 与备份文件"""
    if os.path.exists(CRON_BACKUP_FILE):
        with open(CRON_BACKUP_FILE, 'r') as f:
            backup_cron_jobs = f.read().strip()
        current_cron_jobs = subprocess.run(['crontab', '-l'], stdout=subprocess.PIPE, text=True).stdout.strip()
        if backup_cron_jobs != current_cron_jobs:
            subprocess.run(f'(echo "{backup_cron_jobs}") | crontab -', shell=True)
            print("Cron tasks synchronized with backup.")
    else:
        print("Backup file not found, skipping synchronization.")

import os

ENV_FILE = '/app/config/app.env'

def ensure_env_file():
    """确保 app.env 存在并同步环境变量，如果没有安全码和端口则自动填入默认值"""
    default_port = '5000'
    default_security_code = os.urandom(8).hex()

    # 创建 /app/config 目录（如果不存在）
    config_dir = '/app/config'
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
        logger.info(f"创建了目录: {config_dir}")

    # 从环境变量获取端口和安全码
    port = os.getenv('WEB_PORT', default_port)  # 默认端口5000
    security_code = os.getenv('SECURITY_CODE', default_security_code)



    env_file = os.path.join(config_dir, 'app.env')

    # 检查是否创建了 app.env 文件
    if not os.path.exists(env_file):
        logger.info(f"正在创建 {env_file} 文件")
        with open(env_file, 'w') as f:
            f.write(f"WEB_PORT={port}\n")
            f.write(f"SECURITY_CODE={security_code}\n")
        logger.info(f"成功创建 {env_file} 文件")
    else:
        logger.info(f"{env_file} 文件已存在，检查内容")

        # 如果 app.env 存在，检查并写入默认值（如果缺少）
        lines = []
        found_port = False
        found_security_code = False
        found_host_uid = False
        found_host_gid = False

        with open(env_file, 'r') as f:
            lines = f.readlines()

        # 检查是否有安全码、端口、UID 和 GID 的定义
        for line in lines:
            if line.startswith('WEB_PORT='):
                found_port = True
            if line.startswith('SECURITY_CODE='):
                found_security_code = True


        # 如果没有端口和安全码，则补充默认值
        if not found_port:
            lines.append(f"WEB_PORT={port}\n")
            logger.info(f"添加缺失的 WEB_PORT={port}")
        if not found_security_code:
            lines.append(f"SECURITY_CODE={security_code}\n")
            logger.info(f"添加缺失的 SECURITY_CODE={security_code}")

        # 将更新的内容写回到 app.env 文件
        with open(env_file, 'w') as f:
            f.writelines(lines)
        logger.info(f"{env_file} 文件内容已更新")



def load_port_from_env():
    """从环境变量或 app.env 中加载端口"""
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, 'r') as f:
            for line in f:
                if line.startswith('WEB_PORT='):
                    return int(line.split('=')[1].strip())
    return 5000  # 如果未找到，则返回默认端口



# ============================================================================
# Feature 1: Watch Mode — 后台持续监控
# ============================================================================

@app.route('/watch/start/<int:config_id>', methods=['POST'])
def watch_start(config_id):
    """为指定配置开启 Watch Mode"""
    try:
        interval = int(request.form.get('interval', 300))
        db_handler.upsert_watch_config(config_id, enabled=1, interval_seconds=interval)
        flash(f'Watch Mode 已开启（间隔 {interval} 秒）', 'success')
    except Exception as e:
        flash(f'开启 Watch Mode 失败: {e}', 'error')
    return redirect(url_for('configs'))


@app.route('/watch/stop/<int:config_id>', methods=['POST'])
def watch_stop(config_id):
    """停止指定配置的 Watch Mode"""
    try:
        db_handler.upsert_watch_config(config_id, enabled=0, interval_seconds=300)
        flash('Watch Mode 已停止', 'success')
    except Exception as e:
        flash(f'停止 Watch Mode 失败: {e}', 'error')
    return redirect(url_for('configs'))


# ============================================================================
# Feature 2: Auto-Repair — 修复失效 STRM
# ============================================================================

@app.route('/repair/<int:config_id>', methods=['POST'])
def repair_config(config_id):
    """手动触发 Auto-Repair：检测并修复失效的 STRM 文件"""
    ok = _run_config_impl(config_id, extra_args=['--repair'])
    if ok:
        flash(f'Auto-Repair 已启动，正在检测并修复失效 STRM...', 'success')
    else:
        flash('无法找到 main.py 文件', 'error')
    return redirect(url_for('configs'))


@app.route('/repair_all', methods=['POST'])
def repair_all_configs():
    """对所有配置执行 Auto-Repair"""
    configs = db_handler.get_all_configurations()
    count = 0
    for cfg in configs:
        cid = cfg['config_id'] if hasattr(cfg, '__getitem__') else cfg[0]
        _run_config_impl(cid, extra_args=['--repair'])
        count += 1
    flash(f'已对 {count} 个配置启动 Auto-Repair', 'success')
    return redirect(url_for('configs'))


# ============================================================================
# Feature 4: Progress — 实时进度 SSE 流
# ============================================================================

@app.route('/progress/<int:config_id>')
def progress_sse(config_id):
    """Server-Sent Events 流，实时推送同步进度"""
    from flask import Response

    def generate():
        import time
        last_state = None
        seen_done = False
        while not seen_done:
            state = _progress_store.get(config_id)
            if state and state != last_state:
                last_state = state
                yield f"data: {json.dumps(state)}\n\n"
                if state.get('status') == 'done':
                    seen_done = True
            time.sleep(1)

        yield f"data: {json.dumps({'status': 'closed'})}\\n\\n"

    return Response(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )


@app.route('/progress_status/<int:config_id>')
def progress_status(config_id):
    """JSON API：获取当前进度状态"""
    state = _progress_store.get(config_id, {'status': 'idle'})
    return jsonify(state)


# ============================================================================
# Sync History — 查看同步历史
# ============================================================================

@app.route('/sync_history/<int:config_id>')
def sync_history(config_id):
    """查看指定配置的同步历史"""
    try:
        history = db_handler.get_recent_sync_history(config_id, limit=20)
        return render_template('sync_history.html', history=history, config_id=config_id)
    except Exception as e:
        flash(f'获取历史记录失败: {e}', 'error')
        return redirect(url_for('configs'))


# ============================================================================
# Broken STRMs — 失效 STRM 列表
# ============================================================================

@app.route('/broken_strms')
def broken_strms():
    """查看所有配置的失效 STRM"""
    try:
        broken = db_handler.get_all_broken_strms()
        return render_template('broken_strms.html', broken_strms=broken)
    except Exception as e:
        flash(f'获取失效列表失败: {e}', 'error')
        return redirect(url_for('index'))


@app.route('/broken_strm/<int:broken_id>/repair', methods=['POST'])
def repair_broken_strm(broken_id):
    """手动修复单个失效 STRM"""
    try:
        db_handler.cursor.execute(
            'SELECT config_id, strm_path FROM broken_strms WHERE id = ?', (broken_id,)
        )
        row = db_handler.cursor.fetchone()
        if row:
            # Trigger repair for this config
            _run_config_impl(row[0], extra_args=['--repair'])
        db_handler.remove_broken_strm(broken_id)
        flash('已重新生成该 STRM', 'success')
    except Exception as e:
        flash(f'修复失败: {e}', 'error')
    return redirect(url_for('broken_strms'))


@app.route('/broken_strms/clear/<int:config_id>', methods=['POST'])
def clear_broken_strms(config_id):
    """清除指定配置的失效记录"""
    try:
        db_handler.clear_broken_strms(config_id)
        flash('失效记录已清除', 'success')
    except Exception as e:
        flash(f'清除失败: {e}', 'error')
    return redirect(url_for('broken_strms'))




# ============================================================================
# Internal helpers
# ============================================================================

def _run_config_impl(config_id, extra_args=None):
    """Internal implementation: launch main.py to generate strm (non-blocking)."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    main_script_path = os.path.join(current_dir, 'main.py')
    if os.path.exists(main_script_path):
        args_str = ' '.join(extra_args) if extra_args else ''
        command = f"/usr/local/bin/python3.9 {main_script_path} {config_id} {args_str}".strip()
        logger.info(f"Manual run config ID: {config_id} args={extra_args}")
        subprocess.Popen(command, shell=True)
        return True
    else:
        logger.error(f"main.py not found: {main_script_path}")
        return False


if __name__ == '__main__':
    # 启动应用之前先检查更新
    # check_and_apply_updates()  # DISABLED: auto-update causes crash loop
    sync_cron_with_backup()
    ensure_env_file()
    port = load_port_from_env()
    app.run(host="0.0.0.0", port=port, debug=False)




