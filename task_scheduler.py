"""
OpenList-strm Task Scheduler
System cron wrapper for managing periodic sync tasks.
Falls back to APScheduler if cron is unavailable.
"""
import os
import re
import subprocess
import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# In-memory store for tasks (backup when cron isn't available)
_scheduler = BackgroundScheduler()
_scheduler_started = False
_task_store = {}  # task_id -> task dict
_task_counter = 0

CRON_MARKER_START = "# === OpenList-strm Task Scheduler Start ==="
CRON_MARKER_END = "# === OpenList-STrm Task Scheduler End ==="
CRON_BACKUP_FILE = "/config/cron.bak"


def _ensure_marker_in_crontab():
    """Ensure the OpenList-strm cron section exists in crontab."""
    result = subprocess.run(
        ['crontab', '-l'], capture_output=True, text=True
    )
    existing = result.stdout or ''
    
    if CRON_MARKER_START not in existing:
        marker_block = f"\n{CRON_MARKER_START}\n{CRON_MARKER_END}\n"
        new_cron = existing.rstrip() + marker_block
        subprocess.run(f'(echo "{new_cron}") | crontab -', shell=True)


def _extract_tasks_from_crontab():
    """Parse crontab and extract OpenList-strm tasks."""
    try:
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        lines = (result.stdout or '').strip().split('\n')
    except Exception:
        return []
    
    tasks = []
    in_block = False
    for line in lines:
        line = line.strip()
        if CRON_MARKER_START in line:
            in_block = True
            continue
        if CRON_MARKER_END in line:
            in_block = False
            continue
        if not in_block or not line or line.startswith('#'):
            continue
        
        parts = line.split(None, 5)
        if len(parts) < 6:
            continue
        
        cron_time = ' '.join(parts[:5])
        command = parts[5] if len(parts) > 5 else ''
        
        # Parse config_ids and task info from command comment
        # Format: /app/main.py ... # TASK_<task_id>_<config_ids>_<task_name>_<enabled>
        task_id = None
        config_ids = []
        task_name = 'Untitled'
        is_enabled = True
        
        if '# TASK_' in command:
            cmd_part, meta_part = command.rsplit('# TASK_', 1)
            meta = meta_part.strip()
            
            segments = meta.split('_', 4)
            if len(segments) >= 1 and segments[0]:
                task_id = segments[0]
            if len(segments) >= 2 and segments[1]:
                config_ids = [c for c in segments[1].split(',') if c]
            if len(segments) >= 4:
                task_name = segments[2]
                is_enabled = segments[3].lower() != 'disabled'
            
            command = cmd_part.strip()
        
        tasks.append({
            'task_id': task_id,
            'cron_time': cron_time,
            'command': command,
            'config_ids': config_ids,
            'task_name': task_name,
            'is_enabled': is_enabled,
        })
    
    return tasks


def _save_cron_backup(tasks):
    """Save current tasks to backup file."""
    os.makedirs(os.path.dirname(CRON_BACKUP_FILE), exist_ok=True)
    with open(CRON_BACKUP_FILE, 'w') as f:
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        current = result.stdout or ''
        
        # Rebuild crontab with new tasks
        lines = current.split('\n')
        new_lines = []
        skip = False
        for line in lines:
            if CRON_MARKER_START in line:
                skip = True
                continue
            if CRON_MARKER_END in line:
                skip = False
                continue
            if not skip:
                new_lines.append(line)
        
        # Write back without our section (will re-add below)
        subprocess.run(
            f'(echo "{chr(10).join(new_lines).rstrip()}") | crontab -',
            shell=True
        )
        
        # Write backup
        backup_lines = ['']
        for task in tasks:
            if task.get('is_enabled', True):
                meta = f"TASK_{task['task_id']}_{','.join(task['config_ids'])}_{task['task_name']}_enabled"
            else:
                meta = f"TASK_{task['task_id']}_{','.join(task['config_ids'])}_{task['task_name']}_disabled"
            cron_line = f"{task['cron_time']} {task['command']} # {meta}"
            backup_lines.append(cron_line)
        
        f.write('\n'.join(backup_lines) + '\n')


def list_tasks_in_cron():
    """List all OpenList-strm cron tasks."""
    try:
        return _extract_tasks_from_crontab()
    except Exception:
        return list(_task_store.values())


def convert_to_cron_time(interval_type, interval_value):
    """
    Convert interval type and value to cron expression.
    
    interval_type: 'minute' | 'hourly' | 'daily' | 'weekly' | 'monthly'
    interval_value: integer (minute=1-59, hourly=1-23, daily=1-31, weekly=0-6, monthly=1-12)
    
    Returns: cron expression string
    """
    minute, hour, day, month, weekday = '*', '*', '*', '*', '*'
    
    if interval_type == 'minute':
        minute = f'*/{interval_value}'
    elif interval_type == 'hourly':
        minute = '0'
        hour = f'*/{interval_value}'
    elif interval_type == 'daily':
        minute = '0'
        hour = '0'
        day = f'*/{interval_value}'
    elif interval_type == 'weekly':
        minute = '0'
        hour = '0'
        weekday = str(interval_value)
    elif interval_type == 'monthly':
        minute = '0'
        hour = '0'
        day = '1'
        month = f'*/{interval_value}'
    
    return f'{minute} {hour} {day} {month} {weekday}'


def add_tasks_to_cron(task_name, cron_time, config_ids, task_mode='incremental', is_enabled=True):
    """
    Add a new cron task for each config_id.
    
    Returns: list of created task_id strings
    """
    global _task_counter, _scheduler_started
    
    if not _scheduler_started:
        _scheduler.start()
        _scheduler_started = True
    
    task_ids = []
    
    for config_id in config_ids:
        _task_counter += 1
        task_id = str(_task_counter)
        
        # Build command: run main.py for this config
        command = f'/usr/local/bin/python3.9 /app/main.py {config_id}'
        if task_mode == 'full':
            command += ' --full'
        
        # Build crontab entry with metadata comment
        meta = f"TASK_{task_id}_{config_id}_{task_name}_{'enabled' if is_enabled else 'disabled'}"
        cron_line = f'{cron_time} {command} # {meta}'
        
        # Update crontab
        _ensure_marker_in_crontab()
        try:
            result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
            current = result.stdout or ''
        except Exception:
            current = ''
        
        # Replace marker section
        new_block = f'\n{CRON_MARKER_START}\n'
        # Find existing tasks (not this new one)
        existing_tasks = []
        lines = current.split('\n')
        in_block = False
        for line in lines:
            if CRON_MARKER_START in line:
                in_block = True
                continue
            if CRON_MARKER_END in line:
                in_block = False
                continue
            if in_block:
                existing_tasks.append(line)
        
        new_block += '\n'.join(existing_tasks + [cron_line])
        new_block += f'\n{CRON_MARKER_END}'
        
        new_cron = re.sub(
            f'\n{CRON_MARKER_START}.*?{CRON_MARKER_END}\n',
            new_block,
            current,
            flags=re.DOTALL
        )
        if CRON_MARKER_START not in new_cron:
            new_cron = current.rstrip() + new_block
        
        subprocess.run(f'(echo "{new_cron}") | crontab -', shell=True)
        
        task_ids.append(task_id)
        
        # Also store in memory for APScheduler fallback
        _task_store[task_id] = {
            'task_id': task_id,
            'task_name': task_name,
            'cron_time': cron_time,
            'config_ids': [str(config_id)],
            'command': command,
            'task_mode': task_mode,
            'is_enabled': is_enabled,
        }
    
    return task_ids


def update_tasks_in_cron(task_ids, cron_time, config_ids, task_name, task_mode='incremental', is_enabled=True):
    """Update existing cron tasks."""
    global _scheduler_started
    
    if not _scheduler_started:
        _scheduler.start()
        _scheduler_started = True
    
    # For simplicity, delete old tasks and re-add
    for tid in task_ids:
        _task_store.pop(tid, None)
    
    delete_tasks_from_cron(task_ids)
    return add_tasks_to_cron(task_name, cron_time, config_ids, task_mode, is_enabled)


def delete_tasks_from_cron(task_ids):
    """Remove tasks from crontab by task_id."""
    try:
        result = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        current = result.stdout or ''
    except Exception:
        current = ''
    
    lines = current.split('\n')
    new_lines = []
    skip = False
    
    for line in lines:
        if CRON_MARKER_START in line:
            skip = True
            continue
        if CRON_MARKER_END in line:
            skip = False
            continue
        
        if skip:
            # Check if this line belongs to a task we're deleting
            should_delete = False
            for tid in task_ids:
                if f'# TASK_{tid}_' in line:
                    should_delete = True
                    break
            if should_delete:
                continue
        
        new_lines.append(line)
    
    new_cron = '\n'.join(new_lines)
    subprocess.run(f'(echo "{new_cron}") | crontab -', shell=True)


def run_task_immediately(task_id):
    """Execute a task's command immediately via subprocess."""
    tasks = list_tasks_in_cron()
    task = next((t for t in tasks if t.get('task_id') == task_id), None)
    
    if not task:
        task = _task_store.get(task_id)
    
    if not task:
        raise ValueError(f"Task {task_id} not found")
    
    command = task.get('command')
    if not command:
        raise ValueError(f"No command for task {task_id}")
    
    # Run in background
    subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
