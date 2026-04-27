"""OpenList-strm Task Scheduler
Uses APScheduler for all task scheduling (crontab unavailable in Docker).
"""
import os
import shutil
import subprocess
import logging
import atexit
import json
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

_logger = logging.getLogger(__name__)
_scheduler = BackgroundScheduler(timezone="Asia/Shanghai", job_defaults={"coalesce": True, "max_instances": 1})
_scheduler_started = False
_tasks = {}
_task_counter = 0
TASKS_FILE = "/app/data/scheduler_tasks.json"


def _has_crontab():
    """Check if crontab command is available."""
    return shutil.which("crontab") is not None


def _load_tasks():
    """Load tasks from backup JSON file."""
    global _tasks, _task_counter
    if os.path.exists(TASKS_FILE):
        try:
            with open(TASKS_FILE, "r") as f:
                data = json.load(f)
                _tasks = data.get("tasks", {})
                _task_counter = data.get("counter", 0)
        except Exception:
            pass


def _save_tasks():
    """Persist tasks to JSON backup."""
    os.makedirs(os.path.dirname(TASKS_FILE), exist_ok=True)
    with open(TASKS_FILE, "w") as f:
        json.dump({"tasks": _tasks, "counter": _task_counter}, f)


def _start_scheduler():
    """Start APScheduler if not already started."""
    global _scheduler_started
    if not _scheduler_started:
        _scheduler.start()
        _scheduler_started = True
        atexit.register(_scheduler.shutdown)
        _load_tasks()
        for task in _tasks.values():
            _schedule_job(task)


def _schedule_job(task):
    """Add a job to APScheduler."""
    if not task.get("is_enabled", True):
        return
    job_id = task["task_id"]
    try:
        _scheduler.remove_job(job_id)
    except Exception:
        pass

    config_id = task.get("config_ids", [""])[0]
    command = f'/usr/local/bin/python3.9 /app/main.py {config_id}'
    if task.get("task_mode") == "full":
        command += " --full"

    cron_time = task.get("cron_time", "* * * * *")
    parts = cron_time.split()
    if len(parts) >= 5:
        minute, hour, day, month, dow = parts[0], parts[1], parts[2], parts[3], parts[4]
        trigger = CronTrigger(
            minute=minute, hour=hour, day=day, month=month, day_of_week=dow
        )
    else:
        trigger = CronTrigger(minute="*")

    def run_job():
        try:
            subprocess.Popen(
                command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        except Exception as e:
            _logger.error(f"Task {job_id} failed: {e}")

    _scheduler.add_job(run_job, trigger=trigger, id=job_id, replace_existing=True)


def convert_to_cron_time(interval_type, interval_value):
    """Convert interval type and value to cron expression (5-field)."""
    minute, hour, day, month, weekday = "*", "*", "*", "*", "*"
    if interval_type == "minute":
        minute = f"*/{interval_value}"
    elif interval_type == "hourly":
        minute = "0"
        hour = f"*/{interval_value}"
    elif interval_type == "daily":
        minute, hour = "0", "0"
        day = f"*/{interval_value}" if interval_value > 1 else "*"
    elif interval_type == "weekly":
        minute, hour = "0", "0"
        weekday = str(interval_value)
    elif interval_type == "monthly":
        minute, hour, day = "0", "0", "1"
        month = f"*/{interval_value}" if interval_value > 1 else "*"
    return f"{minute} {hour} {day} {month} {weekday}"


def list_tasks():
    """Return all tasks."""
    _start_scheduler()
    return list(_tasks.values())


def add_tasks_to_cron(task_name, cron_time, config_ids, task_mode="incremental", is_enabled=True):
    """Add new tasks using APScheduler."""
    global _task_counter
    _start_scheduler()

    task_ids = []
    for config_id in config_ids:
        _task_counter += 1
        task_id = str(_task_counter)
        task = {
            "task_id": task_id,
            "task_name": task_name,
            "cron_time": cron_time,
            "config_ids": [str(config_id)],
            "task_mode": task_mode,
            "is_enabled": is_enabled,
        }
        _tasks[task_id] = task
        _schedule_job(task)
        task_ids.append(task_id)

    _save_tasks()
    return task_ids


def update_tasks_in_cron(task_ids, cron_time, config_ids, task_name, task_mode="incremental", is_enabled=True):
    """Update existing tasks."""
    _start_scheduler()
    for tid in task_ids:
        if tid in _tasks:
            _scheduler.remove_job(tid)
        _tasks.pop(tid, None)
    return add_tasks_to_cron(task_name, cron_time, config_ids, task_mode, is_enabled)


def delete_tasks_from_cron(task_ids):
    """Remove tasks."""
    _start_scheduler()
    for tid in task_ids:
        try:
            _scheduler.remove_job(tid)
        except Exception:
            pass
        _tasks.pop(tid, None)
    _save_tasks()


def run_task_immediately(task_id):
    """Run a task immediately."""
    task = _tasks.get(task_id)
    if not task:
        raise ValueError(f"Task {task_id} not found")
    config_id = task.get("config_ids", [""])[0]
    command = f'/usr/local/bin/python3.9 /app/main.py {config_id}'
    if task.get("task_mode") == "full":
        command += " --full"
    subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
