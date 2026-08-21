# -*- coding: utf-8 -*-
"""
路径管理：同时支持「源码运行」与「PyInstaller 打包（frozen）」两种模式。
- base_dir()：程序所在目录（源码=项目目录；打包后=exe 目录）
- bundle_dir()：只读资源目录（static 前端等）
    · 源码运行 → 项目目录
    · 打包后   → PyInstaller 解包的临时目录（sys._MEIPASS）
- user_dir()：统一用户数据目录（隐私数据 + 获得的数据都在这里）
    · 默认 <用户主目录>/TwitterProfileGenerator
    · 可用环境变量 TPG_DATA_DIR 覆盖（便于多开/测试）
- 任务隔离：每次任务调用 set_task_key() 后，输出落在 <输出目录>/<task_key>/ 下，
  避免不同目标（用户/关键词）的数据互相串扰。未设置 key 时回落到输出目录根。
- 输出位置：set_output_dir() 可把输出改到用户指定的目录（界面「选择输出目录」）；
  偏好持久化在 user_dir/app_settings.json，下次启动自动恢复。
"""
import json
import os
import sys

_task_key = ""
_output_dir = ""   # 用户自定义输出目录（绝对路径）；空 = 默认 user_dir/output


def base_dir() -> str:
    """程序所在目录（源码=项目目录；打包后=exe 目录）。"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def bundle_dir() -> str:
    if getattr(sys, "frozen", False):
        return sys._MEIPASS
    return base_dir()


def user_dir() -> str:
    """统一用户数据目录：隐私数据（配置/cookie/账号库）与获得的数据（output）都在这里。"""
    env = os.environ.get("TPG_DATA_DIR", "").strip()
    if env:
        return os.path.abspath(env)
    return os.path.join(os.path.expanduser("~"), "TwitterProfileGenerator")


def _settings_file() -> str:
    return os.path.join(user_dir(), "app_settings.json")


def _load_settings() -> dict:
    try:
        with open(_settings_file(), "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_settings(d: dict) -> None:
    try:
        os.makedirs(user_dir(), exist_ok=True)
        tmp = _settings_file() + f".{os.getpid()}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
        os.replace(tmp, _settings_file())
    except Exception:
        pass


def set_task_key(key: str) -> None:
    """设置当前任务的隔离目录 key；传空字符串则回到输出目录根。"""
    global _task_key
    _task_key = (key or "").strip()


def set_output_dir(path: str) -> str:
    """设置输出目录（绝对路径）并持久化偏好；传空恢复默认 user_dir/output。
    返回生效的绝对路径。目录不存在时自动创建。"""
    global _output_dir
    p = (path or "").strip().strip('"')
    if p:
        p = os.path.abspath(p)
        try:
            os.makedirs(p, exist_ok=True)
        except Exception:
            p = ""   # 无法创建（权限/非法路径）→ 回退默认
    _output_dir = p
    s = _load_settings()
    s["output_dir"] = p
    _save_settings(s)
    return _output_dir


def output_root() -> str:
    """输出根目录：用户自定义目录优先，默认 user_dir/output。"""
    if _output_dir:
        return _output_dir
    return os.path.join(user_dir(), "output")


def out_dir() -> str:
    return os.path.join(output_root(), _task_key) if _task_key else output_root()


def archive_dir() -> str:
    return os.path.join(out_dir(), "archive")


def ensure_dirs() -> None:
    """确保用户数据目录与输出目录存在（启动时调用一次）。"""
    os.makedirs(user_dir(), exist_ok=True)
    os.makedirs(output_root(), exist_ok=True)
