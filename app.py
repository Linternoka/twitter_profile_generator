# -*- coding: utf-8 -*-
"""
推特用户画像生成器 - Web 图形界面 + 流程编排
=============================================
一键完成：抓取推文 → 清洗统计 → LLM 生成用户画像。

运行：
    python app.py [端口]          # 默认 8001
然后浏览器打开 http://127.0.0.1:8001

依赖：twscrape、pandas、httpx（GUI 零额外依赖，纯标准库 http.server）。
"""
import asyncio
import copy
import ctypes
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import webbrowser
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import paths
from paths import bundle_dir, user_dir

STATIC_DIR = os.path.join(bundle_dir(), "static")
# 隐私数据（配置/Cookie/账号库）与获得的数据（output）统一存放在用户数据目录：
# 默认 ~/TwitterProfileGenerator（可用环境变量 TPG_DATA_DIR 覆盖）
CONFIG_FILE = os.path.join(user_dir(), "user_config.json")
LEGACY_CONFIG_FILE = os.path.join(user_dir(), "config.json")
# 多配置文件：命名配置存放在 configs/<名称>.json（"default" = 主配置文件）
CONFIGS_DIR = os.path.join(user_dir(), "configs")
PROGRESS_FILE = os.path.join(user_dir(), "progress.json")
# 模块导入即可能写 progress.json（Progress 初始化），先确保用户数据目录存在
try:
    os.makedirs(user_dir(), exist_ok=True)
except Exception:
    pass

_TASK_KEY_RE = re.compile(r'[\\/:*?"<>|\s]+')


def task_key(cfg: dict) -> str:
    """按模式+目标生成任务隔离目录 key（防跨任务数据串扰）。
    mode 与 target 均做路径清洗，防止路径逃逸；mode 仅接受白名单值。"""
    mode = (cfg.get("mode") or "user").strip().lower()
    if mode not in ("user", "keyword"):
        mode = "user"
    target = (cfg.get("target") or "").strip()
    safe = _TASK_KEY_RE.sub("_", target)[:60].strip("_. ") or "default"
    return f"{mode}_{safe}"


def result_file() -> str:
    """最近一次任务的结果文件路径（按当前任务隔离目录）。"""
    return os.path.join(paths.out_dir(), "result.json")


if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


class StopRequested(Exception):
    pass


class Progress:
    """进度管理：进程内共享 + 原子写入 progress.json 供前端轮询。"""

    def __init__(self):
        # RLock：各 set_*/report 方法在锁内改 data 后会调用 write()（同样加锁），
        # 用可重入锁避免自死锁；读侧（/api/status）通过 snapshot() 取一致快照。
        self.lock = threading.RLock()
        self.data = {
            "status": "idle",       # idle | running | done | error
            "stage": "scrape",      # scrape | process | analyze
            "detail": "",
            "mode": "",
            "target": "",
            "action": "",            # 当前任务操作类型（full/scrape/import/analyze）
            "started_at": "",
            "finished_at": "",
            "auto_shutdown": False,
            "shutdown_in": None,
            "keep_awake": False,      # 本任务是否启用了防休眠（独立于自动关机）
            "last_archive": "",       # 本次输出自动保存的历史归档目录
            "stopping": False,       # 已请求停止（前端按钮显示"正在停止…"）
            "waiting": False,        # 当前是否在等待（限流/请求间隔/LLM 响应等）
            "waiting_detail": "",    # 等待原因
            "events": [],            # [{t: "HH:MM:SS", msg: "..."}]
            "scrape": {"detail": "", "count": 0, "months_total": 0,
                       "months_done": 0, "current_month": None, "month_count": 0},
            "process": {"detail": "", "rows": 0, "dupes": 0, "input_rows": 0},
            "analyze": {"detail": "", "batch": 0, "total": 0, "batch_rows": 0},
            "error": None,
            "error_info": None,      # {category, title, guide, llm_advice}
            "updated_at": "",
        }
        self.write()

    def write(self):
        tmp = PROGRESS_FILE + f".{os.getpid()}.{threading.get_ident()}.tmp"
        with self.lock:
            self.data["updated_at"] = datetime.now(timezone.utc).isoformat()
            try:
                os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(self.data, f, ensure_ascii=False, indent=2)
                os.replace(tmp, PROGRESS_FILE)
            except PermissionError:
                try:
                    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
                        json.dump(self.data, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass
            except Exception:
                pass

    def snapshot(self) -> dict:
        """返回 data 的一致深拷贝（含嵌套 dict/events 列表），供读侧安全序列化。
        任务线程随时在改 data（含 events 追加/截断），直接 json.dumps 活字典可能
        RuntimeError（迭代中被修改）或读到半更新状态。"""
        with self.lock:
            return copy.deepcopy(self.data)

    def report(self, stage: str, **kw):
        with self.lock:
            sec = self.data.get(stage)
            if isinstance(sec, dict):
                for k, v in kw.items():
                    if k in sec:
                        sec[k] = v
            # 顶层字段：等待状态与 detail 一样可直接透传（来自 scraper/llm 回调）
            if "waiting" in kw:
                self.data["waiting"] = bool(kw["waiting"])
            if "waiting_detail" in kw:
                self.data["waiting_detail"] = str(kw["waiting_detail"] or "")
            if "detail" in kw:
                self.data["detail"] = kw["detail"]
                self._append_event(kw["detail"])
        self.write()

    def _append_event(self, msg):
        # 调用方需已持有 self.lock（report/log 均在锁内调用）
        self.data["events"].append({
            "t": datetime.now().strftime("%H:%M:%S"),
            "msg": str(msg),
        })
        if len(self.data["events"]) > 200:
            del self.data["events"][:-200]

    def set_stage(self, stage: str, detail: str = ""):
        with self.lock:
            self.data["stage"] = stage
            self.data["detail"] = detail
            # 阶段切换时清除等待标记，避免上一阶段残留"等待中"
            self.data["waiting"] = False
            self.data["waiting_detail"] = ""
        self.write()

    def set_status(self, status: str, error: str | None = None):
        with self.lock:
            self.data["status"] = status
            if status == "running":
                # 新任务开始：清掉上一任务的错误提示与诊断面板，避免界面残留旧错误
                self.data["error"] = None
                self.data["error_info"] = None
                self.data["stopping"] = False
                self.data["waiting"] = False
                self.data["waiting_detail"] = ""
            if error:
                self.data["error"] = error
            if status in ("done", "error", "idle"):
                self.data["stopping"] = False
                self.data["waiting"] = False
                self.data["waiting_detail"] = ""
            if status in ("done", "error"):
                self.data["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.write()

    def set_stopping(self, on: bool):
        """标记已请求停止（前端据此禁用停止按钮并显示"正在停止…"）。"""
        with self.lock:
            self.data["stopping"] = bool(on)
        self.write()

    def log(self, msg: str):
        """记录一条带时间戳的事件日志（供前端实时展示）。"""
        with self.lock:
            self._append_event(msg)
        self.write()

    def set_task_info(self, mode: str, target: str):
        """记录本次任务信息并重置运行时状态。"""
        with self.lock:
            self.data["mode"] = mode
            self.data["target"] = target
            self.data["action"] = ""
            self.data["started_at"] = datetime.now(timezone.utc).isoformat()
            self.data["finished_at"] = ""
            self.data["auto_shutdown"] = False
            self.data["shutdown_in"] = None
            self.data["keep_awake"] = False
            self.data["last_archive"] = ""
            self.data["stopping"] = False
            self.data["waiting"] = False
            self.data["waiting_detail"] = ""
            self.data["events"] = []
        self.write()

    def set_auto_shutdown(self, on: bool, seconds: int | None = None):
        with self.lock:
            self.data["auto_shutdown"] = bool(on)
            self.data["shutdown_in"] = seconds
        self.write()

    def set_keep_awake(self, on: bool):
        """记录本任务是否启用了防休眠（与自动关机相互独立）。"""
        with self.lock:
            self.data["keep_awake"] = bool(on)
        self.write()

    def set_action(self, action: str):
        """记录当前任务的操作类型（full/scrape/import/analyze），供前端绘制线性流程。"""
        with self.lock:
            self.data["action"] = (action or "").strip()
        self.write()

    def set_last_archive(self, path: str):
        """记录本次输出自动保存的历史归档目录。"""
        with self.lock:
            self.data["last_archive"] = (path or "")
        self.write()

    def set_error_info(self, info: dict | None):
        with self.lock:
            self.data["error_info"] = info
        self.write()

    def set_error_llm(self, advice: str):
        with self.lock:
            ei = self.data.get("error_info")
            if isinstance(ei, dict):
                ei["llm_advice"] = advice
            else:
                self.data["error_info"] = {"category": "other", "title": "未知错误",
                                           "guide": [], "llm_advice": advice}
        self.write()


PROGRESS = Progress()
LOCK = threading.Lock()

# 任务生命周期管理：每任务独立 stop_event + 任务代次，
# 避免「stop 后立即 start」时旧线程继续运行 / 状态互相覆盖。
_TASK_SEQ = 0
_SEQ_LOCK = threading.Lock()
_CURRENT_STOP: dict = {"event": None}
_STOP_LOCK = threading.Lock()


def _next_seq() -> int:
    global _TASK_SEQ
    with _SEQ_LOCK:
        _TASK_SEQ += 1
        return _TASK_SEQ


def _set_current_stop(ev: threading.Event | None):
    with _STOP_LOCK:
        _CURRENT_STOP["event"] = ev


def _signal_stop():
    with _STOP_LOCK:
        ev = _CURRENT_STOP["event"]
    if ev:
        ev.set()


# ------------------------------ 系统电源控制 ------------------------------
_ES_CONTINUOUS = 0x80000000
_ES_SYSTEM_REQUIRED = 0x00000001


def keep_awake(on: bool) -> None:
    """Windows：运行期间防止系统休眠，但允许关闭显示器（息屏）。"""
    if os.name != "nt":
        return
    try:
        flags = _ES_CONTINUOUS | (_ES_SYSTEM_REQUIRED if on else 0)
        ctypes.windll.kernel32.SetThreadExecutionState(flags)
    except Exception:
        pass


def schedule_shutdown(delay_sec: int = 60) -> bool:
    """Windows：延迟关机（60 秒内可用 shutdown /a 取消）。按返回码判断是否成功。"""
    if os.name != "nt":
        return False
    try:
        r = subprocess.run(["shutdown", "/s", "/t", str(delay_sec)],
                           capture_output=True, timeout=15)
        return r.returncode == 0
    except Exception:
        return False


def cancel_shutdown() -> bool:
    """取消已排定的系统关机（无排定计划时 shutdown /a 返回非 0）。"""
    if os.name != "nt":
        return False
    try:
        r = subprocess.run(["shutdown", "/a"],
                           capture_output=True, timeout=15)
        return r.returncode == 0
    except Exception:
        return False


def notify(title: str, message: str) -> None:
    """Windows：任务完成/出错时弹出系统气泡通知（静默失败，不影响任务）。"""
    if os.name != "nt":
        return
    try:
        def _esc(s):
            return str(s).replace("'", "''")
        ps = (f"Add-Type -AssemblyName System.Windows.Forms;"
              f"$n=New-Object System.Windows.Forms.NotifyIcon;"
              f"$n.Icon=[System.Drawing.SystemIcons]::Information;"
              f"$n.Visible=$true;"
              f"$n.ShowBalloonTip(8000,'{_esc(title)}','{_esc(message)}',"
              f"[System.Windows.Forms.ToolTipIcon]::Info);"
              f"Start-Sleep -Seconds 8; $n.Dispose()")
        subprocess.Popen(["powershell", "-NoProfile", "-WindowStyle", "Hidden",
                          "-Command", ps],
                         creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    except Exception:
        pass


# 配置文件名称白名单：中英文/数字/下划线/短横线，1-32 字符，防路径逃逸
_PROFILE_RE = re.compile(r"^[A-Za-z0-9_\-\u4e00-\u9fff]{1,32}$")


def _sanitize_profile(name) -> str:
    """规范化配置文件名称；空/非法/\"default\" 返回 ''（= 默认主配置文件）。"""
    name = (name or "").strip()
    if not name or name in ("default", "默认"):
        return ""
    if not _PROFILE_RE.match(name):
        return ""
    return name


def config_path(name: str = "") -> str:
    """返回指定配置文件路径；name 为空返回默认主配置文件。"""
    name = _sanitize_profile(name)
    if not name:
        return CONFIG_FILE
    return os.path.join(CONFIGS_DIR, f"{name}.json")


def list_configs() -> list:
    """列出所有可用配置文件名称（\"default\" 恒在首位，其后为命名配置）。
    仅列出名称合法（_sanitize_profile 通过）的文件，避免前端选中非法名后
    被回退成默认配置造成名不副实。"""
    names = ["default"]
    try:
        if os.path.isdir(CONFIGS_DIR):
            for fn in sorted(os.listdir(CONFIGS_DIR)):
                if not fn.endswith(".json"):
                    continue
                name = fn[:-5]
                if _sanitize_profile(name):
                    names.append(name)
    except Exception:
        pass
    return names


def _load_one(path: str) -> dict | None:
    """读取单个配置文件；文件不存在或损坏返回 None。"""
    if os.path.exists(path):
        try:
            # utf-8-sig 兼容带 BOM 的文件（如记事本/Excel 编辑过）
            with open(path, "r", encoding="utf-8-sig") as f:
                return json.load(f)
        except Exception:
            return None
    return None


def load_config(name: str = "") -> dict:
    """读取指定配置文件；name 为空读默认（旧版 config.json 存在时自动迁移）。"""
    if _sanitize_profile(name):
        return _load_one(config_path(name)) or {}
    for p in (CONFIG_FILE, LEGACY_CONFIG_FILE):
        cfg = _load_one(p)
        if cfg is not None:
            if p == LEGACY_CONFIG_FILE:
                try:
                    save_config(cfg)
                    os.remove(LEGACY_CONFIG_FILE)
                except Exception:
                    pass
            return cfg
    return {}


def save_config(cfg: dict, name: str = ""):
    """保存配置到指定配置文件（name 为空写默认主配置文件）。
    唯一临时名，避免多线程并发写同一 .tmp 互相截断。"""
    path = config_path(name)
    if _sanitize_profile(name):
        os.makedirs(CONFIGS_DIR, exist_ok=True)
    tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def clear_config(name: str = "") -> None:
    """清除配置文件；name 为空同时清理旧版遗留文件。"""
    if _sanitize_profile(name):
        p = config_path(name)
        if os.path.exists(p):
            os.remove(p)
        return
    for p in (CONFIG_FILE, LEGACY_CONFIG_FILE):
        if os.path.exists(p):
            os.remove(p)


def _strip_meta(cfg: dict) -> dict:
    """剔除不应持久化的元数据字段（操作/配置文件/导入文件/使用已存标记/输出目录），
    仅保存表单偏好。"""
    d = dict(cfg)
    for k in ("action", "profile", "files", "use_saved_cookies", "use_saved_key",
              "output_dir"):
        d.pop(k, None)
    return d


# ------------------------------ 最近使用目标历史 ------------------------------
RECENT_FILE = os.path.join(user_dir(), "recent_targets.json")


def _load_recent() -> list:
    """读取最近使用目标列表（最多 10 条，{mode,target,preset,at}）。"""
    try:
        with open(RECENT_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, list) else []
    except Exception:
        return []


def _record_recent(cfg: dict) -> None:
    """记录一次任务使用的目标到最近历史（去重、置顶、限量 10）。"""
    try:
        mode = (cfg.get("mode") or "user").strip()
        target = (cfg.get("target") or "").strip().lstrip("@")
        if not target:
            return
        entry = {"mode": mode, "target": target,
                 "preset": cfg.get("preset") or "",
                 "at": datetime.now().strftime("%m-%d %H:%M")}
        items = [x for x in _load_recent()
                 if not (x.get("mode") == mode and x.get("target") == target)]
        items.insert(0, entry)
        items = items[:10]
        tmp = RECENT_FILE + f".{os.getpid()}.{threading.get_ident()}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        os.replace(tmp, RECENT_FILE)
    except Exception:
        pass


def _safe_import_filename(name) -> str | None:
    """校验导入文件名，覆盖 Windows 对尾随空格/句点的规范化规则。"""
    raw = (name or "").strip()
    base = os.path.basename(raw)
    if not raw or base != raw or base.startswith("."):
        return None
    canonical = base.rstrip(" .")
    if not canonical.lower().endswith(".csv"):
        return None
    if canonical.lower() == "clean_tweets.csv":
        return None
    stem = canonical[:-4].rstrip(" .").upper()
    if stem in {"CON", "PRN", "AUX", "NUL"} or stem.startswith(("COM", "LPT")) and stem[3:].isdigit():
        return None
    return base


# ------------------------------ 旧数据迁移（程序目录 → 用户数据目录） ------------------------------
def migrate_legacy_data() -> list:
    """把旧版本散落在程序目录（源码=项目目录 / 打包=exe 旁）的隐私数据与抓取数据
    迁移到统一的用户数据目录。仅当目标不存在时迁移（不覆盖新数据）；
    源文件迁移后保留空目录结构不影响使用。返回迁移条目列表（供界面提示）。"""
    src = paths.base_dir()
    dst = paths.user_dir()
    if os.path.abspath(src) == os.path.abspath(dst):
        return []
    moved = []

    def _move_file(name: str, sub: str = "") -> None:
        s = os.path.join(src, sub, name) if sub else os.path.join(src, name)
        d = os.path.join(dst, sub, name) if sub else os.path.join(dst, name)
        if not os.path.exists(s) or os.path.exists(d):
            return
        try:
            os.makedirs(os.path.dirname(d), exist_ok=True)
            shutil.move(s, d)
            moved.append(f"{sub + '/' if sub else ''}{name}")
        except Exception as e:
            print(f"[app] 迁移 {name} 失败（忽略）：{e}")

    # 隐私/运行时文件
    for name in ("user_config.json", "config.json", "progress.json",
                 "accounts.db", "运行锁.lock"):
        _move_file(name)
    # 命名配置目录
    s_cfg = os.path.join(src, "configs")
    d_cfg = os.path.join(dst, "configs")
    if os.path.isdir(s_cfg):
        os.makedirs(d_cfg, exist_ok=True)
        for fn in os.listdir(s_cfg):
            if fn.endswith(".json"):
                _move_file(fn, "configs")
    # 抓取数据目录（整个 output/ 移入用户数据目录）
    s_out = os.path.join(src, "output")
    d_out = os.path.join(dst, "output")
    if os.path.isdir(s_out):
        if not os.path.exists(d_out):
            try:
                shutil.move(s_out, d_out)
                moved.append("output/")
            except Exception as e:
                print(f"[app] 迁移 output/ 失败（忽略）：{e}")
        else:
            # 两边都有 output：按任务目录逐个合并（不覆盖已存在目录）
            try:
                for entry in os.listdir(s_out):
                    se = os.path.join(s_out, entry)
                    de = os.path.join(d_out, entry)
                    if not os.path.exists(de):
                        shutil.move(se, de)
                        moved.append(f"output/{entry}/")
            except Exception as e:
                print(f"[app] 合并 output/ 失败（忽略）：{e}")
    if moved:
        print(f"[app] 已把旧数据迁移到统一用户目录 {dst}：{', '.join(moved)}")
    return moved


# 已保存 Cookie 的掩码标记：/api/config 不再明文回传 cookies，
# 前端看到该标记时提示「已保存」，启动/检测时带 use_saved_cookies 由后端从配置读取。
SAVED_COOKIES_MASK = "__SAVED_COOKIES__"


def _resolve_cookies(data: dict) -> str:
    """解析请求中的 cookies：优先用表单值；若标记 use_saved_cookies 且表单为空，
    则从对应配置文件读取已保存的 cookies。"""
    cookies = (data.get("cookies") or "").strip()
    if cookies and cookies != SAVED_COOKIES_MASK:
        return cookies
    if data.get("use_saved_cookies"):
        saved = load_config(data.get("profile") or "")
        return (saved.get("cookies") or "").strip()
    return ""


def mask_config(cfg: dict) -> dict:
    c = dict(cfg)
    key = c.get("llm_api_key")
    if key:
        c["llm_api_key"] = (key[:4] + "***") if len(key) > 4 else "******"
    # cookies 同样掩码：避免最敏感的账号凭据经 HTTP 明文回传；
    # 前端以「已保存」提示呈现，启动时通过 use_saved_cookies 复用。
    if (c.get("cookies") or "").strip():
        c["cookies"] = SAVED_COOKIES_MASK
    return c


# ------------------------------ 错误诊断 ------------------------------
_ERROR_RULES = [
    (("auth_token", "ct0", "login", "authexception", "cookie", "登录", "登入",
      "username", "has no attribute", "http 401", "http 403", "invalidcookie"), "cookie",
     "Cookie 无效或过期",
     ("重新登录小号后复制最新 Cookie",
      "打开 https://x.com → F12 → Application → Cookies 复制 auth_token 与 ct0",
      "每行需同时包含 auth_token 和 ct0",
      "请使用小号，勿用主号（有风控风险）")),
    (("找不到用户", "user_by_login", "not found", " 404"), "notfound",
     "目标用户不存在",
     ("检查用户名拼写（无需 @ 前缀）", "确认该账号存在且未被封禁", "关键词模式请核对关键词/标签格式")),
    (("429", "rate limit", "too many requests", "限流"), "ratelimit",
     "触发限流",
     ("等待 15 分钟后重试", "增加小号数量（每行一个）提升配额",
      "调大请求间隔（建议 ≥2 秒）", "为每个账号配置独立 |proxy= 代理 IP")),
    (("proxy", "timeout", "timed out", "connection", "connect", "network", "网络", "代理"), "network",
     "网络 / 代理异常",
     ("确认本机代理已开启（Clash / v2rayN 等）", "检查代理地址与端口是否正确",
      "尝试更换代理节点或关闭代理直连", "检查本机网络连接")),
    (("unauthorized", "forbidden", "api_key", "invalid api key",
      "insufficient", "chat/completions"), "llm",
     "LLM API 配置问题",
     ("检查 API Key 是否正确、未过期", "确认 Base URL 与模型名匹配（如 DeepSeek 用 deepseek-chat）",
      "确认账号余额充足", "本地 Ollama 需先启动服务")),
    (("permission", "denied", "拒绝访问", "access"), "permission",
     "文件 / 权限问题",
     ("确认程序目录可写（勿放在 Program Files 等只读目录）", "可尝试以管理员身份运行")),
    (("memoryerror", "memory error", "内存"), "memory",
     "内存不足",
     ("缩小日期范围或抓取量", "关闭其他占用内存的程序后重试")),
]


def _local_diagnose(err_text: str) -> dict:
    """根据错误文本做本地规则分类，返回 {category, title, guide}。"""
    low = (err_text or "").lower()
    for keywords, cat, title, guide in _ERROR_RULES:
        if any(k in low for k in keywords):
            return {"category": cat, "title": title, "guide": list(guide)}
    return {"category": "other", "title": "未知错误",
            "guide": ["查看上方详细错误信息", "可重新运行任务观察是否复现",
                      "若持续出现，可勾选「保存配置」后再试或检查网络/代理"]}


def _spawn_llm_diagnose(cfg: dict, err_text: str, info: dict) -> None:
    """后台调用 LLM 分析错误原因（配置了 LLM 时）；失败静默降级为本地规则。"""
    if not (cfg.get("llm_api_key") and cfg.get("llm_base_url")):
        return

    def _run():
        try:
            asyncio.run(_llm_diagnose(cfg, err_text, info))
        except Exception:
            pass

    threading.Thread(target=_run, daemon=True).start()


async def _llm_diagnose(cfg: dict, err_text: str, info: dict) -> None:
    from llm_analyzer import chat
    prompt = (
        "你是软件「推特用户画像生成器」的支持工程师。该软件抓取 X/Twitter 推文并用 LLM 生成画像，"
        "运行报错，请定位最可能原因并给出可操作建议。\n"
        f"错误信息：{err_text}\n"
        f"初步判断：{info.get('title')}（{info.get('category')}）\n"
        "请用简洁中文回复：1) 最可能的原因；2) 3-5 条可操作步骤。不要泛泛而谈。"
    )
    advice = await chat(cfg.get("llm_base_url", ""), cfg.get("llm_api_key", ""),
                        cfg.get("llm_model", ""),
                        [{"role": "system", "content": "你是资深软件支持工程师，回答简洁、专业、可操作。"},
                         {"role": "user", "content": prompt}],
                        timeout=90)
    PROGRESS.set_error_llm(advice)


def write_result(stats, profile, files: dict, preset: str = ""):
    data = {"stats": stats, "profile": profile, "files": files,
            "preset": preset,
            "generated_at": datetime.now(timezone.utc).isoformat()}
    rf = result_file()
    os.makedirs(os.path.dirname(rf), exist_ok=True)
    # 唯一 tmp 名（与 save_config/Progress.write 一致）：避免并发或上次残留的
    # 同名 .tmp 互相覆盖/被误清理
    tmp = rf + f".{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, rf)
    finally:
        # os.replace 成功后 tmp 已不存在；失败路径清理残留临时文件
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


async def _probe_connect(proxy_url: str, host: str = "x.com", port: int = 443,
                         timeout: float = 3.0):
    """经 HTTP(S) 代理建 CONNECT 隧道到 host:port 验证链路可用；socks5 跳过。
    返回 (ok, err)。死节点/直连被墙时隧道握手会挂起 → 快速识别而非干等搜索超时。"""
    from urllib.parse import urlparse
    u = urlparse(proxy_url if "://" in proxy_url else "http://" + proxy_url)
    if (u.scheme or "").lower() == "socks5":
        return True, ""
    phost = u.hostname or "127.0.0.1"
    pport = u.port or 443
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(phost, pport), timeout=timeout)
        req = f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n"
        writer.write(req.encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        try:
            writer.close()
        except Exception:
            pass
        if b" 200" in line.split(b"\r\n")[0]:
            return True, ""
        return False, line.decode("utf-8", "replace").strip() or "代理拒绝连接"
    except asyncio.TimeoutError:
        return False, "连接超时（节点可能失效）"
    except Exception as e:
        return False, str(e)


async def _classify_account_state(eng) -> str | None:
    """根据 twscrape 账号池状态推断探测失败原因：
    - active=false（会话过期/被封）或 SearchTimeline 被锁约 15 分钟（已登出）→ "cookie"
    - SearchTimeline 短锁（限流 reset 时间）→ "ratelimited"
    - 其它 → None。
    twscrape 内部把 XClIdAccountError（登出）锁 15 分钟、429 限流锁到 reset 时间，
    调用方拿不到原始异常，只能从锁时长反推。"""
    try:
        from twscrape.utils import utc
        accs = await eng.pool.get_all()
        if not accs:
            return None
        acc = accs[0]
        if not getattr(acc, "active", True):
            return "cookie"
        locks = getattr(acc, "locks", {}) or {}
        lock = locks.get("SearchTimeline")
        if lock is not None:
            try:
                remaining = (lock - utc.now()).total_seconds()
            except Exception:
                remaining = 15 * 60
            if remaining >= 14 * 60:
                return "cookie"
            if remaining > 0:
                return "ratelimited"
        return None
    except Exception:
        return None


async def _test_scrape(cookies_text: str, proxy: str | None,
                       mode: str, target: str) -> dict:
    """抓取前快速验证：只取前 5 条推文，验证 Cookie/代理/账号/目标均可用。"""
    import scraper
    used_proxy = proxy or scraper.detect_proxy()
    if not used_proxy:
        return {"ok": False, "state": "noproxy",
                "message": "未检测到本地代理（常见端口均未监听）。请先启动代理或填写「全局代理」后再试。"}
    ok, err = await _probe_connect(used_proxy)
    if not ok:
        return {"ok": False, "state": "network",
                "message": f"代理 {used_proxy} 已监听但连不通 x.com（{err}）。请检查代理节点是否有效。"}
    eng = scraper.Scraper(cookies_text, proxy, delay=0.0, pool_wait_timeout=5.0)
    try:
        await eng.setup()
    except Exception as e:
        return {"ok": False, "state": "error", "message": f"账号初始化失败：{e}"}
    rows = []
    try:
        if mode == "user":
            user = await eng.api.user_by_login(target)
            if user is None:
                return {"ok": False, "state": "notfound",
                        "message": f"用户「{target}」不存在或无法访问"}
            async for tw in eng.api.user_tweets(user.id, limit=5):
                r = scraper.tweet_to_row(tw)
                if r is not None:
                    rows.append(r)
        else:
            async for tw in eng.api.search(target, limit=5):
                r = scraper.tweet_to_row(tw)
                if r is not None:
                    rows.append(r)
    except Exception as e:
        s = str(e).lower()
        if any(k in s for k in ("429", "rate limit", "too many requests", "限流")):
            return {"ok": False, "state": "ratelimited",
                    "message": "当前处于推特限速状态（HTTP 429）。建议：等待 15 分钟、增加小号数量、调大请求间隔。"}
        if scraper._is_auth_error(e):
            return {"ok": False, "state": "cookie",
                    "message": "Cookie 无效或过期（X 返回 401/403），请重新登录小号后更新 Cookie。"}
        return {"ok": False, "state": "error", "message": f"测试抓取异常：{e}"}
    if not rows:
        reason = await _classify_account_state(eng)
        if reason == "cookie":
            return {"ok": False, "state": "cookie",
                    "message": "Cookie 无效或过期（账号被 X 判定已登出），请重新登录小号后更新 Cookie。"}
        if reason == "ratelimited":
            return {"ok": False, "state": "ratelimited",
                    "message": "当前处于推特限速状态。建议：等待 15 分钟或增加小号数量。"}
        return {"ok": False, "state": "empty",
                "message": "目标没有返回任何推文（可能无内容、受保护或账号被限制）"}
    samples = [{"date": r.get("Date", ""),
                "content": (r.get("Content", "") or "")[:60]} for r in rows[:3]]
    return {"ok": True, "state": "ok", "count": len(rows),
            "message": f"测试成功：抓取到 {len(rows)} 条推文（Cookie/代理/目标均可用）",
            "samples": samples}


async def _check_cookies(accounts: list, proxy: str | None) -> dict:
    """逐个账号快速探测有效性（复用限流检测分类）。串行执行，避免账号池互相清空。"""
    results = []
    for i, acc in enumerate(accounts, 1):
        r = await _probe_ratelimit(acc.get("cookies", ""),
                                   acc.get("proxy") or proxy)
        results.append({
            "index": i,
            "ok": bool(r.get("ok")),
            "state": r.get("state", "error"),
            "message": r.get("message", ""),
        })
        if i < len(accounts):
            await asyncio.sleep(0.5)
    good = sum(1 for x in results if x["ok"])
    return {"ok": good == len(results), "good": good, "total": len(results),
            "results": results}


async def _probe_ratelimit(cookies_text: str, proxy: str | None) -> dict:
    """轻量探测当前是否处于推特限速状态：
    - 正常 → {"ok": True, "state": "ok"}
    - 未检测到代理 → {"ok": False, "state": "noproxy"}
    - 代理连不通 x.com → {"ok": False, "state": "network"}
    - 429/限流 → {"ok": False, "state": "ratelimited"}
    - cookie 失效 → {"ok": False, "state": "cookie"}
    - 网络/其它 → {"ok": False, "state": "error"}
    先预检代理可用性（无代理/节点失效会快速失败）；再发一次 limit=1 搜索。
    搜索用短 pool_wait_timeout：账号被 X 锁定（Cookie 失效/限流）时不干等冷却，
    空结果后按账号池锁状态精确分类。不落盘、不影响任务目录。"""
    import scraper
    used_proxy = proxy or scraper.detect_proxy()
    if not used_proxy:
        return {"ok": False, "state": "noproxy",
                "message": "未检测到本地代理（7897/7890 等常见端口均未监听）。国内直连 X 会被墙，请先启动代理（如 Clash）或填写「全局代理」后再试。"}
    ok, err = await _probe_connect(used_proxy)
    if not ok:
        return {"ok": False, "state": "network",
                "message": f"代理 {used_proxy} 已监听但连不通 x.com（{err}）。请检查代理节点是否有效。"}
    eng = scraper.Scraper(cookies_text, proxy, delay=0.0, pool_wait_timeout=5.0)
    try:
        await eng.setup()
    except Exception as e:
        return {"ok": False, "state": "error", "message": f"账号初始化失败：{e}"}
    got = 0
    try:
        async for _ in eng.api.search("x.com limit 1", limit=1):
            got += 1
            break
    except Exception as e:
        s = str(e).lower()
        if any(k in s for k in ("429", "rate limit", "too many requests", "限流")):
            return {"ok": False, "state": "ratelimited",
                    "message": "当前处于推特限速状态（HTTP 429）。建议：等待 15 分钟、增加小号数量、调大请求间隔、为账号配置独立 |proxy= 代理 IP。"}
        if scraper._is_auth_error(e):
            return {"ok": False, "state": "cookie",
                    "message": "Cookie 无效或过期（X 返回 401/403），请重新登录小号后更新 Cookie。"}
        return {"ok": False, "state": "error", "message": f"检测异常：{e}"}
    if got:
        return {"ok": True, "state": "ok", "message": "限流状态正常：可以开始抓取"}
    # 搜索无结果：账号大概率被 X 标记，按账号池锁状态精确分类
    reason = await _classify_account_state(eng)
    if reason == "cookie":
        return {"ok": False, "state": "cookie",
                "message": "Cookie 无效或过期（账号被 X 判定已登出），请重新登录小号后更新 Cookie。"}
    if reason == "ratelimited":
        return {"ok": False, "state": "ratelimited",
                "message": "当前处于推特限速状态。建议：等待 15 分钟、增加小号数量、调大请求间隔、为账号配置独立 |proxy= 代理 IP。"}
    return {"ok": False, "state": "error",
            "message": "检测异常：搜索未返回任何结果且账号状态未知，请稍后重试。"}


# 支持的操作类型（把「抓取 / 导出 / 导入 / LLM 总结」拆分为独立功能）
ACTION_LABELS = {
    "full": "完整流程（抓取+清洗+LLM）",
    "scrape": "仅抓取（清洗统计，不调 LLM）",
    "import": "导入数据",
    "analyze": "仅 LLM 总结",
}


def run_pipeline(cfg: dict, stop_event: threading.Event):
    """在后台线程中执行的流程（stop_event 为该任务独立的停止信号）。
    根据 cfg[\"action\"] 决定只抓取、只总结或完整流程。"""
    seq = _next_seq()
    action = cfg.get("action") or "full"
    # 提前记录任务信息并清空旧事件，避免后续 set_task_info 清掉“防休眠”等日志
    PROGRESS.set_task_info((cfg.get("mode") or "user").strip(),
                           (cfg.get("target") or "").strip())
    PROGRESS.set_action(action)
    # 防休眠与自动关机是两个独立选项
    keep_on = bool(cfg.get("keep_awake", True))
    PROGRESS.set_keep_awake(keep_on)
    if keep_on:
        keep_awake(True)
        PROGRESS.log("任务开始：已启用防休眠（系统不休眠，允许息屏）")
    else:
        PROGRESS.log("任务开始：未启用防休眠")

    def report(stage: str, **kw):
        if stop_event.is_set():
            raise StopRequested()
        PROGRESS.report(stage, **kw)

    try:
        asyncio.run(_pipeline(cfg, report, stop_event))
    except StopRequested:
        # 仅当仍是当前任务时才写状态，避免覆盖新启动的任务
        if seq == _TASK_SEQ:
            PROGRESS.set_status("idle")
            PROGRESS.report("scrape", detail="已停止")
    except Exception as e:
        if seq == _TASK_SEQ:
            PROGRESS.set_status("error", error=str(e))
            PROGRESS.report("scrape", detail=f"失败：{e}")
            # 本地规则诊断 + 后台 LLM 增强诊断
            info = _local_diagnose(str(e))
            PROGRESS.set_error_info(info)
            _spawn_llm_diagnose(cfg, str(e), info)
            # 错误现场自动打包：诊断文件落盘到任务目录
            _write_error_diag(cfg, str(e), info)
    finally:
        if seq == _TASK_SEQ:
            if keep_on:
                keep_awake(False)
                PROGRESS.log("防休眠已恢复")
            # 任务结束系统气泡通知（完成/出错；可在界面勾选关闭）
            if cfg.get("notify"):
                st = PROGRESS.data["status"]
                tgt = (cfg.get("target") or "").strip()
                if st == "done":
                    notify("任务完成", f"{tgt or '目标'} · {ACTION_LABELS.get(action, action)} 全部完成")
                elif st == "error":
                    notify("任务失败", f"{tgt or '目标'} · {str(PROGRESS.data['error'] or '出错')[:80]}")
            if PROGRESS.data["status"] == "done":
                # 输出内容自动保存：归档到 history/<时间戳>/，历史可追溯
                arc = _archive_output()
                if arc:
                    PROGRESS.set_last_archive(arc)
                    PROGRESS.log(f"输出已自动保存：{arc}")
                # 仅在成功完成后自动关机；出错不关机，避免误关整机
                if cfg.get("auto_shutdown"):
                    _trigger_shutdown()


# 历史归档保留份数（每次任务完成自动保存一次输出，超出清理最旧）
_ARCHIVE_KEEP = 20


def _archive_output() -> str | None:
    """任务成功后，把本次输出（清洗 CSV/简约整理/画像/结果）自动归档到
    output/<任务>/history/<时间戳>/ 下，避免被下次运行覆盖；只保留最近 N 份。"""
    out = paths.out_dir()
    if not os.path.isdir(out):
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(out, "history", ts)
    copied = 0
    for name in ("clean_tweets.csv", "tweets_digest.txt", "profile.md", "result.json"):
        src = os.path.join(out, name)
        if os.path.exists(src):
            os.makedirs(dest, exist_ok=True)
            try:
                shutil.copy2(src, os.path.join(dest, name))
                copied += 1
            except Exception:
                pass
    # 清理：仅保留最近 N 份历史（异常时忽略，不阻断任务）
    try:
        hist = os.path.join(out, "history")
        if os.path.isdir(hist):
            dirs = sorted(d for d in os.listdir(hist)
                          if os.path.isdir(os.path.join(hist, d)))
            for d in dirs[:-_ARCHIVE_KEEP]:
                shutil.rmtree(os.path.join(hist, d), ignore_errors=True)
    except Exception:
        pass
    return dest if copied else None


def _write_error_diag(cfg: dict, err_text: str, info: dict | None) -> None:
    """任务失败时把现场信息（错误/诊断/事件日志/任务目录文件清单）落盘为诊断文件。"""
    try:
        out = paths.out_dir()
        os.makedirs(out, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(out, f"错误诊断_{ts}.txt")
        action = ACTION_LABELS.get(cfg.get("action") or "full", cfg.get("action") or "full")
        lines = [f"错误诊断 · {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                 f"模式: {(cfg.get('mode') or 'user').strip()}  目标: {(cfg.get('target') or '').strip()}  操作: {action}",
                 f"错误: {err_text}", ""]
        if isinstance(info, dict):
            lines.append("分类: " + str(info.get("category", "")))
            lines.append("标题: " + str(info.get("title", "")))
            if info.get("guide"):
                lines.append("建议:")
                for g in info["guide"]:
                    lines.append("  - " + str(g))
            if info.get("llm_advice"):
                lines.append("LLM 建议: " + str(info["llm_advice"]))
            lines.append("")
        lines.append("---- 事件日志 ----")
        for e in PROGRESS.snapshot().get("events", []):
            lines.append(f"[{e.get('t','')}] {e.get('msg','')}")
        lines.append("")
        lines.append("---- 任务目录文件 ----")
        try:
            for fn in sorted(os.listdir(out)):
                p = os.path.join(out, fn)
                if os.path.isfile(p):
                    lines.append(f"- {fn} ({os.path.getsize(p)} B)")
        except Exception:
            pass
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        PROGRESS.log(f"错误现场已保存：{path}")
    except Exception:
        pass


def _trigger_shutdown() -> None:
    """运行结束后按配置触发自动关机（延迟 60 秒，界面可取消）。"""
    if schedule_shutdown(60):
        PROGRESS.set_auto_shutdown(True, 60)
        PROGRESS.log("运行结束，系统将于 60 秒后自动关机（可在界面取消）")
    else:
        PROGRESS.log("自动关机启动失败（仅 Windows 支持）")


def _calc_progress(d: dict) -> int:
    """按线性流程估算整体进度（0-100），供前端进度图表展示：
    抓取(5-60%) → 清洗(62%) → LLM 批次(65-100%)。入参为 Progress.snapshot()。"""
    status = d.get("status")
    if status == "done":
        return 100
    if status != "running":
        return 0
    stage = d.get("stage", "scrape")
    sc = d.get("scrape") or {}
    an = d.get("analyze") or {}
    if stage == "scrape":
        total = sc.get("months_total") or 0
        done = sc.get("months_done") or 0
        if total:
            return int(5 + 55 * min(done, total) / total)
        return 5   # 近端抓取阶段（无月份目标）：显示进行中
    if stage == "process":
        return 62
    if stage == "analyze":
        t = an.get("total") or 0
        b = an.get("batch") or 0
        if t:
            return int(65 + 35 * min(b, t) / t)
        return 65
    return 5


async def _pipeline(cfg: dict, report, stop_event=None):
    import processor
    import scraper
    import llm_analyzer

    # 先切到任务隔离目录，保证本任务所有读写都在 output/<task_key>/ 下
    paths.set_task_key(task_key(cfg))
    PROGRESS.set_status("running")
    action = cfg.get("action") or "full"

    cookies = cfg.get("cookies", "")
    proxy = cfg.get("proxy") or None
    mode = cfg.get("mode", "user")
    target = cfg.get("target", "").strip()
    PROGRESS.log(f"任务：{mode} 模式 · 目标「{target}」· 操作：{ACTION_LABELS.get(action, action)}")
    start = cfg.get("start_date") or None
    end = cfg.get("end_date") or None
    force_recent = bool(cfg.get("force_recent"))
    try:
        delay = float(cfg.get("delay", 2.0))
    except (TypeError, ValueError):
        delay = 2.0

    def _parse_date(s):
        try:
            return date(*[int(x) for x in s.split("-")])
        except Exception:
            return None

    sd = _parse_date(start)
    ed = _parse_date(end)
    if sd and ed and sd > ed:
        raise ValueError("起始日期不能晚于结束日期")

    # ---- 1) 抓取（仅 full / scrape 需要）----
    if action in ("full", "scrape"):
        PROGRESS.set_stage("scrape", "初始化抓取…")
        eng = scraper.Scraper(cookies, proxy, delay=delay, report=report,
                              stop_event=stop_event)
        await eng.setup()
        if mode == "user":
            await eng.scrape_user(target, sd, ed, force_recent)
        else:
            await eng.scrape_keyword(target, sd, ed)

    # ---- 2) 清洗统计（所有操作都会执行：生成 clean CSV + 简约整理 + 统计）----
    PROGRESS.set_stage("process", "清洗与统计…")
    PROGRESS.log("开始清洗与统计…")
    df = processor.process(start, end, report=report)
    stats = processor.compute_stats(df)
    PROGRESS.log(f"统计完成：{stats.get('total', 0)} 条推文")

    # ---- 3) LLM 画像（仅 full / analyze 需要）----
    profile = ""
    if action in ("full", "analyze"):
        llm_cfg = {"base_url": cfg.get("llm_base_url", ""),
                   "api_key": cfg.get("llm_api_key", ""),
                   "model": cfg.get("llm_model", "")}
        if llm_cfg.get("api_key") and llm_cfg.get("base_url"):
            PROGRESS.set_stage("analyze", "LLM 分析中…")
            preset = cfg.get("preset") or llm_analyzer.DEFAULT_PRESET
            PROGRESS.log(f"画像预设：{llm_analyzer.PRESETS.get(preset, {}).get('label', preset)}")
            profile = await llm_analyzer.analyze(df, stats, llm_cfg, preset=preset,
                                                 report=report, stop_event=stop_event)
        else:
            PROGRESS.log("未配置 LLM（API Key / Base URL 为空），跳过画像生成；"
                         "可稍后选择「仅 LLM 总结」在已有数据上生成画像")
    else:
        PROGRESS.log("本操作不生成 LLM 画像（结果统计与整理已导出）")

    write_result(stats, profile, {
        "clean_csv": os.path.join(paths.out_dir(), "clean_tweets.csv"),
        "profile_md": llm_analyzer.profile_file(),
        "digest_txt": processor.digest_file(),
    }, preset=cfg.get("preset") or llm_analyzer.DEFAULT_PRESET)
    PROGRESS.set_stage("process", "完成")
    PROGRESS.set_status("done")
    PROGRESS.log("全部完成")
    print("[app] 流程完成")


# ------------------------------ HTTP 服务 ------------------------------
class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        # 本地服务也需要防慢速/半开请求长期占用线程。
        self.connection.settimeout(30)

    @staticmethod
    def _read_content_length(raw_length, limit: int) -> int | None:
        try:
            length = int(raw_length)
        except (TypeError, ValueError):
            return None
        if length < 0 or length > limit:
            return None
        return length

    def _send(self, code, body: bytes, ctype: str):
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass  # 客户端提前断开，忽略

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8")

    def _origin_allowed(self) -> bool:
        """CSRF 纵深防御：只允许本机来源（127.0.0.1/localhost）或非浏览器请求。"""
        origin = self.headers.get("Origin") or self.headers.get("Referer") or ""
        if not origin:
            return True
        try:
            host = (urlparse(origin).hostname or "").lower()
        except Exception:
            return False
        return host in ("127.0.0.1", "localhost", "::1", "[::1]")

    def _host_allowed(self) -> bool:
        """校验 Host 头为本机，防 DNS rebinding 读取敏感接口。"""
        host = (self.headers.get("Host") or "").split(":")[0].strip("[]").lower()
        return host in ("127.0.0.1", "localhost", "::1")

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                with open(os.path.join(STATIC_DIR, "index.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except OSError:
                self._send(404, "index.html 缺失".encode("utf-8"), "text/plain; charset=utf-8")
        elif path == "/api/status":
            # 取一致快照后再读取/序列化，避免任务线程并发修改 data 导致
            # json.dumps RuntimeError 或读到半更新状态
            d = PROGRESS.snapshot()
            # 由后端计算关机剩余秒数，避免前端时区/解析问题
            shutdown_remaining = None
            if d["auto_shutdown"] and d["shutdown_in"] is not None:
                try:
                    up = datetime.fromisoformat(d["updated_at"])
                    elapsed = (datetime.now(timezone.utc) - up).total_seconds()
                    shutdown_remaining = max(0, int(d["shutdown_in"] - elapsed))
                except Exception:
                    shutdown_remaining = d["shutdown_in"]
            self._json({
                "running": d["status"] == "running",
                "status": d["status"],
                "stage": d["stage"],
                "detail": d["detail"],
                "mode": d["mode"],
                "target": d["target"],
                "action": d.get("action", ""),
                "progress": _calc_progress(d),
                "started_at": d["started_at"],
                "finished_at": d["finished_at"],
                "auto_shutdown": d["auto_shutdown"],
                "shutdown_in": d["shutdown_in"],
                "shutdown_remaining": shutdown_remaining,
                "keep_awake": d.get("keep_awake", False),
                "last_archive": d.get("last_archive", ""),
                "stopping": d["stopping"],
                "waiting": d["waiting"],
                "waiting_detail": d["waiting_detail"],
                "events": d["events"],
                "scrape": d["scrape"],
                "process": d["process"],
                "analyze": d["analyze"],
                "error": d["error"],
                "error_info": d["error_info"],
                "result_exists": os.path.exists(result_file()),
                # 数据目录信息：前端展示「隐私数据与获得的数据统一存放位置」
                "data_dir": paths.user_dir(),
                "output_dir": paths.output_root(),
            })
        elif path == "/api/output_dir":
            if not self._origin_allowed() or not self._host_allowed():
                self._send(403, b"Forbidden", "text/plain")
                return
            self._json({"output_dir": paths.output_root(),
                        "data_dir": paths.user_dir()})
        elif path == "/api/open_dir":
            # 在文件管理器中打开用户数据目录 / 输出目录（仅本机）
            if not self._origin_allowed() or not self._host_allowed():
                self._send(403, b"Forbidden", "text/plain")
                return
            q = urllib.parse.parse_qs(urlparse(self.path).query).get("d", [""])[0]
            target = paths.user_dir() if q == "data" else paths.output_root()
            if os.path.isdir(target):
                try:
                    if os.name == "nt":
                        os.startfile(target)  # type: ignore[attr-defined]
                    elif sys.platform == "darwin":
                        subprocess.Popen(["open", target])
                    else:
                        subprocess.Popen(["xdg-open", target])
                    self._json({"ok": True})
                    return
                except Exception as e:
                    self._json({"error": f"打开失败：{e}"})
                    return
            self._json({"error": "目录不存在"}, 404)
        elif path == "/api/config":
            if not self._origin_allowed() or not self._host_allowed():
                self._send(403, b"Forbidden", "text/plain")
                return
            q = urllib.parse.parse_qs(urlparse(self.path).query)
            name = _sanitize_profile(q.get("name", [""])[0])
            self._json({"config": mask_config(load_config(name)),
                        "name": name or "default"})
        elif path == "/api/configs":
            if not self._origin_allowed() or not self._host_allowed():
                self._send(403, b"Forbidden", "text/plain")
                return
            self._json({"configs": list_configs()})
        elif path == "/api/recent":
            if not self._origin_allowed() or not self._host_allowed():
                self._send(403, b"Forbidden", "text/plain")
                return
            self._json({"recent": _load_recent()})
        elif path == "/docs":
            # 工作原理文档：优先读打包内的 static/工作原理.md，兼容运行目录
            doc_path = os.path.join(STATIC_DIR, "工作原理.md")
            if not os.path.exists(doc_path):
                doc_path = os.path.join(paths.base_dir(), "工作原理.md")
            if os.path.exists(doc_path):
                try:
                    with open(doc_path, "r", encoding="utf-8") as f:
                        self._send(200, f.read().encode("utf-8"),
                                   "text/markdown; charset=utf-8")
                except OSError:
                    self._send(404, b"Not Found", "text/plain")
            else:
                self._send(404, "工作原理.md 缺失".encode("utf-8"),
                           "text/plain; charset=utf-8")
        elif path == "/api/result":
            if not self._origin_allowed() or not self._host_allowed():
                self._send(403, b"Forbidden", "text/plain")
                return
            rf = result_file()
            if os.path.exists(rf):
                try:
                    with open(rf, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    self._json({"error": "结果文件损坏，请重新运行任务"}, 500)
                    return
                self._json(data)
            else:
                self._json({"error": "暂无结果"}, 404)
        elif path == "/dl":
            if not self._origin_allowed() or not self._host_allowed():
                self._send(403, b"Forbidden", "text/plain")
                return
            f = urllib.parse.parse_qs(urlparse(self.path).query).get("f", [""])[0]
            if f == "zip":
                # 结果一键打包：内存生成 zip（CSV + 画像 + 简约整理 + 结果 JSON）
                import io, zipfile
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
                    for name in ("clean_tweets.csv", "profile.md", "tweets_digest.txt", "result.json"):
                        p = os.path.join(paths.out_dir(), name)
                        if os.path.exists(p):
                            z.write(p, arcname=name)
                body = buf.getvalue()
                if body:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Disposition",
                                     'attachment; filename="twitter_profile_result.zip"')
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                else:
                    self._send(404, b"Not Found", "text/plain")
                return
            target = {"clean": os.path.join(paths.out_dir(), "clean_tweets.csv"),
                      "md": os.path.join(paths.out_dir(), "profile.md"),
                      "digest": os.path.join(paths.out_dir(), "tweets_digest.txt")}.get(f)
            if target and os.path.exists(target):
                with open(target, "rb") as fh:
                    body = fh.read()
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Disposition",
                                 f'attachment; filename="{os.path.basename(target)}"')
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self._send(404, b"Not Found", "text/plain")
        else:
            self._send(404, b"Not Found", "text/plain")

    def do_POST(self):
        # 与 GET 敏感接口一致：同时校验 Origin（CSRF）与 Host（防 DNS rebinding）
        if not self._origin_allowed() or not self._host_allowed():
            self._send(403, b"Forbidden", "text/plain")
            return
        path = urlparse(self.path).path
        try:
            # 导入接口可携带较大 CSV 内容，放宽限制；其余接口保持 2MB
            limit = 32 * 1024 * 1024 if path == "/api/import" else 2 * 1024 * 1024
            raw_length = self.headers.get("Content-Length")
            if raw_length is None:
                self._json({"error": "缺少 Content-Length"}, 411)
                return
            length = self._read_content_length(raw_length, limit)
            if length is None:
                self._json({"error": "Content-Length 非法或请求体过大"}, 400)
                return
            body = self.rfile.read(length)
            if len(body) != length:
                self._json({"error": "请求体不完整"}, 400)
                return
            data = json.loads(body.decode("utf-8"))
            if not isinstance(data, dict):
                self._json({"error": "请求体必须是 JSON 对象"}, 400)
                return
        except Exception:
            self._json({"error": "请求体解析失败"}, 400)
            return

        if path == "/api/start":
            action = data.get("action") or "full"
            if action not in ("full", "scrape", "analyze", "import"):
                self._json({"error": "非法操作"})
                return
            if action == "import":
                self._json({"error": "导入请使用文件选择后点「导入并清洗」"})
                return
            if not data.get("target"):
                self._json({"error": "缺少目标对象"})
                return
            # 表单未填 cookie 但标记了使用已保存配置时，从配置读取
            data["cookies"] = _resolve_cookies(data)
            # LLM API Key 同样支持复用已保存值（掩码后不强制重输）
            if not data.get("llm_api_key") and data.get("use_saved_key"):
                saved = load_config(data.get("profile") or "")
                data["llm_api_key"] = (saved.get("llm_api_key") or "").strip()
            # 抓取类操作必须要有 cookie；LLM 总结必须要有 API Key；
            # 其余操作在不提供 API Key 的前提下也能工作
            if action in ("full", "scrape") and not data.get("cookies"):
                self._json({"error": "缺少 cookie（抓取必需）"})
                return
            if action == "analyze" and not data.get("llm_api_key"):
                self._json({"error": "「仅 LLM 总结」需要填写 LLM API Key"})
                return
            mode = data.get("mode")
            if mode is not None and mode not in ("user", "keyword"):
                self._json({"error": "非法模式"})
                return
            # 新任务开始：无条件取消可能已排定的自动关机。
            # 不能仅凭 auto_shutdown 标志判断——任务 done 到真正排定关机之间
            # 存在小窗口（先 schedule_shutdown 后置标志），否则新任务会在
            # 60 秒后被整机关机；shutdown /a 无排定时返回非 0，无副作用。
            cancel_shutdown()
            PROGRESS.set_auto_shutdown(False, None)
            with LOCK:
                if PROGRESS.data["status"] == "running":
                    self._json({"error": "已有任务在运行"})
                    return
                # 在启动线程前先置为 running，避免并发 start 竞态
                PROGRESS.set_status("running")
            try:
                # 必须带 profile：用户选命名配置时，表单偏好应存到对应文件，
                # 而不是误写入默认 user_config.json
                save_config(_strip_meta(data), _sanitize_profile(data.get("profile") or ""))
            except Exception as e:
                # 配置保存失败不阻断任务启动，仅记录
                PROGRESS.log(f"配置保存失败（忽略）：{e}")
            stop_event = threading.Event()
            _set_current_stop(stop_event)
            # 记录最近使用目标（供前端快速回填）
            _record_recent(data)
            threading.Thread(target=run_pipeline, args=(data, stop_event), daemon=True).start()
            self._json({"ok": True})
        elif path == "/api/import":
            # 导入已有 CSV：写入当前任务目录后走「清洗统计」，无需抓取与 LLM
            target = (data.get("target") or "").strip()
            if not target:
                self._json({"error": "缺少目标对象"})
                return
            mode = data.get("mode")
            if mode is not None and mode not in ("user", "keyword"):
                self._json({"error": "非法模式"})
                return
            files = data.get("files")
            if not isinstance(files, dict) or not files:
                self._json({"error": "缺少导入的 CSV 文件"})
                return
            with LOCK:
                if PROGRESS.data["status"] == "running":
                    self._json({"error": "已有任务在运行"})
                    return
                PROGRESS.set_status("running")
            # 与 /api/start 一致：导入也会启动新任务，必须取消可能已排定的
            # 自动关机，否则上一任务 done 后 60s 内导入会被整机关机打断
            cancel_shutdown()
            PROGRESS.set_auto_shutdown(False, None)
            try:
                data = dict(data)
                data["action"] = "import"
                data["cookies"] = data.get("cookies") or ""
                paths.set_task_key(task_key(data))
                os.makedirs(paths.out_dir(), exist_ok=True)
                written = []
                for fname, content in files.items():
                    base = _safe_import_filename(fname)
                    # 仅允许普通 .csv 文件名；拒绝隐藏文件、保留设备名与自身清洗结果，
                    # 避免 Windows 规范化后覆盖/污染任务数据
                    if base is None or base in written:
                        continue
                    try:
                        with open(os.path.join(paths.out_dir(), base), "w",
                                  encoding="utf-8-sig", newline="") as f:
                            f.write(str(content))
                        written.append(base)
                    except Exception:
                        continue
                if not written:
                    PROGRESS.set_status("idle")
                    self._json({"error": "没有写入任何 CSV 文件（仅支持 .csv）"})
                    return
                PROGRESS.log(f"已导入 {len(written)} 个 CSV 文件：{', '.join(written)}")
                stop_event = threading.Event()
                _set_current_stop(stop_event)
                _record_recent(data)
                threading.Thread(target=run_pipeline, args=(data, stop_event), daemon=True).start()
                self._json({"ok": True, "imported": written})
            except Exception as e:
                PROGRESS.set_status("idle")
                self._json({"error": f"导入失败：{e}"}, 500)
        elif path == "/api/ratelimit":
            # 检测当前是否处于推特限速状态（轻量探测，不落盘）
            # 探测会重建共享 accounts.db；整个探测期间持锁，避免与 start/import
            # 交错，否则探测可能清掉正在运行任务的账号池。
            with LOCK:
                if PROGRESS.data["status"] == "running":
                    self._json({"error": "任务运行中，请结束后再检测限流状态"})
                    return
                cookies = _resolve_cookies(data)
                if not cookies:
                    self._json({"error": "缺少 cookie：请填写 Cookies 输入框，或标记使用已保存 Cookie"})
                    return
                try:
                    proxy = data.get("proxy") or None
                    # 探测放独立线程 + 硬超时 join：asyncio.wait_for 取消后事件循环
                    # 清理仍可能拖更久，这里保证接口最多约 15 秒返回（后台线程 daemon 收尾）
                    box: dict = {}
                    finished = threading.Event()

                    def _probe_worker():
                        try:
                            async def _wrapped():
                                return await asyncio.wait_for(
                                    _probe_ratelimit(cookies, proxy), timeout=12)
                            box["result"] = asyncio.run(_wrapped())
                        except asyncio.TimeoutError:
                            box["result"] = {"ok": False, "state": "timeout",
                                             "message": "检测超时（12 秒）：X 未在时限内响应，请检查代理节点或稍后再试。"}
                        except Exception as e:
                            box["error"] = str(e) or "未知错误"
                        finally:
                            finished.set()

                    threading.Thread(target=_probe_worker, daemon=True).start()
                    if not finished.wait(15):
                        self._json({"ok": False, "state": "timeout",
                                    "message": "检测超时（15 秒）：请检查代理节点/网络后重试"})
                    elif "error" in box:
                        self._json({"error": f"限流检测失败：{box['error']}"}, 500)
                    else:
                        self._json(box["result"])
                except Exception as e:
                    self._json({"error": f"限流检测失败：{e}"}, 500)
        elif path == "/api/test_scrape":
            # 抓取前快速验证：Cookie/代理/目标是否可用（只抓前 5 条）
            with LOCK:
                if PROGRESS.data["status"] == "running":
                    self._json({"error": "任务运行中，请结束后再测试"})
                    return
                cookies = _resolve_cookies(data)
                if not cookies:
                    self._json({"error": "缺少 cookie：请填写 Cookies 输入框，或标记使用已保存 Cookie"})
                    return
                mode = (data.get("mode") or "user").strip()
                target = (data.get("target") or "").strip().lstrip("@")
                if not target:
                    self._json({"error": "缺少目标对象"})
                    return
                proxy = data.get("proxy") or None
                box: dict = {}
                finished = threading.Event()

                def _worker():
                    try:
                        async def _wrapped():
                            return await asyncio.wait_for(
                                _test_scrape(cookies, proxy, mode, target), timeout=25)
                        box["result"] = asyncio.run(_wrapped())
                    except asyncio.TimeoutError:
                        box["result"] = {"ok": False, "state": "timeout",
                                         "message": "测试超时（25 秒）：请检查代理节点/网络后重试"}
                    except Exception as e:
                        box["error"] = str(e) or "未知错误"
                    finally:
                        finished.set()

                threading.Thread(target=_worker, daemon=True).start()
                if not finished.wait(25):
                    self._json({"ok": False, "state": "timeout",
                                "message": "测试超时（25 秒）：请检查代理节点/网络后重试"})
                elif "error" in box:
                    self._json({"error": f"测试抓取失败：{box['error']}"}, 500)
                else:
                    self._json(box["result"])
        elif path == "/api/test_llm":
            # LLM 连通性测试：发一条极小的 chat 请求验证 Base URL + Key + 模型
            base_url = (data.get("base_url") or "").strip().rstrip("/")
            model = (data.get("model") or "").strip()
            api_key = (data.get("llm_api_key") or "").strip()
            if not api_key and data.get("use_saved_key"):
                saved = load_config(data.get("profile") or "")
                api_key = (saved.get("llm_api_key") or "").strip()
            if not base_url:
                self._json({"error": "请先填写 LLM API Base URL"})
                return
            if not model:
                self._json({"error": "请填写模型名"})
                return
            if not api_key:
                self._json({"error": "请填写 LLM API Key（或标记使用已保存的 Key）"})
                return
            try:
                import llm_analyzer
                reply = asyncio.run(llm_analyzer.chat(
                    base_url, api_key, model,
                    [{"role": "user", "content": "请只回复两个字母：OK"}],
                    timeout=20, retries=1))
                self._json({"ok": True, "reply": (reply or "").strip()[:200]})
            except Exception as e:
                s = str(e).lower()
                if any(k in s for k in ("401", "403", "unauthorized", "authentication", "invalid api key")):
                    self._json({"error": f"API Key 无效或无权访问（{e}）"})
                elif any(k in s for k in ("404", "model", "not found")):
                    self._json({"error": f"模型名或 Base URL 可能不正确：{e}"})
                else:
                    self._json({"error": f"LLM 测试失败：{e}"})
        elif path == "/api/check_cookies":
            # 逐个账号批量预检 Cookie 有效性（复用限流检测分类）
            with LOCK:
                if PROGRESS.data["status"] == "running":
                    self._json({"error": "任务运行中，请结束后再检查 Cookie"})
                    return
                cookies = _resolve_cookies(data)
                if not cookies:
                    self._json({"error": "缺少 cookie：请填写 Cookies 输入框，或标记使用已保存 Cookie"})
                    return
                proxy = data.get("proxy") or None
                import scraper
                accounts = scraper.parse_accounts(cookies, proxy)
                if not accounts:
                    self._json({"error": "未解析到有效 Cookie（每行需含 auth_token 和 ct0）"})
                    return
                box: dict = {}
                finished = threading.Event()

                def _worker():
                    try:
                        async def _wrapped():
                            return await asyncio.wait_for(
                                _check_cookies(accounts, proxy), timeout=60)
                        box["result"] = asyncio.run(_wrapped())
                    except asyncio.TimeoutError:
                        box["result"] = {"ok": False, "good": 0,
                                         "total": len(accounts),
                                         "results": [],
                                         "message": "检查超时（60 秒），部分账号未完成"}
                    except Exception as e:
                        box["error"] = str(e) or "未知错误"
                    finally:
                        finished.set()

                threading.Thread(target=_worker, daemon=True).start()
                if not finished.wait(60):
                    self._json({"ok": False, "good": 0, "total": len(accounts),
                                "results": [], "message": "检查超时（60 秒），部分账号未完成"})
                elif "error" in box:
                    self._json({"error": f"检查 Cookie 失败：{box['error']}"}, 500)
                else:
                    self._json(box["result"])
        elif path == "/api/models":
            # 自动识别当前 Base URL 下可用的模型列表（OpenAI 兼容 /models）
            base_url = (data.get("base_url") or "").strip().rstrip("/")
            api_key = (data.get("llm_api_key") or "").strip()
            if not api_key and data.get("use_saved_key"):
                saved = load_config(data.get("profile") or "")
                api_key = (saved.get("llm_api_key") or "").strip()
            if not base_url:
                self._json({"error": "请先填写 LLM API Base URL"})
                return
            if not api_key:
                self._json({"error": "请填写 LLM API Key（或标记使用已保存的 Key）"})
                return
            try:
                import httpx
                host = (urlparse(base_url).hostname or "").lower()
                # 本地 LLM（如 Ollama localhost）不走系统代理，避免被 Clash 劫持；
                # 外部 API 保留系统代理（国内访问 OpenAI 等被墙服务时需要）
                is_local = host in ("localhost", "127.0.0.1", "::1")
                resp = httpx.get(base_url + "/models",
                                 headers={"Authorization": f"Bearer {api_key}"},
                                 timeout=10, trust_env=not is_local)
                if resp.status_code in (401, 403):
                    self._json({"error": "API Key 无效或无权访问（HTTP 401/403），请检查 Key 与 Base URL"})
                    return
                resp.raise_for_status()
                obj = resp.json()
                models: list[str] = []
                if isinstance(obj, dict) and isinstance(obj.get("data"), list):
                    for m in obj["data"]:
                        if isinstance(m, dict) and m.get("id"):
                            models.append(str(m["id"]))
                elif isinstance(obj, list):  # 部分服务直接返回列表
                    for m in obj:
                        if isinstance(m, dict) and m.get("id"):
                            models.append(str(m["id"]))
                models = sorted(set(models))
                if not models:
                    self._json({"error": "接口未返回任何模型（该服务可能不支持 /models）"})
                    return
                self._json({"models": models, "base_url": base_url})
            except httpx.HTTPStatusError as e:
                self._json({"error": f"获取模型列表失败（HTTP {e.response.status_code}）：请确认 Base URL 是 OpenAI 兼容地址（如 …/v1）"})
            except Exception as e:
                self._json({"error": f"获取模型列表失败：{e}"})
        elif path == "/api/stop":
            _signal_stop()
            PROGRESS.set_stopping(True)
            PROGRESS.log("已请求停止：正在安全停止当前任务…")
            self._json({"ok": True})
        elif path == "/api/shutdown_cancel":
            if cancel_shutdown():
                PROGRESS.set_auto_shutdown(False, None)
                PROGRESS.log("已取消自动关机")
                self._json({"ok": True})
            else:
                self._json({"error": "取消自动关机失败（仅 Windows 支持）"})
        elif path == "/api/save_config":
            # 保存配置到当前所选配置文件；llm_api_key / cookies 为空且标记
            # 使用已保存值时，保留该配置已保存的对应值（避免掩码状态覆盖丢失）
            profile = _sanitize_profile(data.pop("profile", ""))
            try:
                old = load_config(profile)
                if not data.get("llm_api_key"):
                    if old.get("llm_api_key"):
                        data["llm_api_key"] = old["llm_api_key"]
                if not (data.get("cookies") or "").strip():
                    if data.get("use_saved_cookies") and old.get("cookies"):
                        data["cookies"] = old["cookies"]
                save_config(_strip_meta(data), profile)
                PROGRESS.log("配置已保存" + (f"（{profile}）" if profile else ""))
                self._json({"ok": True, "name": profile or "default"})
            except Exception as e:
                self._json({"error": f"保存配置失败：{e}"}, 500)
        elif path == "/api/clear_config":
            profile = _sanitize_profile(data.get("profile", ""))
            try:
                clear_config(profile)
                PROGRESS.log("已清除保存的配置" + (f"（{profile}）" if profile else ""))
                self._json({"ok": True})
            except Exception as e:
                self._json({"error": f"清除配置失败：{e}"}, 500)
        elif path == "/api/save_config_as":
            # 把当前表单另存为新的命名配置；新名不存在时才允许。
            # 表单未改动的已保存 Cookie/Key 从源配置继承，避免掩码状态丢凭据。
            data = dict(data)
            src = _sanitize_profile(data.get("profile", ""))
            new_name = _sanitize_profile(data.get("new_name", ""))
            data.pop("new_name", None)
            if not new_name:
                self._json({"error": "新配置名称无效（1-32 位中英文/数字/下划线/短横线）"}, 400)
            elif os.path.exists(config_path(new_name)):
                self._json({"error": f"配置「{new_name}」已存在，请选择后编辑或换名"})
            else:
                try:
                    old = load_config(src) or {}
                    if not data.get("llm_api_key") and old.get("llm_api_key"):
                        data["llm_api_key"] = old["llm_api_key"]
                    if not (data.get("cookies") or "").strip() and data.get("use_saved_cookies") and old.get("cookies"):
                        data["cookies"] = old["cookies"]
                    save_config(_strip_meta(data), new_name)
                    PROGRESS.log(f"配置已另存为（{new_name}）")
                    self._json({"ok": True, "name": new_name})
                except Exception as e:
                    self._json({"error": f"另存配置失败：{e}"}, 500)
        elif path == "/api/rename_config":
            # 重命名命名配置：校验后复制到新名再删旧文件
            data = dict(data)
            old_name = _sanitize_profile(data.get("profile", ""))
            new_name = _sanitize_profile(data.get("new_name", ""))
            if not old_name:
                self._json({"error": "默认配置不可重命名（可用「另存为…」新建）"})
            elif not new_name:
                self._json({"error": "新配置名称无效（1-32 位中英文/数字/下划线/短横线）"}, 400)
            elif new_name == old_name:
                self._json({"error": "新名称与当前相同"})
            elif os.path.exists(config_path(new_name)):
                self._json({"error": f"配置「{new_name}」已存在"})
            else:
                try:
                    cfg = load_config(old_name)
                    if not cfg:
                        self._json({"error": f"源配置「{old_name}」不存在"})
                    else:
                        save_config(cfg, new_name)
                        clear_config(old_name)
                        PROGRESS.log(f"配置已重命名（{old_name} → {new_name}）")
                        self._json({"ok": True, "name": new_name})
                except Exception as e:
                    self._json({"error": f"重命名失败：{e}"}, 500)
        elif path == "/api/pick_dir":
            # 弹出系统目录选择对话框（tkinter，独立线程 + 超时保护）。
            # 打包 windowed 模式下 tkinter 可能不可用 → 返回错误提示手动粘贴路径
            kind = data.get("kind") or "output"
            if PROGRESS.data["status"] == "running":
                self._json({"error": "任务运行中，请结束后再切换目录"})
                return
            res = {"path": ""}

            def _pick():
                try:
                    import tkinter as tk
                    from tkinter import filedialog
                    root = tk.Tk()
                    root.withdraw()
                    root.attributes("-topmost", True)
                    title = "选择输出目录" if kind == "output" else "选择目录"
                    p = filedialog.askdirectory(title=title, parent=root)
                    root.destroy()
                    res["path"] = p or ""
                except Exception as e:
                    res["error"] = str(e)

            t = threading.Thread(target=_pick, daemon=True)
            t.start()
            t.join(timeout=120)
            if t.is_alive():
                self._json({"error": "目录选择超时"})
                return
            if res.get("error"):
                self._json({"error": "无法弹出目录选择框（" + res["error"]
                            + "）：请直接在输入框粘贴目录路径后按回车"})
                return
            self._json({"ok": True, "path": res["path"],
                        "cancelled": not res["path"]})
        elif path == "/api/set_output_dir":
            # 设置输出目录（用户选择的位置）；任务运行中禁止切换，避免任务
            # 中途换目录导致数据分裂
            with LOCK:
                if PROGRESS.data["status"] == "running":
                    self._json({"error": "任务运行中，请结束后再切换输出目录"})
                    return
                p = paths.set_output_dir(data.get("output_dir", ""))
                PROGRESS.log("输出目录已切换：" + (p or "默认（用户数据目录/output）"))
                self._json({"ok": True, "output_dir": p})
        else:
            self._json({"error": "未知接口"}, 404)

    def log_message(self, *args):
        pass


def _pid_alive(pid: int) -> bool:
    """检查进程是否存活。无法判断（如权限不足）时保守视为存活，避免误抢占单实例锁。"""
    if os.name == "nt":
        try:
            k32 = ctypes.windll.kernel32
            # 显式声明签名：默认 restype 是 c_int，64 位下会把 HANDLE 截断为
            # 32 位（句柄高位丢失 → 误判/CloseHandle 错误句柄）
            k32.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
            k32.OpenProcess.restype = ctypes.c_void_p
            k32.GetExitCodeProcess.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_ulong)]
            k32.GetExitCodeProcess.restype = ctypes.c_int
            k32.CloseHandle.argtypes = [ctypes.c_void_p]
            k32.CloseHandle.restype = ctypes.c_int
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            h = k32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if not h:
                # ERROR_INVALID_PARAMETER(87)=进程不存在；其余（如 ACCESS_DENIED=5）保守视为存活
                return k32.GetLastError() != 87
            try:
                code = ctypes.c_ulong()
                if not k32.GetExitCodeProcess(h, ctypes.byref(code)):
                    return True
                return code.value == 259   # STILL_ACTIVE
            finally:
                k32.CloseHandle(h)
        except Exception:
            return True
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
    except Exception:
        return True


def _read_lock_pid(lock_path: str) -> int:
    """读取锁文件中的 PID；无法解析时返回 0。"""
    try:
        with open(lock_path, "r", encoding="utf-8") as f:
            return int(f.read().strip() or "0")
    except Exception:
        return 0


def _force_kill(pid: int) -> bool:
    """强制结束指定 PID 的进程（Windows 用 taskkill /F /T，其它平台 SIGKILL）。"""
    try:
        if os.name == "nt":
            r = subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True, timeout=15,
            )
            return r.returncode == 0
        os.kill(pid, 9)   # SIGKILL
        return True
    except Exception:
        return False


def _wait_pid_exit(pid: int, timeout: float = 8.0) -> bool:
    """轮询等待进程退出（被杀进程退出时会自行删除锁文件）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.2)
    return not _pid_alive(pid)


def _confirm_force(pid: int) -> bool:
    """有控制台（源码/批处理运行）时询问用户是否强制结束旧实例；无控制台直接拒绝。"""
    try:
        if not sys.stdin or not sys.stdin.isatty():
            return False
        ans = input(f"是否强制结束旧实例（PID {pid}）并启动新实例？[y/N]: ").strip().lower()
        return ans in ("y", "yes", "是")
    except Exception:
        return False


def _acquire_lock(lock_path: str, force: bool = False):
    """获取单实例锁；若锁文件残留（进程已死）则自动接管。
    force=True 时，若检测到存活实例则强制结束它并接管。返回句柄或 None。"""
    try:
        f = open(lock_path, "x", encoding="utf-8")
        f.write(str(os.getpid()))
        f.flush()
        return f
    except FileExistsError:
        pass
    # 锁已存在：读取 PID 判断是否残留
    pid = _read_lock_pid(lock_path)
    if pid > 0 and _pid_alive(pid):
        if force:
            print(f"[app] 检测到已有实例（PID {pid}），正在强制结束…")
            if _force_kill(pid) and _wait_pid_exit(pid):
                print(f"[app] 旧实例（PID {pid}）已结束")
            else:
                print(f"[app] 强制结束旧实例（PID {pid}）失败，无法接管运行锁")
                return None
        else:
            return None   # 确有实例在运行
    # 残留锁（进程已死）或旧实例已结束：删除后重新获取
    try:
        os.remove(lock_path)
        f = open(lock_path, "x", encoding="utf-8")
        f.write(str(os.getpid()))
        f.flush()
        return f
    except (FileExistsError, OSError):
        return None


def serve(port: int = 8001, open_browser: bool = True, force: bool = False) -> None:
    """启动 Web 服务；open_browser=True 时自动打开浏览器（独立软件体验）。
    force=True 时，若已有实例在运行则强制结束它并接管运行锁。"""
    # 恢复用户选择的输出目录（app_settings.json），并确保目录存在
    try:
        saved_out = paths._load_settings().get("output_dir", "")
        if saved_out and os.path.isdir(saved_out):
            paths.set_output_dir(saved_out)
    except Exception:
        pass
    paths.ensure_dirs()
    # 旧版本数据（散落在程序目录）迁移到统一用户数据目录
    try:
        moved = migrate_legacy_data()
        if moved:
            PROGRESS.log("检测到旧版数据，已自动迁移到统一用户目录："
                         + paths.user_dir())
    except Exception as e:
        print(f"[app] 旧数据迁移失败（忽略）：{e}")
    # 单实例锁：防止多开进程抢同一批账号；残留锁（进程已死）自动接管
    lock_path = os.path.join(paths.user_dir(), "运行锁.lock")
    lock_f = _acquire_lock(lock_path, force=force)
    if lock_f is None:
        pid = _read_lock_pid(lock_path)
        print(f"[app] 检测到已有实例在运行（PID {pid or '?'}），请勿重复启动（锁文件：{lock_path}）")
        if force:
            print("[app] 已尝试强制结束旧实例但失败，退出")
            return
        if _confirm_force(pid):
            print("[app] 正在强制结束旧实例…")
            if _force_kill(pid):
                _wait_pid_exit(pid)
            lock_f = _acquire_lock(lock_path, force=True)
            if lock_f:
                print("[app] 已强制结束旧实例并接管运行锁，继续启动")
        if lock_f is None:
            return
    try:
        server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        # 请求线程设为 daemon：Ctrl+C 退出时不被残留的半开连接阻塞
        server.daemon_threads = True
        print(f"[app] 服务已启动 http://127.0.0.1:{port}（Ctrl+C 退出）")
        if open_browser:
            threading.Timer(0.6, lambda: webbrowser.open(f"http://127.0.0.1:{port}")).start()
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[app] 已退出")
    except OSError as e:
        msg = f"[app] 启动失败：{e}（端口可能被占用）"
        print(msg)
        # windowed 打包后无控制台，把启动错误写入日志
        try:
            with open(os.path.join(paths.user_dir(), "软件错误.log"), "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass
    finally:
        if lock_f:
            try:
                lock_f.close()
                os.remove(lock_path)
            except Exception:
                pass


if __name__ == "__main__":
    _port = 8001
    _force = False
    for _a in sys.argv[1:]:
        if _a in ("--force", "-f"):
            _force = True
        elif _a.isdigit():
            _port = int(_a)
    serve(_port, open_browser=True, force=_force)
