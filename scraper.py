# -*- coding: utf-8 -*-
"""
通用推特抓取引擎
================
支持两种模式：
  1. 用户模式（username）：user_tweets（近3200主推文）+ user_tweets_and_replies（近3200含回复）
     + search 按月切片补历史（since/until，until 为排他）
  2. 关键词模式（keyword/标签）：search 按月切片抓取

特性：多账号自动轮换、每账号可独立代理 IP、429 限流自动等待、
     按月落盘断点续跑、进度上报（供 GUI 实时展示）。
"""
import asyncio
import csv
import os
import re
import socket
import time
from datetime import date, timedelta

import pandas as pd
from twscrape import API, AccountsPool

import paths
from app import StopRequested  # noqa: 由 app 统一管理的任务停止信号


def recent_main_csv() -> str:
    """user_tweets 主推文 CSV 路径（按当前任务隔离目录）。"""
    return os.path.join(paths.out_dir(), "recent_main.csv")


def recent_replies_csv() -> str:
    """user_tweets_and_replies 含回复 CSV 路径（按当前任务隔离目录）。"""
    return os.path.join(paths.out_dir(), "recent_replies.csv")


def keyword_csv() -> str:
    """关键词模式结果 CSV 路径（按当前任务隔离目录）。"""
    return os.path.join(paths.out_dir(), "keyword.csv")

COLUMNS = [
    "Date", "Link", "ID", "Author", "DisplayName", "Content", "Language",
    "Replies", "Retweets", "Likes", "Quotes", "Views",
    "Hashtags", "Cashtags", "Links", "Mentions",
    "MediaPhotos", "MediaVideos", "MediaGIFs", "Source",
    "InReplyToTweetId", "InReplyToScreenName", "IsQuote", "ConversationId",
    "IsRetweet",
]

# 常见本地代理端口（Clash / Clash Verge / v2rayN 等）
PROXY_CANDIDATES = [
    ("http", 7897), ("http", 7890), ("http", 7891),
    ("http", 10809), ("socks5", 10808), ("http", 8888), ("socks5", 1080),
]

# 自动选取起始日期的下限：X 搜索对 2010 年以前的数据支持有限，
# 且月份过多会拖慢整体进度；留空起始日期时不下探到该日期以前。
_AUTO_START_FLOOR = date(2010, 1, 1)


def detect_proxy() -> str | None:
    """探测本机常见代理端口，返回第一个可用的代理 URL（如 http://127.0.0.1:7897）。"""
    for scheme, port in PROXY_CANDIDATES:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.3)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return f"{scheme}://127.0.0.1:{port}"
    return None


_COOKIE_AUTH_RE = re.compile(r"auth_token=([^;\s]+)", re.IGNORECASE)
_COOKIE_CT0_RE = re.compile(r"ct0=([^;\s]+)", re.IGNORECASE)


def parse_accounts(text: str, global_proxy: str | None = None) -> list[dict]:
    """
    把多行 cookie 文本解析成 [{cookies, proxy}]。每行一个小号，支持三种填法：
      1. 直接两个值：token值 ct0值（空格/逗号/分号/制表符分隔，自动补前缀）
      2. 完整格式：auth_token=xxx; ct0=yyy
      3. 整段 Cookie 头：Cookie: auth_token=xxx; ct0=yyy; twid=zzz（自动提取）
    可选 |proxy= 指定独立出口；# 开头为注释。
    """
    result = []
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # 兼容粘贴的 "Cookie: ..." 头
        line = re.sub(r"^cookie\s*[:：]\s*", "", line, flags=re.IGNORECASE)
        proxy = global_proxy
        if "|proxy=" in line:
            line, _, tail = line.partition("|proxy=")
            proxy = tail.strip() or global_proxy
        auth = _COOKIE_AUTH_RE.search(line)
        ct0 = _COOKIE_CT0_RE.search(line)
        if auth and ct0:
            # 标准/整段格式：只保留这两个关键 cookie，避免多余内容影响登录
            result.append({"cookies": f"auth_token={auth.group(1)}; ct0={ct0.group(1)}",
                           "proxy": proxy})
            continue
        # 直接两个值：token值 ct0值（首段不含 "="，否则视为其它格式跳过）
        parts = [p for p in re.split(r"[\s,，;；|]+", line) if p]
        if len(parts) >= 2 and "=" not in parts[0]:
            result.append({"cookies": f"auth_token={parts[0]}; ct0={parts[1]}",
                           "proxy": proxy})
    return result


def count_csv_rows(path: str) -> int:
    """统计 CSV 数据行数（正确处理引号内换行）。"""
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return max(sum(1 for _ in csv.reader(f)) - 1, 0)


def _is_auth_error(e) -> bool:
    """X 鉴权失败（Cookie 无效/过期/被风控）特征。
    twscrape 在 Cookie 失效时会抛 HttpStatusError("HTTP 401") / ("HTTP 403")
    或 InvalidCookieException 等，需要向用户给出清晰提示而非隐藏或误判。
    """
    s = str(e).lower()
    return any(k in s for k in ("http 401", "http 403", "invalidcookie",
                                "authexception", "not logged in", "logged out"))


COOKIE_EXPIRED_MSG = "Cookie 无效或过期：X 返回 401/403，请更新小号 Cookie 后重试"


def _safe_join(items, attr=None, default=""):
    """安全拼接列表字段：items 为 None 或含 None 元素时跳过，attr 指定取属性。"""
    if not items:
        return default
    parts = []
    for it in items:
        if it is None:
            continue
        try:
            v = getattr(it, attr) if attr is not None else it
        except Exception:
            continue
        if v is not None and str(v):
            parts.append(str(v))
    return ", ".join(parts)


def tweet_to_row(t) -> dict:
    """twscrape Tweet → CSV 行（逐字段容错，单条异常不影响整体抓取）。
    部分 twscrape 版本中 rawContent 是属性且内部访问 retweetedTweet.user.username，
    Cookie 失效/用户被限制时该 user 可能为 None 而抛 AttributeError，这里统一兜底。
    """
    if t is None:
        return None
    try:
        content = t.rawContent or ""
    except Exception:
        content = ""
    try:
        media = t.media
    except Exception:
        media = None
    user = t.user if getattr(t, "user", None) is not None else None
    return {
        # 日期缺失时置空（清洗时按 NaT 剔除），避免伪造当前时间污染统计
        "Date": t.date.isoformat() if getattr(t, "date", None) else "",
        "Link": getattr(t, "url", "") or "",
        "ID": getattr(t, "id_str", ""),
        "Author": user.username if user else "",
        "DisplayName": user.displayname if user else "",
        "Content": content,
        "Language": getattr(t, "lang", "") or "",
        "Replies": getattr(t, "replyCount", 0) or 0,
        "Retweets": getattr(t, "retweetCount", 0) or 0,
        "Likes": getattr(t, "likeCount", 0) or 0,
        "Quotes": getattr(t, "quoteCount", 0) or 0,
        "Views": getattr(t, "viewCount", None) or "",
        "Hashtags": _safe_join(getattr(t, "hashtags", None)),
        "Cashtags": _safe_join(getattr(t, "cashtags", None)),
        "Links": _safe_join(getattr(t, "links", None), attr="url"),
        "Mentions": _safe_join(getattr(t, "mentionedUsers", None), attr="username"),
        "MediaPhotos": len(media.photos) if media and media.photos else 0,
        "MediaVideos": len(media.videos) if media and media.videos else 0,
        "MediaGIFs": len(media.animated) if media and media.animated else 0,
        "Source": getattr(t, "sourceLabel", "") or "",
        "InReplyToTweetId": getattr(t, "inReplyToTweetIdStr", "") or "",
        "InReplyToScreenName": getattr(t, "inReplyToScreenName", "") or "",
        "IsQuote": bool(getattr(t, "isQuoteStatus", False)),
        "ConversationId": getattr(t, "conversationIdStr", "") or "",
        "IsRetweet": bool(getattr(t, "isRetweet", False)),
    }


def save_rows(rows: list[dict], path: str) -> int:
    """写入 CSV（utf-8-sig + 全引号），先按 Link 去重。"""
    df = pd.DataFrame(rows, columns=COLUMNS)
    if not df.empty:
        df = df.drop_duplicates(subset=["Link"], keep="first")
        df = df.sort_values("Date")
    df.to_csv(path, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
    return len(df)


def month_ranges(start: date, end: date):
    """
    生成 (标签, since, until) 逐月覆盖 [start, end]。
    X 的 until: 为【排他】，故 until = 下月1号；最后一个月 until = end + 1 天。
    """
    if start > end:
        return
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        since = date(y, m, 1)
        if (y, m) == (end.year, end.month):
            until = end + timedelta(days=1)
        else:
            ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
            until = date(ny, nm, 1)
        yield f"{y:04d}_{m:02d}", since.isoformat(), until.isoformat()
        m += 1
        if m > 12:
            m, y = 1, y + 1


def month_done(label: str) -> bool:
    """仅当月份文件里确实有数据行才算完成（空结果会被重查）。"""
    path = os.path.join(paths.archive_dir(), f"{label}.csv")
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            return sum(1 for _ in csv.reader(f)) > 1
    except Exception:
        return False


class Scraper:
    """通用抓取引擎。report(stage, **data) 是进度回调（可选）。"""

    def __init__(self, cookies_text: str, global_proxy: str | None = None,
                 delay: float = 2.0, report=None, stop_event=None,
                 pool_wait_timeout: float | None = None):
        self.accounts = parse_accounts(cookies_text, global_proxy)
        self.global_proxy = global_proxy or detect_proxy()
        self.delay = max(0.0, delay)
        self.report = report
        self.stop_event = stop_event
        # 账号池无可用账号时的最长等待（探测用短值避免被 15 分钟冷却锁干等）
        self.pool_wait_timeout = pool_wait_timeout
        self.pool: AccountsPool | None = None
        self.api: API | None = None
        os.makedirs(paths.out_dir(), exist_ok=True)
        os.makedirs(paths.archive_dir(), exist_ok=True)

    def _r(self, **data):
        if self.report:
            self.report("scrape", **data)

    def _check_stop(self):
        """检查停止信号：用户点停止后立即抛出，实现「真停止」。"""
        if self.stop_event and self.stop_event.is_set():
            raise StopRequested()

    async def _interruptible_sleep(self, sec: float, reason: str = ""):
        """可中断等待：等待期间上报 waiting 状态，并每 0.1s 响应停止信号。"""
        if sec <= 0:
            return
        if reason:
            self._r(waiting=True, waiting_detail=reason)
        try:
            end = time.monotonic() + sec
            while time.monotonic() < end:
                self._check_stop()
                await asyncio.sleep(0.1)
        finally:
            if reason:
                self._r(waiting=False, waiting_detail="")

    async def _consume(self, agen, on_progress, idle_label: str):
        """消费异步生成器，始终返回 (rows, error)。
        - 每收到一行即检查停止信号（停止即时生效，error=StopRequested）
        - 连续 >8s 无新数据时置 waiting（界面显示"等待中"）
        - on_progress(n) 供调用方按需上报进度
        - error 为内部异常（StopRequested 或其它），无异常为 None；调用方负责处理
        """
        rows = []
        last_emit = time.monotonic()
        waiting_reported = False
        error = None

        async def feed():
            nonlocal rows, last_emit, waiting_reported
            async for tw in agen:
                self._check_stop()
                row = tweet_to_row(tw)
                if row is None:
                    continue
                rows.append(row)
                last_emit = time.monotonic()
                if waiting_reported:
                    waiting_reported = False
                    self._r(waiting=False, waiting_detail="")
                on_progress(len(rows))

        task = asyncio.create_task(feed())
        try:
            while True:
                if task.done():
                    try:
                        task.result()
                    except asyncio.CancelledError:
                        error = StopRequested()
                    except Exception as e:
                        error = e
                    break
                if self.stop_event and self.stop_event.is_set():
                    error = StopRequested()
                    break
                if time.monotonic() - last_emit > 8 and not waiting_reported:
                    waiting_reported = True
                    self._r(waiting=True, waiting_detail=idle_label)
                await asyncio.sleep(0.2)
        finally:
            if not task.done():
                task.cancel()
        return rows, error

    async def setup(self) -> None:
        """创建账号池并加入所有 cookie 账号（多账号自动轮换）。
        新版 twscrape 无 Account.login()：cookie 账号即用即验（has_session），
        会话是否有效由实际 API 调用时的 401/403 反映（见 _is_auth_error）。"""
        # 数据库固定到统一用户数据目录（隐私数据集中存放），避免依赖随机工作目录
        pool = AccountsPool(db_file=os.path.join(paths.user_dir(), "accounts.db"),
                            wait_timeout=self.pool_wait_timeout)
        # 清理上次残留账号（失败不致命，忽略即可）
        try:
            existing = await pool.get_all()
            if existing:
                await pool.delete_accounts([a.username for a in existing])
        except Exception as e:
            print(f"[scraper] 清理旧账号失败（忽略）：{e}")
        added = []
        for i, acc in enumerate(self.accounts, 1):
            proxy = acc["proxy"] or self.global_proxy
            uname = f"acct_{i}"
            try:
                # 防御：批量清理失败时，单独确保该账号不存在再添加，
                # 避免 twscrape 对已存在账号静默跳过导致旧 cookie 被复用
                old = await pool.get_account(uname)
                if old is not None:
                    await pool.delete_accounts([uname])
                await pool.add_account(uname, "_", "_", "_",
                                       cookies=acc["cookies"], proxy=proxy)
                # 注：twscrape 当前版本的 add_account 不返回账号对象（无 return，
                # 恒为 None），且账号已存在时也会静默跳过。统一从库中重新读取，
                # 避免 a 为 None 导致后续 a.login()/a.username 抛 AttributeError。
                a = await pool.get_account(uname)
                if a is None:
                    print(f"[scraper] 第 {i} 行账号读取失败（忽略）")
                    continue
                # 新版 twscrape 无 Account.login()：cookie 账号即用即验，
                # 会话有效性由实际 API 调用时的 401/403 反映（见 _is_auth_error）
                if not a.has_session:
                    print(f"[scraper] 第 {i} 行账号缺少 auth_token/ct0，忽略")
                    continue
                added.append(a)
            except Exception as e:
                # 捕获所有异常：单个账号格式/解析问题不影响其它账号
                print(f"[scraper] 第 {i} 行账号添加失败：{e}")
        if not added:
            raise RuntimeError("没有可用的 cookie（每行需同时含 auth_token 和 ct0）")
        self.pool = pool
        self.api = API(pool=pool, proxy=self.global_proxy)
        self._r(detail=f"已就绪 {len(added)} 个账号" + (f"，代理 {self.global_proxy}" if self.global_proxy else ""))

    async def scrape_user(self, username: str, start_date: date | None,
                          end_date: date | None, force_recent: bool = False):
        """用户模式：主推文 + 含回复 + search 按月历史。"""
        # 容错：用户误填 @ 前缀时自动剥离（前端提示无需 @，但后端兜底）
        username = (username or "").strip().lstrip("@")
        if not username:
            raise RuntimeError("目标用户名为空")
        try:
            user = await self.api.user_by_login(username)
        except Exception as e:
            if _is_auth_error(e):
                raise RuntimeError(COOKIE_EXPIRED_MSG) from e
            raise
        if user is None:
            raise RuntimeError(f"找不到用户 @{username}")
        statuses = getattr(user, "statusesCount", None)
        self._r(detail=f"目标 @{username}" + (f"（statuses_count={statuses}）" if statuses is not None else ""))

        created = getattr(user, "created", None)
        end = end_date or date.today()
        if start_date is not None:
            # 用户手动指定：尊重原值，但仍在界面上说明
            start = start_date
            self._r(detail=f"起始日期：{start}（手动指定）")
        elif created is not None:
            try:
                # 自动选取：从账号创建当月起归档（ACG 从业者的账号常始于入行时期）
                start = date(created.year, created.month, 1)
                if start < _AUTO_START_FLOOR:
                    start = _AUTO_START_FLOOR
                self._r(detail=f"自动选取起始日期：{start}（账号创建于 {created.date()}）")
            except Exception:
                start = date.today()
                self._r(detail="未能解析账号创建时间，起始日期回退为今天")
        else:
            start = date.today()
            self._r(detail="未能获取账号创建时间，起始日期回退为今天")

        # Pass A：主推文（用数据行数判断，避免空表头文件被误判为已完成而永久跳过）
        main_csv = recent_main_csv()
        main_rows = count_csv_rows(main_csv) if os.path.exists(main_csv) else 0
        if main_rows == 0 or force_recent:
            await self._fetch_paginated(lambda: self.api.user_tweets(user.id, limit=-1),
                                        main_csv, "user_tweets（主推文）")
        else:
            self._r(detail=f"主推文已存在（{main_rows} 条），跳过")
        await self._interruptible_sleep(self.delay, "请求间隔等待（防限流）")

        # Pass C：含回复
        replies_csv = recent_replies_csv()
        replies_rows = count_csv_rows(replies_csv) if os.path.exists(replies_csv) else 0
        if replies_rows == 0 or force_recent:
            await self._fetch_paginated(lambda: self.api.user_tweets_and_replies(user.id, limit=-1),
                                        replies_csv, "user_tweets_and_replies（含回复）")
        else:
            self._r(detail=f"含回复推文已存在（{replies_rows} 条），跳过")
        await self._interruptible_sleep(self.delay, "请求间隔等待（防限流）")

        # Pass B：search 按月补历史
        await self._scrape_months(f"from:{username}", start, end)
        return user

    async def scrape_keyword(self, keyword: str, start_date: date | None, end_date: date | None):
        """关键词/标签模式：search 按月切片。"""
        start = start_date or _AUTO_START_FLOOR
        end = end_date or date.today()
        self._r(detail=(f"关键词「{keyword}」{start} ~ {end}"
                        + ("（起始日期留空，自动选取 2010-01-01）" if start_date is None else "")))
        await self._scrape_months(keyword, start, end)
        return None

    async def _fetch_paginated(self, gen_factory, path: str, name: str) -> int:
        self._r(detail=f"抓取中：{name}", pass_name=name)
        print(f"[scraper] 开始 {name} ……")

        def on_progress(n):
            if n % 25 == 0:
                self._r(detail=f"{name}：已 {n} 条", count=n)

        rows, err = await self._consume(gen_factory(), on_progress,
                                        f"{name}：等待数据返回…")
        if isinstance(err, StopRequested):
            raise err   # 用户停止：立即向上传播，不当作普通失败
        if err is not None:
            if _is_auth_error(err):
                raise RuntimeError(COOKIE_EXPIRED_MSG) from err
            # 失败不落盘（与 _scrape_months 一致）：若把部分行写入，下次运行
            # count_csv_rows>0 会误判"已存在"而跳过，缺失部分将永久不抓
            print(f"[scraper] {name} 失败：{err}")
            self._r(detail=f"{name} 失败：{err}")
            return 0
        n = save_rows(rows, path)
        self._r(detail=f"{name} 完成：{n} 条", count=n)
        print(f"[scraper] {name} 完成：{n} 条 → {path}")
        return n

    async def _scrape_months(self, query: str, start: date, end: date) -> int:
        ranges = list(month_ranges(start, end))
        self._r(detail=f"按月归档 {len(ranges)} 个月", months_total=len(ranges),
                months_done=0, count=0, month_count=0)
        total, done = 0, 0
        for label, since, until in ranges:
            if month_done(label):
                done += 1
                self._r(detail=f"月份 {label} 已存在，跳过", months_done=done)
                continue
            self._r(detail=f"抓取月份 {label}（{since}~{until}）", current_month=label)
            q = f"{query} since:{since} until:{until}"
            ok = True

            def on_progress(n):
                if n % 50 == 0:
                    self._r(detail=f"月份 {label}：已 {n} 条",
                            count=total + n, month_count=n,
                            current_month=label, waiting=False, waiting_detail="")

            rows, err = await self._consume(
                self.api.search(q, limit=-1), on_progress,
                f"月份 {label}：等待数据返回…")
            if isinstance(err, StopRequested):
                raise err
            if err is not None:
                if _is_auth_error(err):
                    raise RuntimeError(COOKIE_EXPIRED_MSG) from err
                ok = False
                print(f"[scraper] 月份 {label} 失败：{err}")
                self._r(detail=f"月份 {label} 失败：{err}", current_month=None)
            if ok:
                # 仅成功时落盘；失败不覆盖旧文件，下次重跑会重查该月
                save_rows(rows, os.path.join(paths.archive_dir(), f"{label}.csv"))
                total += len(rows)
                done += 1
                self._r(detail=f"月份 {label}：{len(rows)} 条", months_done=done,
                        current_month=None, count=total, month_count=len(rows))
            await self._interruptible_sleep(self.delay, "请求间隔等待（防限流）")
        self._r(detail=f"按月归档完成：共 {total} 条", count=total)
        return total
