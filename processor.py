# -*- coding: utf-8 -*-
"""
数据清洗与统计分析
==================
把抓取的多个 CSV 分片合并成一份干净的数据集：
  - 按 Link（推文 URL）去重
  - 按 Date 升序排序
  - 可选按日期范围过滤
  - 文本清洗（去 URL/多余空白，保留原文）
输出：output/clean_tweets.csv + 统计字典 stats
"""
import csv
import glob
import os
import re
from collections import Counter
from datetime import datetime, timedelta

import pandas as pd

import paths
from scraper import recent_main_csv, recent_replies_csv, keyword_csv, COLUMNS


# 数值列 / 布尔列默认值：导入的最小 CSV（如仅 Date/Link/Content）缺列时补齐，
# 避免后续统计比较/求和时出现 KeyError 或 str/int 比较崩溃。
_NUM_COLS = ("Replies", "Retweets", "Likes", "Quotes", "Views",
             "MediaPhotos", "MediaVideos", "MediaGIFs")
_BOOL_COLS = ("IsRetweet", "IsQuote")


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    """补齐缺失列：数值列补 0、布尔列补 False、其余文本列补空串。"""
    for c in COLUMNS:
        if c in df.columns:
            continue
        if c in _NUM_COLS:
            df[c] = 0
        elif c in _BOOL_COLS:
            df[c] = False
        else:
            df[c] = ""
    return df


def clean_csv() -> str:
    """清洗结果 CSV 路径（按当前任务隔离目录）。"""
    return os.path.join(paths.out_dir(), "clean_tweets.csv")


def digest_file() -> str:
    """简约推文整理文件路径（仅含时间、链接、内容）。"""
    return os.path.join(paths.out_dir(), "tweets_digest.txt")


_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")

# ACG 业界从业者特征关键词分组（用于统计与采样加权，命中即计入对应职业线索）。
# 供 LLM 画像判断剧本家/画师/声优/官方账号等身份时作为硬信号。
_ACG_GROUPS = [
    ("画师/插画", ("イラスト", "插画", "插图", "线稿", "上色", "塗り", "落書き",
                   "絵を", "絵が", "絵の", "絵描き", "絵師", "pixiv", "fanart",
                   "sketch", "作画", "原画")),
    ("剧本/脚本", ("シナリオ", "脚本", "台本", "執筆", "执笔", "剧情", "剧本",
                   "文案", "小説", "小说", "テキスト", "scenario", "ストーリー")),
    ("声优/配音", ("声優", "声优", "アフレコ", "収録", "配音", "ボイス",
                   "ナレーション", "キャスト", "cv")),
    ("动画/制作", ("アニメ", "动画", "動画", "anime", "作監", "演出", "監督",
                   "监督", "制作进行")),
    ("音乐/作曲", ("作曲", "作詞", "作词", "楽曲", "音楽", "音乐", "编曲",
                   "ost", "bgm")),
    ("官方/宣发", ("発売", "リリース", "放送", "配信", "上线", "开播", "发布",
                   "宣传", "新作", "予約", "预约", "公式", "初回")),
    ("活动/展会", ("イベント", "コミケ", "comiket", "展示", "展会", "サイン会",
                   "签售", "生放送", "ライブ", "舞台挨拶", "见面会")),
    ("联动/合作", ("コラボ", "联动", "合作", "対談", "访谈", "インタビュー")),
]
_ACG_PATTERNS = [(name, re.compile("|".join(re.escape(k) for k in kws), re.IGNORECASE))
                 for name, kws in _ACG_GROUPS]
# 任一 ACG 关键词（采样时用于识别创作/宣发类推文）
_ACG_ANY_RE = re.compile(
    "|".join(re.escape(k) for _, kws in _ACG_GROUPS for k in kws), re.IGNORECASE)


def clean_text(s) -> str:
    """基础文本清洗：去 URL、归一化空白。保留原文在 Content 列。"""
    if not isinstance(s, str):
        return ""
    t = _URL_RE.sub(" ", s)
    t = _WS_RE.sub(" ", t).strip()
    return t


def collect_frames(include_archive: bool = True) -> list[pd.DataFrame]:
    """读取所有抓取分片（近端主推文 + 含回复 + 按月归档 + 关键词 + 导入的任意 CSV）。
    损坏/无法解析的分片会被跳过，避免单个坏文件导致整体失败。"""
    frames = []
    for p in (recent_main_csv(), recent_replies_csv(), keyword_csv()):
        if os.path.exists(p) and os.path.getsize(p) > 0:
            try:
                frames.append(_normalize_frame(pd.read_csv(p)))
            except Exception:
                print(f"[processor] 跳过无法读取的分片：{p}")
    # 导入的任意 CSV：out_dir 根目录下的其它 .csv（排除自身清洗结果，避免重复计数）
    base_names = {os.path.basename(p) for p in
                  (recent_main_csv(), recent_replies_csv(), keyword_csv())}
    for f in sorted(glob.glob(os.path.join(paths.out_dir(), "*.csv"))):
        base = os.path.basename(f).lower()
        if base == "clean_tweets.csv" or os.path.basename(f) in base_names:
            continue
        if os.path.getsize(f) > 0:
            try:
                frames.append(_normalize_frame(pd.read_csv(f)))
            except Exception:
                print(f"[processor] 跳过无法读取的分片：{f}")
    if include_archive:
        for f in sorted(glob.glob(os.path.join(paths.archive_dir(), "*.csv"))):
            if os.path.getsize(f) > 0:
                try:
                    frames.append(_normalize_frame(pd.read_csv(f)))
                except Exception:
                    print(f"[processor] 跳过无法读取的分片：{f}")
    return frames


def process(start_date: str | None = None, end_date: str | None = None,
            report=None) -> pd.DataFrame:
    """
    合并 → 去重 → 排序 → 范围过滤 → 文本清洗。
    report(stage, **data) 为进度回调。
    """
    if report:
        report("process", detail="读取分片文件…")
    frames = collect_frames()
    if not frames:
        raise RuntimeError("没有找到推文数据：请先抓取，或使用「导入数据」导入已有 CSV")

    df = pd.concat(frames, ignore_index=True)
    before = len(df)
    df = df.drop_duplicates(subset=["Link"], keep="first")
    dupes = before - len(df)
    if report:
        report("process", detail=f"合并 {len(frames)} 个分片，共 {before} 条，去重 {dupes} 条",
               input_rows=before, dupes=dupes)

    # utc=True 统一时区：naive 视为 UTC、aware 转 UTC，避免混合时区比较崩溃
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df = df.sort_values("Date", na_position="last").reset_index(drop=True)

    if start_date:
        try:
            cutoff = pd.Timestamp(start_date, tz="UTC")
            df = df[df["Date"] >= cutoff]
        except Exception as e:
            print(f"[processor] 起始日期过滤失败（忽略）：{e}")
    if end_date:
        try:
            # 截止值取次日零点（排他），保留截止当天 23:59:59
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() + timedelta(days=1)
            cutoff = pd.Timestamp(end_dt, tz="UTC")
            df = df[df["Date"] < cutoff]
        except Exception as e:
            print(f"[processor] 截止日期过滤失败（忽略）：{e}")

    df = df.reset_index(drop=True)
    df["CleanedContent"] = df["Content"].map(clean_text)

    if report:
        report("process", detail="保存清洗结果…")
    df.to_csv(clean_csv(), index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
    # 简约推文整理：仅含时间、链接、内容（供快速浏览/归档）
    write_digest(df)
    if report:
        report("process", detail=f"清洗完成：{len(df)} 条（去重 {dupes}）", rows=int(len(df)))
    return df


def write_digest(df: pd.DataFrame) -> str:
    """生成简约推文整理文件（tweets_digest.txt）：每条推文仅含时间、链接、内容。
    返回文件路径；无数据时不生成。"""
    if df is None or len(df) == 0:
        return ""
    d = df.copy()
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce", utc=True)
    d = d.sort_values("Date", na_position="last").reset_index(drop=True)
    lines = []
    for _, r in d.iterrows():
        ts = r.get("Date")
        ts = str(ts) if pd.notna(ts) else ""
        link = str(r.get("Link", "") or "")
        content = str(r.get("Content", "") or "").replace("\r", "").replace("\n", " ")
        lines.append(f"[{ts}] {link}\n{content}")
    text = "\n\n".join(lines)
    path = digest_file()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def compute_stats(df: pd.DataFrame) -> dict:
    """从清洗后的 DataFrame 计算统计摘要（供 LLM 分析与画像使用）。"""
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df = df.dropna(subset=["Date"])          # 剔除解析失败的日期，避免 NaN 年份崩溃
    df["Year"] = df["Date"].dt.year.astype(int)
    total = len(df)
    if total == 0:
        return {"total": 0}

    def _sum(col):
        return int(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())

    def _avg(col):
        v = pd.to_numeric(df[col], errors="coerce").dropna()
        return round(float(v.mean()), 1) if len(v) else 0

    # 主题/标签
    hashtag_counter = Counter()
    mention_counter = Counter()
    for h in df["Hashtags"].dropna().astype(str):
        for tag in h.split(","):
            tag = tag.strip()
            if tag:
                hashtag_counter[tag] += 1
    for m in df["Mentions"].dropna().astype(str):
        for mm in m.split(","):
            mm = mm.strip()
            if mm:
                mention_counter[mm] += 1

    langs = Counter(df["Language"].dropna().astype(str))
    sources = Counter(df["Source"].dropna().astype(str))

    # ACG 业界特征：命中各职业关键词的推文数（供 ACG 从业者画像判断身份/领域）
    content_all = df["Content"].fillna("").astype(str)
    acg_counts = {}
    for name, pat in _ACG_PATTERNS:
        acg_counts[name] = int(content_all.str.contains(pat).sum())

    # pandas 3.x 中 astype(str) 对 NaN 保持 NaN（不再转成 "nan"），需先 fillna，
    # 否则 text_len 含 NaN 导致 mean()/max() 为 NaN、int(NaN) 抛 ValueError
    content = df["Content"].fillna("").astype(str)
    text_len = content.str.len()

    return {
        "total": total,
        "start": str(df["Date"].min()),
        "end": str(df["Date"].max()),
        "years": {int(k): int(v) for k, v in df.groupby("Year").size().items()},
        "avg_len": round(float(text_len.mean()), 1),
        "max_len": int(text_len.max()),
        "sum_likes": _sum("Likes"),
        "sum_retweets": _sum("Retweets"),
        "sum_replies": _sum("Replies"),
        "sum_quotes": _sum("Quotes"),
        "avg_likes": _avg("Likes"),
        "avg_retweets": _avg("Retweets"),
        # 非回复的 InReplyToTweetId 可能是空串/NaN 或 "0"（部分 CSV 导出的写法），
        # pandas 读取时会被解析成 0.0/NaN，统一 to_numeric 判断 >0 才计为回复，
        # 避免导入数据虚高回复数；仅用于计数，不依赖 ID 精度
        "is_reply": int((pd.to_numeric(df["InReplyToTweetId"], errors="coerce")
                         .fillna(0) > 0).sum()),
        # is_retweet：优先用 IsRetweet 列（pandas 2.x 读回是字符串，需解析）；
        # 旧数据无该列时按内容 "RT @" 前缀判断（大小写不敏感）
        "is_retweet": int(df["IsRetweet"].map(lambda v: str(v).strip().lower() == "true")
                       .fillna(False).astype(bool).sum())
        if "IsRetweet" in df.columns
        else int(df["Content"].astype(str).str.contains(r"^RT\s*@", case=False, regex=True).sum()),
        # 统一 to_numeric：导入的 CSV 数值列可能是字符串（含空串），直接比较会崩
        "with_media": int(((pd.to_numeric(df["MediaPhotos"], errors="coerce").fillna(0) > 0)
                           | (pd.to_numeric(df["MediaVideos"], errors="coerce").fillna(0) > 0)
                           | (pd.to_numeric(df["MediaGIFs"], errors="coerce").fillna(0) > 0)).sum()),
        "media_photos": int((pd.to_numeric(df["MediaPhotos"], errors="coerce").fillna(0) > 0).sum()),
        "media_videos": int((pd.to_numeric(df["MediaVideos"], errors="coerce").fillna(0) > 0).sum()),
        "acg": acg_counts,
        "top_hashtags": hashtag_counter.most_common(15),
        "top_mentions": mention_counter.most_common(15),
        "langs": langs.most_common(10),
        "sources": sources.most_common(8),
    }


def sample_tweets(df: pd.DataFrame, max_batch: int = 120, cap: int = 1200) -> list[pd.DataFrame]:
    """
    采样策略（LLM 上下文有限），面向 ACG 从业者画像优化：
      - 高互动 Top 100（原创内容加权，转发降权，更侧重本人发言）
      - 创作/媒体类样本优先（含图片视频、命中 ACG 关键词的作品/宣发动态），
        保证画师作品、声优收录、官方宣发等关键信号进入样本
      - 其余按年分层抽样，控制总量
    返回分批后的 DataFrame 列表（每批 max_batch 条）。
    """
    if df.empty:
        return []
    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)
    df = df.dropna(subset=["Date"])
    if len(df) == 0:
        return []

    # 转发标记：优先用 IsRetweet 列，旧数据按内容 "RT @" 前缀兜底
    if "IsRetweet" in df.columns:
        is_ret = (df["IsRetweet"].map(lambda v: str(v).strip().lower() == "true")
                  .fillna(False).astype(bool))
    else:
        is_ret = df["Content"].fillna("").astype(str).str.contains(
            r"^RT\s*@", case=False, regex=True)

    likes = pd.to_numeric(df["Likes"], errors="coerce").fillna(0)
    rets = pd.to_numeric(df["Retweets"], errors="coerce").fillna(0)
    # 原创内容加权：使高互动样本更侧重本人发言而非转发
    eng = likes + rets * 2 + (~is_ret).astype(int) * 3
    top = (df.assign(_eng=eng.to_numpy())
             .nlargest(min(100, len(df)), "_eng")
             .drop(columns="_eng"))

    rest = df.drop(index=top.index)
    if len(rest) > 0:
        rest = rest.copy()
        content = rest["Content"].fillna("").astype(str)
        has_media = ((pd.to_numeric(rest["MediaPhotos"], errors="coerce").fillna(0) > 0)
                     | (pd.to_numeric(rest["MediaVideos"], errors="coerce").fillna(0) > 0))
        hit_acg = content.str.contains(_ACG_ANY_RE)
        creative = rest[has_media | hit_acg]
        plain = rest[~(has_media | hit_acg)]
        # 创作/媒体类优先纳入（ACG 画像需要作品与宣发动态），控制预算避免挤占全量
        creative_budget = max(200, cap // 4)
        if len(creative) > creative_budget:
            creative = creative.sample(creative_budget, random_state=7)
        rest_left = pd.concat([creative, plain], ignore_index=True)
        rest_left["_year"] = rest_left["Date"].dt.year
        per = max(1, (cap - len(top) - len(creative)) // max(1, rest_left["_year"].nunique()))
        sampled = (rest_left.groupby("_year", group_keys=False)
                           .apply(lambda g: g.sample(min(per, len(g)), random_state=42)))
        sampled = sampled.reset_index(drop=True)   # 分组键已作为索引被丢弃
        sample = pd.concat([top, sampled], ignore_index=True).drop_duplicates(subset=["Link"])
    else:
        sample = top

    sample = sample.sort_values("Date").reset_index(drop=True)
    batches = []
    for i in range(0, len(sample), max_batch):
        batches.append(sample.iloc[i:i + max_batch])
    return batches
