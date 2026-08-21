# -*- coding: utf-8 -*-
"""核心逻辑测试（不联网）：多画像预设、ACG 统计、采样优化、模板格式化。

运行方式：
    python tests/test_core.py          # 项目根目录下执行
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

import llm_analyzer  # noqa: E402
import processor  # noqa: E402
import scraper  # noqa: E402

ok = True


def check(name, cond):
    global ok
    print(("PASS" if cond else "FAIL"), name)
    if not cond:
        ok = False


def _make_df(n=300):
    """合成数据：混合 ACG 创作/宣发、转发与闲聊，补全真实 CSV 的全部列。"""
    default = {c: "" for c in scraper.COLUMNS}
    default.update({"Likes": 0, "Retweets": 0, "Replies": 0, "Quotes": 0,
                    "MediaPhotos": 0, "MediaVideos": 0, "MediaGIFs": 0,
                    "IsRetweet": False, "IsQuote": False})
    rows = []
    for i in range(n):
        r = dict(default)
        r.update({"Date": f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d} 00:00:00+00:00",
                  "Link": f"https://x.com/u/status/{i}", "Author": "u"})
        if i % 4 == 0:
            r.update({"Content": "新作イラスト公開！描きました #art",
                      "MediaPhotos": 1, "Likes": 50, "Retweets": 20, "Language": "ja"})
        elif i % 4 == 1:
            r.update({"Content": "アフレコ収録お疲れ様でした #声優",
                      "Likes": 30, "Retweets": 10, "Language": "ja"})
        elif i % 4 == 2:
            r.update({"Content": "RT @someone 转发别人的消息",
                      "Likes": 5, "Retweets": 1, "IsRetweet": True})
        else:
            r.update({"Content": "今天天气不错，随便聊聊", "Likes": 3})
        rows.append(r)
    return pd.DataFrame(rows)


def test_presets():
    keys = set(llm_analyzer.PRESETS)
    check("预设键齐全", {"general", "acg", "brand", "creator", "expert"} <= keys)
    check("resolve_preset 未知回退 acg",
          llm_analyzer.resolve_preset("nope") == llm_analyzer.DEFAULT_PRESET)
    check("resolve_preset brand", llm_analyzer.resolve_preset("brand") == "brand")
    for k, p in llm_analyzer.PRESETS.items():
        for f in ("label", "system", "task", "batch", "sections"):
            check(f"预设 {k}.{f} 非空", bool(p.get(f)))
        # 新增公共章节：人物历程（时间线）+ 数据来源标注
        check(f"预设 {k} 含人物历程章节", "人物历程" in p["sections"])
        check(f"预设 {k} 含数据来源章节", "数据来源" in p["sections"])


def test_batch_link():
    """批次格式化必须附带来源链接与日期，供 LLM 画像引用。"""
    s = llm_analyzer._fmt_batch(_make_df(3))
    check("批次含来源链接", "来源: https://x.com/u/status/" in s)
    check("批次含日期", "[2024-" in s)


def test_templates():
    notes = "### 批次 1\n测试"
    for k, p in llm_analyzer.PRESETS.items():
        b = llm_analyzer.BATCH_TEMPLATE.format(
            focus=p["batch"], body="[2026-01-01] (赞1/转1) 测试")
        f = llm_analyzer.FINAL_TEMPLATE.format(
            task=p["task"], sections=p["sections"],
            stats="总推文数: 10", notes=notes)
        check(f"模板 {k} 可格式化",
              "{body}" not in b and "{notes}" not in f and "{sections}" not in f)


def test_acg_stats():
    df = _make_df()
    stats = processor.compute_stats(df)
    check("stats.total", stats["total"] == len(df))
    acg = stats.get("acg") or {}
    check("acg 画师线索>0", acg.get("画师/插画", 0) >= len(df) // 4)
    check("acg 声优线索>0", acg.get("声优/配音", 0) >= len(df) // 4)
    check("media_photos", stats.get("media_photos", 0) >= len(df) // 4)


def test_sampling():
    df = _make_df()
    batches = processor.sample_tweets(df)
    sampled = pd.concat(batches, ignore_index=True)
    text = " ".join(sampled["Content"].fillna("").astype(str))
    check("采样含创作类(画师)", "イラスト" in text or "描き" in text)
    check("采样含声优", "アフレコ" in text or "収録" in text)
    check("采样数量合理", 100 < len(sampled) <= 1300)
    orig = (~sampled["IsRetweet"].astype(bool)).mean()
    check("采样原创占比高", orig > 0.5)


def test_fmt_stats_preset():
    stats = processor.compute_stats(_make_df())
    check("acg 摘要含职业线索", "ACG 职业线索" in llm_analyzer._fmt_stats(stats, "acg"))
    check("brand 摘要不含职业线索", "ACG 职业线索" not in llm_analyzer._fmt_stats(stats, "brand"))


def test_digest():
    """简约推文整理：仅含时间、链接、内容。"""
    import paths
    df = _make_df(20)
    paths.set_task_key("_unittest_digest")
    try:
        p = processor.write_digest(df)
        check("digest 文件生成", os.path.exists(p))
        with open(p, encoding="utf-8") as f:
            txt = f.read()
        check("digest 含链接", "https://x.com/u/status/" in txt)
        check("digest 含内容", "新作" in txt or "アフレコ" in txt or "今天天气" in txt)
        check("digest 每条含时间与链接", "[2024-" in txt)
    finally:
        import shutil
        d = os.path.join(paths.output_root(), "_unittest_digest")
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
        paths.set_task_key("")


def test_collect_imported_csv():
    """导入的任意 CSV（out_dir 根目录）应被清洗流程纳入。"""
    import paths
    paths.set_task_key("_unittest_import")
    try:
        os.makedirs(paths.out_dir(), exist_ok=True)
        p = os.path.join(paths.out_dir(), "custom_import.csv")
        _make_df(5).to_csv(p, index=False, encoding="utf-8-sig")
        frames = processor.collect_frames(include_archive=False)
        total = sum(len(f) for f in frames)
        check("导入的任意 CSV 被纳入清洗", total >= 5)
    finally:
        import shutil
        d = os.path.join(paths.output_root(), "_unittest_import")
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
        paths.set_task_key("")


def test_sanitize_profile():
    """配置文件名校验：防路径逃逸。"""
    import app
    check("profile 空→''", app._sanitize_profile("") == "")
    check("profile default→''", app._sanitize_profile("default") == "")
    check("profile 合法中文名", app._sanitize_profile("工作A_1") == "工作A_1")
    check("profile 路径逃逸被拒", app._sanitize_profile("../evil") == "")
    check("profile 过长被拒", app._sanitize_profile("a" * 40) == "")


def test_request_limits():
    """请求长度边界不能把负数变成 read(-1)，也不能绕过上限。"""
    import app
    check("负 Content-Length 被拒", app.Handler._read_content_length(-1, 100) is None)
    check("超限 Content-Length 被拒", app.Handler._read_content_length(101, 100) is None)
    check("合法 Content-Length 保留", app.Handler._read_content_length(100, 100) == 100)


def test_import_filename_safety():
    """导入文件名在 Windows 规范化后也不能覆盖 clean_tweets.csv。"""
    import app
    check("导入文件名允许普通 CSV", app._safe_import_filename("tweets.csv") == "tweets.csv")
    check("导入文件名拒绝清洗结果", app._safe_import_filename("clean_tweets.csv") is None)
    check("导入文件名拒绝尾随句点绕过", app._safe_import_filename("clean_tweets.csv.") is None)
    check("导入文件名拒绝路径", app._safe_import_filename("..\\clean_tweets.csv") is None)
    check("导入文件名拒绝设备名", app._safe_import_filename("CON.csv") is None)


def test_cookies_mask_and_resolve():
    """cookies 不再明文回传（掩码）；use_saved_cookies 从配置读取。"""
    import app
    m = app.mask_config({"cookies": "auth_token=a; ct0=b", "llm_api_key": "sk-12345678"})
    check("cookies 掩码", m.get("cookies") == app.SAVED_COOKIES_MASK)
    check("key 掩码", m.get("llm_api_key") == "sk-1***")
    check("表单 cookies 优先",
          app._resolve_cookies({"cookies": "auth_token=x; ct0=y"}) == "auth_token=x; ct0=y")
    check("无 cookies 且未标记→空", app._resolve_cookies({}) == "")
    check("掩码值不当作真实 cookies",
          app._resolve_cookies({"cookies": app.SAVED_COOKIES_MASK}) == "")
    # use_saved_cookies 从命名配置读取
    app.save_config({"cookies": "auth_token=saved; ct0=s", "target": "t"}, "测试cookies")
    check("use_saved_cookies 读取命名配置",
          app._resolve_cookies({"use_saved_cookies": True, "profile": "测试cookies"})
          == "auth_token=saved; ct0=s")
    app.clear_config("测试cookies")


def test_minimal_csv_columns():
    """导入的最小 CSV（仅 Date/Link/Content）应补齐列并正确统计：
    不崩溃、is_reply 不计空串。"""
    import paths
    paths.set_task_key("_unittest_minimal")
    try:
        os.makedirs(paths.out_dir(), exist_ok=True)
        p = os.path.join(paths.out_dir(), "mini.csv")
        with open(p, "w", encoding="utf-8-sig") as f:
            f.write("Date,Link,Content\n"
                    "2025-01-01 00:00:00+00:00,https://x.com/a/1,测试一\n"
                    "2025-01-02 00:00:00+00:00,https://x.com/a/2,测试二\n")
        df = processor.process(None, None)
        stats = processor.compute_stats(df)
        check("最小 CSV 统计 total", stats.get("total") == 2)
        check("最小 CSV is_reply=0（空串不计回复）", stats.get("is_reply") == 0)
        check("最小 CSV is_retweet=0", stats.get("is_retweet") == 0)
    finally:
        import shutil
        d = os.path.join(paths.output_root(), "_unittest_minimal")
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
        paths.set_task_key("")


def test_with_media_string_cols():
    """导入 CSV 的数值列为字符串/混合值时 with_media 不应崩溃（to_numeric 兜底）。"""
    import paths
    paths.set_task_key("_unittest_media")
    try:
        os.makedirs(paths.out_dir(), exist_ok=True)
        p = os.path.join(paths.out_dir(), "media.csv")
        with open(p, "w", encoding="utf-8-sig") as f:
            f.write("Date,Link,Content,MediaPhotos,MediaVideos\n"
                    "2025-01-01 00:00:00+00:00,https://x.com/a/1,测试,2,abc\n"
                    "2025-01-02 00:00:00+00:00,https://x.com/a/2,测试,abc,1\n")
        df = processor.process(None, None)
        stats = processor.compute_stats(df)
        check("字符串数值列 with_media 不崩溃", stats.get("with_media") == 2)
        check("字符串数值列 media_photos=1", stats.get("media_photos") == 1)
    finally:
        import shutil
        d = os.path.join(paths.output_root(), "_unittest_media")
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
        paths.set_task_key("")


def test_is_reply_zero_not_counted():
    """部分 CSV 导出用 "0" 表示非回复，不应计入 is_reply。"""
    import paths
    paths.set_task_key("_unittest_reply0")
    try:
        os.makedirs(paths.out_dir(), exist_ok=True)
        p = os.path.join(paths.out_dir(), "reply0.csv")
        with open(p, "w", encoding="utf-8-sig") as f:
            f.write("Date,Link,Content,InReplyToTweetId\n"
                    "2025-01-01 00:00:00+00:00,https://x.com/a/1,原创一,0\n"
                    "2025-01-02 00:00:00+00:00,https://x.com/a/2,回复二,123456\n"
                    "2025-01-03 00:00:00+00:00,https://x.com/a/3,原创三,\n")
        df = processor.process(None, None)
        stats = processor.compute_stats(df)
        check("is_reply 不计 0/空串，仅计真实回复", stats.get("is_reply") == 1)
    finally:
        import shutil
        d = os.path.join(paths.output_root(), "_unittest_reply0")
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
        paths.set_task_key("")


def test_archive_output():
    """输出内容自动保存：任务完成后归档到 history/<时间戳>/ 并保留文件。"""
    import paths
    import app
    paths.set_task_key("_unittest_archive")
    try:
        os.makedirs(paths.out_dir(), exist_ok=True)
        for name in ("clean_tweets.csv", "tweets_digest.txt", "result.json"):
            with open(os.path.join(paths.out_dir(), name), "w",
                      encoding="utf-8") as f:
                f.write("x")
        dest = app._archive_output()
        check("归档目录生成", bool(dest) and os.path.isdir(dest))
        check("归档含文件", dest and all(
            os.path.exists(os.path.join(dest, n))
            for n in ("clean_tweets.csv", "tweets_digest.txt", "result.json")))
    finally:
        import shutil
        d = os.path.join(paths.output_root(), "_unittest_archive")
        if os.path.isdir(d):
            shutil.rmtree(d, ignore_errors=True)
        paths.set_task_key("")


if __name__ == "__main__":
    test_presets()
    test_templates()
    test_batch_link()
    test_acg_stats()
    test_sampling()
    test_fmt_stats_preset()
    test_digest()
    test_collect_imported_csv()
    test_sanitize_profile()
    test_request_limits()
    test_import_filename_safety()
    test_cookies_mask_and_resolve()
    test_minimal_csv_columns()
    test_is_reply_zero_not_counted()
    test_with_media_string_cols()
    test_archive_output()
    print("\nALL_OK" if ok else "HAS_FAILURES")
    sys.exit(0 if ok else 1)
