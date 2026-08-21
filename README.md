# 推特用户画像生成器

一个把「抓取推文 → 清洗统计 → LLM 生成画像」串起来的本地小工具。填好目标账号、几个 cookie 和 LLM 的 API Key，在浏览器界面里点一下开始，就能拿到按月归档的推文数据、统计摘要，以及一份 Markdown 格式的用户画像。

> 注意：本项目仅供学习研究。自动抓取 X（Twitter）可能违反其服务条款，请务必使用小号并自行承担风险。

## 功能

- **两种抓取模式**：用户模式归档某账号的全部推文（近期主推文 + 含回复 + 按月补历史）；关键词模式按关键词/标签逐月搜索。
- **多账号与代理**：cookie 每行一个小号，自动轮换缓解单账号限流；行尾加 `|proxy=` 可给该账号单独指定出口代理。
- **断点续跑**：数据按月落盘，中途中断后重启会自动跳过已完成的部分。
- **清洗统计**：合并去重、日期过滤、文本清洗，输出总量、按年分布、互动、热门标签、高频提及等统计摘要。ACG 预设还会额外统计职业线索（画师/剧本/声优/动画/音乐/宣发/活动/联动）和媒体特征。
- **LLM 画像**：对推文分层采样后分批交给大模型提炼，再汇总成画像。内置 ACG 从业者（默认）、通用个人、品牌官方号、内容创作者、专家意见领袖五套预设，均带「人物历程」和「数据来源与可信度」章节，结论会注明推文链接与日期。
- **四种操作拆分**：完整流程、仅抓取、导入已有 CSV、仅 LLM 总结可以分开用。抓取、导入和清洗统计完全不依赖 LLM，不填 API Key 也能用。
- **隐私与安全**：cookie 和 API Key 在界面上只显示掩码；配置、账号库和抓取数据统一放在用户数据目录（默认 `~/TwitterProfileGenerator/`），程序目录不落任何数据文件。
- **其他**：防休眠/自动关机、限流状态检测、多套命名配置、自定义输出目录、历史输出自动归档等，界面里都有说明。

## 安装

```bash
pip install -r requirements.txt
```

## 运行

```bash
python app.py            # 默认端口 8001
python app.py 8080       # 指定端口
```

浏览器打开 <http://127.0.0.1:8001>

## 使用

在界面里依次填写：

- **模式与目标**：用户（填账号名）或关键词。
- **操作**：完整流程（默认）、仅抓取、导入数据、仅 LLM 总结。
- **画像预设**：ACG 从业者（默认）、通用个人、品牌官方号、内容创作者、专家意见领袖。
- **起止日期**：留空会自动选取（用户模式取账号创建月起，不早于 2010-01；关键词模式默认 2010-01-01），也可以手动指定或用快捷按钮（近1年/近3年/近5年/全部/自定义）。
- **Cookies**：每行一个小号，三种填法任选——
  - 直接填两个值：`token值 ct0值`（自动补前缀，最简单）
  - 完整格式：`auth_token=xxx; ct0=yyy`
  - 整段粘贴 Cookie 头（自动提取）
  - 行尾可加 `|proxy=代理地址`，`#` 开头是注释。输入时界面会实时显示识别到几个小号，也可以点「检测限流状态」看看有没有被限速。
  - 已保存的 cookie 和 API Key 不会回显明文，直接用已保存配置启动即可，不用重复粘贴。
- **LLM**：API Base URL（OpenAI 兼容）+ API Key + 模型名。只有生成画像时才需要。
- **电源**：防休眠默认开启；「完成后自动关机」单独勾选。
- **配置文件**：默认 `user_config.json`，也可以新建/切换命名配置。
- **输出目录**：默认在数据目录 `output/` 下，可点「选择…」改到任意位置；「打开输出目录 / 打开数据目录」可直接在文件管理器里定位。

填好后点「开始生成」（输入框内按回车也行）。导入操作是选好 CSV 后点「导入并清洗」。

跑完后可以查看统计卡片和画像，并下载清洗 CSV、简约整理（每条只有时间/链接/内容）和画像 MD。右上角的「工作原理」可以阅读完整原理说明。

## 怎么拿 Cookie

务必用小号，别用主号（有风控风险）。

1. 打开 <https://x.com> 并登录小号
2. F12 → Application → Cookies → 选择 `https://x.com`
3. 复制 `auth_token` 和 `ct0` 的 Value
4. 填进界面的 Cookies 输入框（每行一个小号）

## LLM 配置示例

| 服务 | Base URL | 模型示例 |
| --- | --- | --- |
| DeepSeek | <https://api.deepseek.com/v1> | deepseek-chat |
| OpenAI | <https://api.openai.com/v1> | gpt-4o-mini |
| 通义千问 | <https://dashscope.aliyuncs.com/compatible-mode/v1> | qwen-plus |
| Ollama 本地 | <http://localhost:11434/v1> | llama3.1 |

## 目录结构

```text
twitter_profile_generator/          # 程序目录（只放代码与只读资源）
├── app.py              # Web 界面 + 流程编排
├── scraper.py          # 通用抓取引擎（含自动起始日期）
├── processor.py        # 清洗与统计（含 ACG 职业线索统计、画像采样、简约整理）
├── llm_analyzer.py     # LLM 画像生成（多预设 PRESETS、人物历程与来源标注）
├── paths.py            # 路径管理（程序目录 / 用户数据目录 / 输出目录）
├── static/index.html   # 前端界面（内联 SVG 图标）
├── static/工作原理.md  # 工作原理文档（界面「工作原理」按钮可读）
├── tests/test_core.py  # 核心逻辑测试（不联网）
└── requirements.txt

~/TwitterProfileGenerator/          # 统一用户数据目录（隐私数据 + 获得的数据）
├── user_config.json    # 主配置（Cookie / API Key 等，掩码回传）
├── configs/            # 命名配置文件（configs/<名称>.json，可选）
├── accounts.db         # twscrape 账号库
├── progress.json       # 任务进度
├── app_settings.json   # 界面偏好（如自定义输出目录）
├── 运行锁.lock         # 单实例锁
└── output/             # 抓取数据（可按界面「输出目录」改到任意位置）
    └── <模式_目标>/     # 按目标隔离：分片/CSV/画像/简约整理/history 归档
```

> 输出按目标自动隔离：每次抓取写入 `output/<模式_目标>/`（如 `output/user_elonmusk/`、
> `output/keyword_机器学习/`），**切换目标不会与上次数据串扰**；同目标重复运行仍走断点续跑。
>
> 隐私数据（Cookie / API Key / 账号库）与抓取数据统一存放在用户数据目录，
> **程序目录不再产生任何数据文件**；可用环境变量 `TPG_DATA_DIR` 指定其他位置。
> 旧版散落在程序目录的数据（`user_config.json` / `configs/` / `output/` 等）首次启动时自动迁移。

## 测试

```bash
python tests/test_core.py    # 不联网，验证多预设、ACG 统计、采样等核心逻辑
```

## 打包成 exe

不装 Python 的话，直接运行 `dist\推特用户画像生成器.exe`（会自动打开浏览器）。

修改代码后重新打包，运行 `build_exe.bat`，产物在 `dist\`。打包要点：

- 入口是 `launcher.py`（启动服务 + 自动开浏览器 + 异常写入「软件错误.log」，日志在用户数据目录）
- `paths.py` 区分包内只读资源（static）和用户数据目录（`~/TwitterProfileGenerator/`）
- PyInstaller 需要 `--collect-all twscrape --collect-all fake_useragent`（已写进 build_exe.bat）

## 注意事项

- 国内访问 X 需要代理。程序会自动探测本机 Clash/v2rayN 的端口，也可以手动填全局代理。
- 单账号限流比较严（大约 300 条/15 分钟），多账号 + 多 IP 是提速的关键。
- search 抓不到历史回复，近期的回复由单独的通道补充。
- 程序带单实例锁（`运行锁.lock`），重复启动会被拒绝；异常退出留下的残留锁下次启动会自动接管。旧实例卡死时可用 `python launcher.py --force`（或 `app.py --force`）强制结束并接管。
- 同一时刻只运行一个抓取进程，避免多个进程抢同一批账号。
- 凭据只在后端本机读取，界面上只显示掩码。

## 许可证

MIT，见 [LICENSE](LICENSE)。
