# -*- coding: utf-8 -*-
"""
独立软件启动器：启动 Web 服务并自动打开浏览器。
源码运行：python launcher.py [端口]
打包后  ：直接双击 exe（入口即此逻辑）
"""
import os
import sys
import traceback

import paths
from app import serve


def main() -> int:
    port = 8001
    force = False
    for a in sys.argv[1:]:
        if a in ("--force", "-f"):
            force = True
        elif a.isdigit():
            port = int(a)
    try:
        serve(port, open_browser=True, force=force)
        return 0
    except Exception:
        # 日志写到统一用户数据目录，避免 cwd 不确定导致日志丢失
        log_path = os.path.join(paths.user_dir(), "软件错误.log")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
