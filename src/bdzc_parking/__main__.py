"""命令行入口，支持通过 `python -m bdzc_parking` 启动程序。"""

from bdzc_parking.app import main


if __name__ == "__main__":
    # 将真实启动流程交给 app.main，便于脚本入口和模块入口复用。
    raise SystemExit(main())
