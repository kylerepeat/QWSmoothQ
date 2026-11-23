"""Mock 企微服务命令行入口：``mock-wecom``。"""

from __future__ import annotations

import argparse
import logging

import uvicorn


def mock_main() -> None:
    parser = argparse.ArgumentParser(
        prog="mock-wecom",
        description="本地 Mock 企微服务：可注入单接口/全局限流",
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    uvicorn.run("mock_wecom.app:app", host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    mock_main()
