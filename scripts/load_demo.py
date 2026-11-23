"""验收辅助：批量向中间件灌消息制造积压。

用法示例：
    python scripts/load_demo.py --n 200 --apis message/send,user/get --target http://127.0.0.1:9000/cgi-bin
"""

from __future__ import annotations

import argparse
import asyncio
import time

import httpx


async def main() -> None:
    parser = argparse.ArgumentParser(description="批量投递消息到 QWSmoothQ")
    parser.add_argument("--middleware", default="http://127.0.0.1:8000")
    parser.add_argument("--target", default="http://127.0.0.1:9000/cgi-bin")
    parser.add_argument("--n", type=int, default=100, help="总消息数")
    parser.add_argument("--apis", default="message/send,user/get", help="逗号分隔的接口标识")
    parser.add_argument("--max-retries", type=int, default=5)
    args = parser.parse_args()

    apis = [item.strip() for item in args.apis.split(",") if item.strip()]
    started = time.time()
    async with httpx.AsyncClient(base_url=args.middleware, timeout=30.0) as client:
        for i in range(args.n):
            api = apis[i % len(apis)]
            resp = await client.post(
                "/messages",
                json={
                    "api": api,
                    "url": f"{args.target}/{api}",
                    "method": "POST",
                    "body": {"index": i, "content": "hello"},
                    "max_retries": args.max_retries,
                },
            )
            resp.raise_for_status()
            if (i + 1) % 20 == 0:
                print(f"enqueued {i + 1}/{args.n}")
    print(f"done: {args.n} messages in {time.time() - started:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
