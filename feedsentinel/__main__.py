"""python -m feedsentinel serve [--port 8000] [--start 10:30] [--speed 5]"""
from __future__ import annotations

import argparse
import logging


def main() -> None:
    ap = argparse.ArgumentParser(prog="feedsentinel")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sv = sub.add_parser("serve", help="run the dashboard server on the replay")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8000)
    sv.add_argument("--start", default="10:30", help="market time to start the replay (ET)")
    sv.add_argument("--speed", type=int, default=5, choices=[1, 2, 5, 10, 20])
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if args.cmd == "serve":
        import uvicorn

        from .server.app import create_app
        uvicorn.run(create_app(start=args.start, speed=args.speed), host=args.host, port=args.port,
                    log_level="warning")


if __name__ == "__main__":
    main()
