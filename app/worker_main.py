"""Command-line worker entrypoint for Hydra Engine."""

from __future__ import annotations

import asyncio
import logging

from app.worker import run_worker_forever


def main() -> None:
    """Run the durable workflow worker until the process is stopped."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(run_worker_forever())


if __name__ == "__main__":
    main()
