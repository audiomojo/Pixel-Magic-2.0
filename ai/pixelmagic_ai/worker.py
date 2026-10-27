"""The AI worker process: `python -m pixelmagic_ai.worker` (run by the launchd agent)."""

import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker

from .activities import Activities, Deps
from .config import Settings, load_settings
from .schedules import TASK_QUEUE, ensure_schedules
from .workflows import DescribeWorkflow

REPO_ROOT = Path(__file__).resolve().parents[2]

log = logging.getLogger("pixelmagic_ai")


def build_deps(settings: Settings) -> Deps:
    """The real models, loaded once per process."""
    from .embed import SiglipEmbedder

    if sys.platform == "darwin":
        from .faces import AppleVisionDetector

        detector = AppleVisionDetector()
    else:  # no Vision framework: no faces
        from .faces import FakeDetector

        detector = FakeDetector([])
    return Deps(settings, detector, SiglipEmbedder(device="mps"))


def build_worker(client: Client, deps: Deps, executor: ThreadPoolExecutor) -> Worker:
    n = deps.settings.describe_concurrency
    return Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[DescribeWorkflow],
        activities=Activities(deps).all(),
        activity_executor=executor,
        max_concurrent_activities=n,
    )


async def run(settings: Settings) -> None:
    log.info("loading models")
    deps = build_deps(settings)
    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    await ensure_schedules(client, settings)
    with ThreadPoolExecutor(max_workers=settings.describe_concurrency) as executor:
        log.info("worker on task queue %r, concurrency %d", TASK_QUEUE, settings.describe_concurrency)
        await build_worker(client, deps, executor).run()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run(load_settings(os.environ, REPO_ROOT)))


if __name__ == "__main__":
    main()
