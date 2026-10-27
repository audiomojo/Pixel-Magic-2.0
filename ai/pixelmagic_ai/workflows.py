"""DescribeWorkflow: describes every candidate photo without features, then re-ranks.

Deterministic: no I/O here, only activities. Activities are called by name so this module
imports nothing outside the SDK.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

PAGE_SIZE = 200
PAGES_PER_RUN = 5  # then continue-as-new, so the history stays small

_SHORT = dict(start_to_close_timeout=timedelta(minutes=2))
_RANK = dict(start_to_close_timeout=timedelta(minutes=30))
_DESCRIBE = dict(
    start_to_close_timeout=timedelta(minutes=10),
    retry_policy=RetryPolicy(maximum_attempts=5),
)


@workflow.defn(name="DescribeWorkflow")
class DescribeWorkflow:
    @workflow.run
    async def run(self, concurrency: int, described: int = 0) -> int:
        """Photos described (ok or failed) over the whole continue-as-new chain."""
        sem = asyncio.Semaphore(max(1, concurrency))

        async def describe(sha256: str, ext: str) -> bool:
            async with sem:
                try:
                    await workflow.execute_activity(
                        "describe_photo", args=[sha256, ext], result_type=str, **_DESCRIBE
                    )
                    return True
                except ActivityError as e:
                    # Out of retries (e.g. a vault file that cannot be read): it stays in the
                    # backlog for the next scheduled run; the rest carries on.
                    workflow.logger.warning("describe_photo %s gave up: %s", sha256, e.cause)
                    return False

        for _ in range(PAGES_PER_RUN):
            page = await workflow.execute_activity(
                "describe_backlog_page", PAGE_SIZE, result_type=list[tuple[str, str]], **_SHORT
            )
            if not page:
                break
            done = await asyncio.gather(*(describe(sha, ext) for sha, ext in page))
            ok = sum(done)
            described += ok
            if ok == 0:
                break  # only stuck photos are left: stop until the next scheduled run
        else:
            workflow.continue_as_new(args=[concurrency, described])

        if described > 0:
            await workflow.execute_activity("rank_sets", result_type=int, **_RANK)
        return described
