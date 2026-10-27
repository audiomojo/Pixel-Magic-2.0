"""DescribeWorkflow: describes every candidate photo without features, then re-ranks when it
described something or the sets changed since the last ranking.
TrainWorkflow: learns from the picks, then re-ranks when the new model was promoted.

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

# Every start of TrainWorkflow (nightly schedule, the TypeScript server) uses this id with the
# USE_EXISTING id-conflict policy, so a trigger while one runs joins it instead of a second run.
TRAIN_WORKFLOW_ID = "ai-train"

_SHORT = dict(start_to_close_timeout=timedelta(minutes=2))
# train_ranker and rank_sets
_LONG = dict(
    start_to_close_timeout=timedelta(minutes=30),
    retry_policy=RetryPolicy(maximum_attempts=3),
)
_DESCRIBE = dict(
    start_to_close_timeout=timedelta(minutes=10),
    retry_policy=RetryPolicy(maximum_attempts=5),
)


@workflow.defn(name="DescribeWorkflow")
class DescribeWorkflow:
    @workflow.run
    async def run(
        self, concurrency: int, described: int = 0, after_sha: str | None = None
    ) -> int:
        """Photos described (ok or failed) over the whole continue-as-new chain.

        Pages walk the backlog by a sha256 cursor (carried through continue-as-new), so a photo
        that keeps failing is tried once per scheduled run and never hides the ones after it."""
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
                    # backlog; the cursor moves past it and the next scheduled run tries again.
                    workflow.logger.warning("describe_photo %s gave up: %s", sha256, e.cause)
                    return False

        for _ in range(PAGES_PER_RUN):
            page = await workflow.execute_activity(
                "describe_backlog_page",
                args=[PAGE_SIZE, after_sha],
                result_type=list[tuple[str, str]],
                **_SHORT,
            )
            if not page:
                break
            done = await asyncio.gather(*(describe(sha, ext) for sha, ext in page))
            described += sum(done)
            after_sha = page[-1][0]
        else:
            workflow.continue_as_new(args=[concurrency, described, after_sha])

        # Scores are set-relative: sets that merged or split need ranking again even when
        # nothing new was described.
        if described > 0 or await workflow.execute_activity(
            "rank_needed", result_type=bool, **_SHORT
        ):
            await workflow.execute_activity("rank_sets", result_type=int, **_LONG)
        return described


@workflow.defn(name="TrainWorkflow")
class TrainWorkflow:
    @workflow.run
    async def run(self) -> int | None:
        """The promoted model version, or None when the new model was kept unpromoted."""
        version = await workflow.execute_activity(
            "train_ranker", result_type=int | None, **_LONG
        )
        if version is not None:
            await workflow.execute_activity("rank_sets", result_type=int, **_LONG)
        return version
