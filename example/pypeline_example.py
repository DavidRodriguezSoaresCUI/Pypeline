import logging
import time
from dataclasses import dataclass
from pathlib import Path

from pypeline.activity import Activity, ExitState, ActivityData
from pypeline.pypeline import Orchestrator
from pypeline.processor import Processor, get_available_processors

PYPELINE_EXAMPLE_DIR = Path(__file__).parent / "pypeline"


@dataclass
class ExceptionActivity(ActivityData):
    """Dumb class, needs to be JSON serializable"""

    wait_before_raise: int
    """Wait n seconds before raising exception"""


class ExceptionProcessor(Processor):
    """Raises an exception"""

    INPUT_ACTIVITY_TYPE = "ExceptionActivity"
    OUTPUT_ACTIVITY_TYPES = set()

    @classmethod
    def execute(cls, activity: Activity, log: logging.Logger) -> ExitState:
        """Waits some time and raises exception"""

        wait_before_raise = ExceptionActivity.from_json(activity.data).wait_before_raise

        time.sleep(wait_before_raise)

        raise RuntimeError("Sneaky error")


@dataclass
class SumActivity(ActivityData):
    """Dumb class, needs to be JSON serializable"""

    sum_up_to: int
    """Sum integers up to this number (included)"""


class SumProcessor(Processor):
    """Wastes time by summing integers up to a given limit"""

    INPUT_ACTIVITY_TYPE = "SumActivity"
    OUTPUT_ACTIVITY_TYPES = set()

    @classmethod
    def execute(cls, activity: Activity, log: logging.Logger) -> ExitState:
        """Computes the sum"""

        sum_up_to = SumActivity.from_json(activity.data).sum_up_to

        _sum = 0
        for x in range(sum_up_to + 1):
            _sum += x
        log.info(
            "Sum of integers up to %s (not included) is %s",
            sum_up_to,
            _sum,
        )

        return ExitState.success("All is done")


def main() -> None:
    Orchestrator(
        processors=get_available_processors(globals()),
        root_dir=PYPELINE_EXAMPLE_DIR,
        worker_id="ExampleWorker",
    ).run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
    print("END OF PROGRAM")
