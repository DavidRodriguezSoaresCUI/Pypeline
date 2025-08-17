import unittest
from datetime import datetime, timedelta

from .pypeline import TimedExecutionRule


def datetime_difference_under_a_second(
    datetime_a: datetime, datetime_b: datetime
) -> bool:
    """Checks whether the difference between datetime instances is under a second"""
    _diff: timedelta = datetime_a - datetime_b
    return abs(_diff.total_seconds()) < 1.0


class TimedExecutionRuleTest(unittest.TestCase):
    def test_is_up_and_mark_executed_normal_workflow(self) -> None:
        sut = TimedExecutionRule.from_expression("@every 5m")
        curr_timestamp = datetime.now()
        assert (
            datetime_difference_under_a_second(sut.next_execution, curr_timestamp)
            is True
        )
        assert sut.is_up(curr_timestamp) is True
        sut.mark_executed(curr_timestamp)

        for _ in range(5):
            curr_timestamp += timedelta(minutes=4, seconds=58)
            assert sut.is_up(curr_timestamp) is False
            curr_timestamp += timedelta(seconds=2)
            assert sut.is_up(curr_timestamp) is True
            sut.mark_executed(curr_timestamp)
