import logging
import unittest
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict

from pypeline.timed_execution_rule import TimedExecutionRule
from pypeline.utils import add_file_handler

LOG = logging.getLogger(__file__)
add_file_handler(LOG, Path(__file__ + ".log"))


def datetime_difference_under_a_second(
    datetime_a: datetime, datetime_b: datetime
) -> bool:
    """Checks whether the difference between datetime instances is under a second"""
    _diff: timedelta = datetime_a - datetime_b
    return abs(_diff.total_seconds()) < 1.0


def mkdate(hour: int = 0, minute: int = 0) -> datetime:
    """2023 was picked because it begins with a sunday"""
    return datetime(2023, month=1, day=1, hour=hour, minute=minute)


class TimedExecutionRuleTest(unittest.TestCase):
    """Test for class TimedExecutionRuleTest. Are implicitely covered:

    - TimedExecutionRule.__init__
    - TimedExecutionRule.from_expression (partially)
    - TimedExecutionRule.is_up
    - TimedExecutionRule.mark_executed
    """

    def test_from_simple_frequency_macro(self) -> None:
        """covers TimedExecutionRule.from_simple_frequency_macro"""
        sut = TimedExecutionRule.from_expression("@every 5m")
        curr_timestamp = datetime.now()
        # inject curr_timestamp into sut
        sut.next_execution = curr_timestamp

        # simulates the earliest condition where time is up
        assert sut.is_up(curr_timestamp) is True
        # this call is necessary for the next execution time to be computed
        sut.mark_executed(curr_timestamp)

        for _ in range(5):
            curr_timestamp += timedelta(minutes=4, seconds=58)
            assert sut.is_up(curr_timestamp) is False
            curr_timestamp += timedelta(seconds=2)
            assert sut.is_up(curr_timestamp) is True
            sut.mark_executed(curr_timestamp)

    def test_from_cronlite_times(self) -> None:
        """covers TimedExecutionRule.from_cronlite for generated times"""

        TestTimeSpec = namedtuple(
            "TestTimeSpec", field_names=("initial", "first_delta", "next_deltas")
        )

        examples: Dict[str, Callable[[int], timedelta]] = {
            "0,30 * *": TestTimeSpec(
                mkdate(minute=2),
                timedelta(minutes=28),
                lambda _: timedelta(
                    minutes=30  # happen twice an hour at 30 minutes intervals
                ),
            ),
            "* * *": TestTimeSpec(
                mkdate(minute=1),
                timedelta(minutes=1),
                lambda _: timedelta(minutes=1),  # happen every minute
            ),
            "0 * *": TestTimeSpec(
                mkdate(minute=1),
                timedelta(minutes=59),
                lambda _: timedelta(hours=1),  # happen every hour
            ),
            "0 0,4,6 *": TestTimeSpec(
                mkdate(minute=1),
                timedelta(hours=3, minutes=59),
                lambda n: timedelta(
                    hours=2
                    if n % 3 == 1
                    else (
                        18 if n % 3 == 2 else 4
                    )  # happen 3 times a day with various intervals
                ),
            ),
            "0 0 *": TestTimeSpec(
                mkdate(hour=7, minute=1),
                timedelta(hours=16, minutes=59),
                lambda _: timedelta(hours=24),  # happen every day
            ),
            "0 0 2": TestTimeSpec(
                mkdate(hour=7, minute=1),
                timedelta(days=1, hours=16, minutes=59),
                lambda _: timedelta(days=7),  # happen every week
            ),
            "0 0 2,5": TestTimeSpec(
                mkdate(hour=7, minute=1),
                timedelta(days=1, hours=16, minutes=59),
                lambda n: timedelta(
                    days=3 if n % 2 == 1 else 4
                ),  # happen every 3/4 days
            ),
        }

        for cronlite_rule, time_spec in examples.items():
            sut = TimedExecutionRule.from_expression(cronlite_rule)
            sut.next_execution = time_spec.initial

            # (discounting the actual "first execution time" that was manually set)
            # check that the first generated next execution time is set to happen after the expected amount of time
            _next = sut.compute_next_execution(time_spec.initial)
            assert (
                _next - time_spec.initial
            ) == time_spec.first_delta, f"{cronlite_rule}: {_next.isoformat()} - {time_spec.initial.isoformat()} != {time_spec.first_delta}"
            sut.mark_executed(time_spec.initial)

            for n in range(1, 100):
                # check if subsequent execution times happen at expected intervals
                _last = _next
                _next = sut.compute_next_execution(_last)
                assert (_next - _last) == time_spec.next_deltas(
                    n
                ), f"{cronlite_rule}: {_next.isoformat()} - {_last.isoformat()} != {time_spec.next_deltas(n)}"
                sut.mark_executed(_next)
