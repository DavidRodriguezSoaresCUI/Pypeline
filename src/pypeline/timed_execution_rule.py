import logging
import re
from datetime import datetime, timedelta
from typing import Callable

from .utils import (
    PYPELINE_LOGGER,
    TimeResolution,
    clone_datetime,
    datetime_to_cron_day,
)

LOG = logging.getLogger(PYPELINE_LOGGER)


class TimedExecutionRule:
    """An execution rule specifies the scheduling of a periodical action.
    By design for this particular use, there is a strong time resolution
    focus on the minute to day of the week range.

    It hinges on defining a function that can resolve next execution time, with signature
    <last_execution: datetime | None>, <current_time: datetime> -> <next_execution: datetime>

    Usage : Initialize via one of the `from_*` class methods, then use `is_up` to check if the time for next execution is up and `mark_executed` after execution
    """

    next_datetime_generator: Callable[[datetime | None, datetime], datetime]
    """Resolves next execution time from last execution time and current time. Computed at initialization by `from_*` class methods."""
    next_execution: datetime
    """Stores the last retruned value of `next_datetime_generator` to avoid calling it again"""

    CRON_LITE_STYLE_RULE_PATTERN = re.compile(
        r" ".join(
            [
                r"((?:[0-5]?\d,)*[0-5]?\d|\*)",  # minute in hour
                r"((?:[0-1]?\d,|2[0-4],)*(?:[0-1]?\d|2[0-4])|\*)",  # hour in day
                r"((?:[0-6],)*[0-6]|(?:[A-Z]{3},)*[A-Z]{3}|\*)",  # day of the week
            ]
        )
    )
    CRON_LITE_DAY_OF_WEEK = {
        "SUN": 0,
        "MON": 1,
        "TUE": 2,
        "WED": 3,
        "THU": 4,
        "FRI": 5,
        "SAT": 6,
    }
    SIMPLE_FREQUENCY_MACRO_PATTERN = re.compile(r"@every (\d+)(m|h)")
    SIMPLE_FREQUENCY_MACRO_SUFFIX_MAPPER = {
        "m": "minutes",
        "h": "hours",
    }

    def __init__(
        self, next_datetime_generator: Callable[[datetime | None, datetime], datetime]
    ) -> None:
        self.next_datetime_generator = next_datetime_generator
        self.next_execution = next_datetime_generator(None, datetime.now())

    @classmethod
    def __from_simple_frequency_macro(
        cls, simple_frequency_macro: str
    ) -> "TimedExecutionRule | None":
        """Decodes a simple frequency macro"""

        _match = cls.SIMPLE_FREQUENCY_MACRO_PATTERN.match(simple_frequency_macro)
        if _match is None:
            return None

        _count = int(_match.group(1))
        _unit = cls.SIMPLE_FREQUENCY_MACRO_SUFFIX_MAPPER[_match.group(2)]
        _delta = timedelta(**{_unit: _count})  # type: ignore[arg-type]

        def simple_frequency_next_datetime_generator(
            last_execution: datetime | None, current_time: datetime
        ) -> datetime:
            if last_execution is None:
                return current_time
            return last_execution + _delta

        return TimedExecutionRule(simple_frequency_next_datetime_generator)

    @classmethod
    def __from_cronlite(cls, cron_lite_rule: str) -> "TimedExecutionRule | None":
        """See CRON_lite.md for specifications and syntax"""

        # Check if cronlite rule
        _match = cls.CRON_LITE_STYLE_RULE_PATTERN.match(cron_lite_rule)
        if _match is None:
            return None

        _parts = cron_lite_rule.split()
        if len(_parts) != 3:
            LOG.warning("Not a cronlite rule: '%s'", cron_lite_rule)
            return None

        def parse_cronlite_part(
            value: str, time_resolution: TimeResolution
        ) -> set[int] | None:
            """Returns values as set of integers"""
            allowed_values = set(range(time_resolution.value))

            # parse special characters
            if value == "*":
                return allowed_values

            # parse values
            values = {
                int(
                    cls.CRON_LITE_DAY_OF_WEEK.get(v, v)
                    if time_resolution is TimeResolution.DAY
                    else v
                )
                for v in value.split(",")
            }

            # check for illegal values
            illegal_values = [v for v in values if v not in allowed_values]
            if illegal_values:
                LOG.warning("Illegal values %s", illegal_values)

            return values

        # parsing allowed execution times
        _mins = parse_cronlite_part(_parts[0], TimeResolution.MINUTE)
        _hours = parse_cronlite_part(_parts[1], TimeResolution.HOUR)
        _days = parse_cronlite_part(_parts[2], TimeResolution.DAY)

        if not _mins or not _hours or not _days:
            LOG.warning("Not a valid cronlite expression: %s", cron_lite_rule)
            return None

        def cronlite_next_datetime_generator(
            last_execution: datetime | None, current_time: datetime
        ) -> datetime:
            """Measured to typically take <.2ms to generate the next execution time on a slow ARM NAS"""

            def time_to_wait(
                current_value: int,
                valid_values: set[int],
                time_resolution: TimeResolution,
            ) -> timedelta:
                return timedelta(
                    **{
                        time_resolution.name.lower()
                        + "s": min(
                            (v - current_value) % time_resolution.value
                            for v in valid_values
                        )
                    }
                )

            next_time = (
                current_time
                if last_execution is None
                else last_execution + timedelta(minutes=1)
            )
            while True:
                if (next_time_day := datetime_to_cron_day(next_time)) not in _days:
                    next_time = clone_datetime(
                        next_time, TimeResolution.DAY
                    ) + time_to_wait(next_time_day, _days, TimeResolution.DAY)
                elif next_time.hour not in _hours:
                    next_time = clone_datetime(
                        next_time, TimeResolution.HOUR
                    ) + time_to_wait(next_time.hour, _hours, TimeResolution.HOUR)
                elif next_time.minute not in _mins:
                    next_time += +time_to_wait(
                        next_time.minute, _mins, TimeResolution.MINUTE
                    )
                else:
                    return next_time

        return TimedExecutionRule(cronlite_next_datetime_generator)

    @classmethod
    def from_expression(cls, expression: str) -> "TimedExecutionRule | None":
        """Main expression decoder function"""

        for parser in (cls.__from_simple_frequency_macro, cls.__from_cronlite):
            if (res := parser(expression)) is not None:
                return res
        return None

    def __compute_next_execution(self, current_time: datetime) -> datetime:
        """Private method; Assumes the value in self.next_execution is being marked as executed, thus is actually the 'last execution time'"""
        _last_execution = self.next_execution
        return self.next_datetime_generator(_last_execution, current_time)

    def is_up(self, current_time: datetime) -> bool:
        """Checks if timer is up for next execution"""
        return current_time >= self.next_execution

    def mark_executed(self, current_time: datetime) -> None:
        """To be called when the associated action is executed, to update the 'next execution' timer"""
        _last_execution = self.next_execution
        self.next_execution = self.__compute_next_execution(current_time)
        LOG.debug(
            "mark_executed called, switching next_execution=%s -> %s",
            _last_execution,
            self.next_execution,
        )
