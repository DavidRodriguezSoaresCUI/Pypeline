import logging
from datetime import datetime

from .activity import Activity, ActivityCreator
from .timed_execution_rule import TimedExecutionRule
from .utils import PYPELINE_LOGGER

LOG = logging.getLogger(PYPELINE_LOGGER)


class ActivityBootstrapRule:
    """Handles the logic related to parsing an activity boostrap rule and using it to create Activity instances"""

    activity_type: str
    """Activity type"""
    bootstrap_execution: TimedExecutionRule
    """Rule for when to bootstrap the activity"""
    activity_file_contents: str
    """Contents of the activity file"""
    fire_on_next_cycle: bool
    """Used to not to create an activity on the very first call"""

    def __init__(
        self,
        activity_type: str,
        bootstrap_rule: str,
        activity_file_contents: str | None = None,
        fire_on_first_cycle: bool = True,
    ) -> None:
        self.activity_type = activity_type
        _rule = TimedExecutionRule.from_expression(bootstrap_rule)
        if _rule is None:
            raise ValueError(f"Can't decode rule '{bootstrap_rule}'")
        self.bootstrap_execution = _rule
        self.activity_file_contents = (
            "" if activity_file_contents is None else activity_file_contents
        )
        self.fire_on_next_cycle = fire_on_first_cycle

    def apply(self, current_time: datetime, reserved_ids: set[str]) -> Activity | None:
        """If appropriate, returns a new activity to be created"""
        if self.bootstrap_execution.is_up(current_time):
            self.bootstrap_execution.mark_executed(current_time)
            LOG.debug(
                "ActivityBootstrapRule.apply: execution for type %s; fire_on_next_cycle=%s",
                self.activity_type,
                self.fire_on_next_cycle,
            )

            if self.fire_on_next_cycle:
                return ActivityCreator.get_instance().create_activity(
                    activity_type=self.activity_type,
                    activity_data=self.activity_file_contents,
                    current_time=current_time,
                    reserved_ids=reserved_ids,
                )
            self.fire_on_next_cycle = True
        return None
