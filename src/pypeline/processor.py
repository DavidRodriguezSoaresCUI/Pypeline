from abc import ABC, abstractmethod
import logging

from .activity import (
    VALID_ACTIVITY_TYPE_PATTERN,
    VALID_ACTIVITY_TYPE_PATTERN_HELP,
    Activity,
    ActivityCreator,
    ExitState,
)
from .notification import NotificationActivityData, NOTIFICATION_ACTIVITY_TYPE
from .properties_manager import PropertiesManager, PropertySpec


class Processor(ABC):
    """
    Each processor :

    - must at least implement :
        - class constant INPUT_ACTIVITY_TYPE: str
        - class constant OUTPUT_ACTIVITY_TYPES: set[str]
        - class method ``execute(cls, activity: Activity, log: logging.Logger, config: dict) -> ExitState``
    - should contain :
        - If reads properties through PropertyManager, these should be declared in a
          class constant PROCESSOR_PROPERTIES: list[PropertySpec]
        - If reads expects configuration as argument, its contents should be declared in a
          class constant PROCESSOR_CONFIGURATION: list[PropertySpec]
    - contains the logic required to process a specific type of activity
    - is implemented as a subclass of the base Processor class
    - is passed to the orchestrator, thus enabling it to process specific activity types
    - only contain class method and attributes => stateless by design
    - has access to a properties manager with `PropertiesManager.get_instance()`
      (tip: use `cls` as value for methods' parameter `obj`)
    - may create further activities using provided method `create_activities`
    - logs are produced in a file named ``<activityFileStem>.<processingStartTimestamp>.log``
      with the timestamp being a ISO date with seconds-level precision

    The implementation has access to the following methods:

    - `create_activities(cls, activity_type: str, activities_data: list[str]) -> None` : Used to create
      further activities (don't forget to declare them in OUTPUT_ACTIVITY_TYPES).
    - `notify(cls, notifications: list[str]) -> None` : Used to notify the user (will create activities)

    Design recommendations :

    - idempotence : Processing the same activity multiple times should produce the same result.
      Special attention is to be made in evaluation where re-processing is not desirable (resource-heavy
      computation, when it may lead to loss of data, etc)
    - atomicity : Activities are designed to represent an atomic workload, that is either fully completed
      or not at all. Writing Processors that may only partially complete the given workload is discouraged
      because of the ambiguous exit state, and may need to adopt strategies, like marking the partially
      successful activity as PROCESSED while creating a new activity containing the subset of failed work
      items for retry or failure.
    """

    __property_prefix__: str
    """(Class property) makes it simpler to interact with PropertyManager"""

    @classmethod
    def set_property_prefix(cls, value: str) -> None:
        """Sets property prefix to make it possible for PropertyManager to get the id for this processor.
        To be called before `execute`"""
        cls.__property_prefix__ = cls.get_input_activity_type() + "." + value

    @classmethod
    def validate(cls) -> None:
        """Executes some basic calls to check that class attributes are set.
        To be called by the Orchestrator instance at init time."""
        _input = cls.get_input_activity_type()
        if not isinstance(_input, str):
            raise ValueError(
                f"Processor {cls.__name__} has INPUT_ACTIVITY_TYPE of type {type(_input)} instead of str"
            )
        if not VALID_ACTIVITY_TYPE_PATTERN.match(_input):
            raise ValueError(
                f"Processor {cls.__name__} has invalid INPUT_ACTIVITY_TYPE '{_input}': {VALID_ACTIVITY_TYPE_PATTERN_HELP}"
            )
        _outputs = cls.get_output_activity_types()
        if not isinstance(_outputs, set):
            raise ValueError(
                f"Processor {cls.__name__} has OUTPUT_ACTIVITY_TYPES of type {type(_outputs)} instead of list"
            )
        _outputs_disallowed_item_type = {type(x) for x in _outputs}.difference({str})
        if _outputs_disallowed_item_type:
            raise ValueError(
                f"Processor {cls.__name__} has OUTPUT_ACTIVITY_TYPES containing items of type {_outputs_disallowed_item_type} instead of only str"
            )

    @classmethod
    def get_input_activity_type(cls) -> str:
        """Declares which activity type the processor accepts"""
        res = getattr(cls, "INPUT_ACTIVITY_TYPE")
        if res is None:
            raise NotImplementedError(
                f"Class constant 'INPUT_ACTIVITY_TYPE' not declared for processor {cls.__name__}"
            )
        return res

    @classmethod
    def get_output_activity_types(cls) -> set[str]:
        """Declares which activity types the processor can generate"""
        res = getattr(cls, "OUTPUT_ACTIVITY_TYPES")
        if res is None:
            raise NotImplementedError(
                f"Class constant 'OUTPUT_ACTIVITY_TYPES' not declared for processor {cls.__name__}"
            )
        return res

    @classmethod
    def get_properties(cls) -> list[PropertySpec]:
        """Declares which properties the processor may read. For now this is not enforced."""
        if not hasattr(cls, "PROCESSOR_PROPERTIES"):
            return []
        res = getattr(cls, "PROCESSOR_PROPERTIES")
        if not isinstance(res, list) or not all(
            isinstance(x, PropertySpec) for x in res
        ):
            raise ValueError(
                f"Expected list[PropertySpec] in {cls.__name__}.PROCESSOR_PROPERTIES, found {res}"
            )
        return res

    @classmethod
    def get_configuration(cls) -> list[PropertySpec]:
        """Declares which properties the processor may read. For now this is not enforced."""
        if not hasattr(cls, "PROCESSOR_CONFIGURATION"):
            return []
        res = getattr(cls, "PROCESSOR_CONFIGURATION")
        if not isinstance(res, list) or not all(
            isinstance(x, PropertySpec) for x in res
        ):
            raise ValueError(
                f"Expected list[PropertySpec] in {cls.__name__}.PROCESSOR_CONFIGURATION, found {res}"
            )
        return res

    @classmethod
    @abstractmethod
    def execute(
        cls, activity: Activity, log: logging.Logger, config: dict
    ) -> ExitState:
        """Processes the activity"""

    @classmethod
    def create_activities(
        cls, activity_type: str, activities_data: list[str], start_delay_s: int = 0
    ) -> None:
        """Create new activities. Takes parameters:
        `activity_type`: name of the activity
        `activities_data`: list of activities' data as strings (typically JSON-encoded data)
        `start_delay_s`: optional. Sets the number of seconds in the future for a timestamp with which to mark the created activities with, so they are not processed before said timestamp
        """
        pm: PropertiesManager = PropertiesManager.get_instance()
        activity_limit = pm.get_int(cls, "activity-creation-limit", default=10)
        if len(activities_data) > activity_limit:
            raise ValueError(
                f"Tried to exceed activity creation limits: max={activity_limit} given={len(activities_data)}"
            )
        ac: ActivityCreator = ActivityCreator.get_instance()
        ac.create_activities(
            activity_type, activities_data, cls.__name__, start_delay_s=start_delay_s
        )

    @classmethod
    def notify(cls, notifications: list[str]) -> None:
        """Creates a NotificationActivity"""
        cls.create_activities(
            NOTIFICATION_ACTIVITY_TYPE,
            [NotificationActivityData(cls.__name__, notifications).to_json()],
        )


def get_available_processors(_globals: dict) -> list[type]:
    """Useful for callers of the Orchestrator constructor"""
    return [
        v
        for v in _globals.values()
        if isinstance(v, type) and issubclass(v, Processor) and v is not Processor
    ]
