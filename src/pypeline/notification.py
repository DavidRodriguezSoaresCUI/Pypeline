from dataclasses import dataclass
from .activity import ActivityData


NOTIFICATION_ACTIVITY_TYPE = "NotificationActivity"


@dataclass
class NotificationActivityData(ActivityData):
    """Represents notifications sent from a processor"""

    processor_name: str
    """Name of the processor"""
    notifications: str
    """List of notification messages"""
