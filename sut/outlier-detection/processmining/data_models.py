from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class DiscoveredModel:
    version: int
    model: str
    timestamp: int


@dataclass
class ProcessEvent:
    case: str
    activity: str
    timestamp: datetime
    sequence: int
    model_version: int | None = None
    model_update: DiscoveredModel | None = None


@dataclass
class DirectlyFollowsData:
    directly_follows_graph: Counter[tuple[str, str]]
    activities: list[str]
    activities_occurrences: Counter[str]
    start_activities: Counter[str]
    end_activities: Counter[str]


@dataclass
class DiscoveryWindow:
    events: list[ProcessEvent]
    directly_follows_data: DirectlyFollowsData | None


@dataclass
class CaseConformanceState:
    model_version: int
    pending_events: list[ProcessEvent]
    processed_events: list[ProcessEvent] = field(default_factory=list)
