import math
import operator
from collections import deque
from dataclasses import dataclass
from datetime import datetime

OPERATORS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le}
AGGREGATIONS = {
    "avg": lambda values: math.fsum(values) / len(values),
    "min": min,
    "max": max,
    "sum": math.fsum,
    "count": len,
}


@dataclass(slots=True)
class Reading:
    sensor_id: str
    type: str
    value: float
    ts: datetime

    @property
    def epoch(self):
        return self.ts.timestamp()


@dataclass(frozen=True)
class Rule:
    id: int
    name: str
    sensor_type: str
    sensor_id: str | None
    kind: str
    operator: str
    threshold: float
    window_seconds: int | None = None
    aggregation: str | None = None
    min_samples: int = 1
    version: object = None

    @classmethod
    def from_row(cls, row):
        return cls(
            id=row["id"],
            name=row["name"],
            sensor_type=row["sensor_type"],
            sensor_id=row["sensor_id"],
            kind=row["kind"],
            operator=row["operator"],
            threshold=row["threshold"],
            window_seconds=row["window_seconds"],
            aggregation=row["aggregation"],
            min_samples=row["min_samples"],
            version=row["updated_at"],
        )

    def applies_to(self, sensor_id):
        return self.sensor_id is None or self.sensor_id == sensor_id

    def breached(self, value):
        return OPERATORS[self.operator](value, self.threshold)

    def describe(self, value, samples=None):
        if self.kind == "single":
            return f"{self.sensor_type} {value:g} {self.operator} {self.threshold:g}"
        return (
            f"{self.aggregation}({self.sensor_type}) over {self.window_seconds}s = {value:.2f} "
            f"{self.operator} {self.threshold:g} ({samples} samples)"
        )


class Window:
    __slots__ = ("span", "points")

    def __init__(self, span):
        self.span = span
        self.points = deque()

    def add(self, epoch, value):
        points = self.points
        if points and epoch < points[-1][0]:
            if epoch <= points[-1][0] - self.span:
                return False
            index = len(points)
            while index > 0 and points[index - 1][0] > epoch:
                index -= 1
            points.insert(index, (epoch, value))
        else:
            points.append((epoch, value))
        cutoff = points[-1][0] - self.span
        while points[0][0] <= cutoff:
            points.popleft()
        return True

    def values(self):
        return [value for _, value in self.points]


@dataclass
class AlertCandidate:
    rule_id: int
    sensor_id: str
    message: str
    first_value: float
    last_value: float
    first_ts: datetime
    last_ts: datetime
    count: int = 1


class RuleEngine:
    """Evaluates readings against the enabled rules. Only the ingest consumer thread uses it."""

    def __init__(self):
        self._rules = {}
        self._by_type = {}
        self._windows = {}

    @property
    def rule_count(self):
        return len(self._rules)

    @property
    def window_count(self):
        return len(self._windows)

    def clear(self):
        self._rules = {}
        self._by_type = {}
        self._windows = {}

    def set_rules(self, rules, load_history=None):
        new_rules = {rule.id: rule for rule in rules}
        for rule_id, old in self._rules.items():
            if new_rules.get(rule_id) != old:
                self._windows = {key: w for key, w in self._windows.items() if key[0] != rule_id}

        for rule in new_rules.values():
            if rule.kind == "window" and self._rules.get(rule.id) != rule and load_history:
                for sensor_id, epoch, value in load_history(rule):
                    self._window(rule, sensor_id).add(epoch, value)

        by_type = {}
        for rule in new_rules.values():
            by_type.setdefault(rule.sensor_type, []).append(rule)
        self._rules = new_rules
        self._by_type = by_type

    def evaluate(self, readings):
        found = {}
        touched = {}
        for reading in readings:
            for rule in self._by_type.get(reading.type, ()):
                if not rule.applies_to(reading.sensor_id):
                    continue
                if rule.kind == "single":
                    if rule.breached(reading.value):
                        self._record(found, rule, reading.sensor_id, reading.value, reading.ts,
                                     rule.describe(reading.value))
                elif self._window(rule, reading.sensor_id).add(reading.epoch, reading.value):
                    touched[(rule.id, reading.sensor_id)] = (rule, reading.ts)

        for key, (rule, ts) in touched.items():
            values = self._windows[key].values()
            if len(values) < rule.min_samples:
                continue
            aggregate = AGGREGATIONS[rule.aggregation](values)
            if rule.breached(aggregate):
                self._record(found, rule, key[1], aggregate, ts, rule.describe(aggregate, len(values)))
        return list(found.values())

    def _window(self, rule, sensor_id):
        key = (rule.id, sensor_id)
        window = self._windows.get(key)
        if window is None:
            window = self._windows[key] = Window(rule.window_seconds)
        return window

    @staticmethod
    def _record(found, rule, sensor_id, value, ts, message):
        key = (rule.id, sensor_id)
        candidate = found.get(key)
        if candidate is None:
            found[key] = AlertCandidate(rule.id, sensor_id, message, value, value, ts, ts)
        else:
            candidate.message = message
            candidate.last_value = value
            candidate.last_ts = max(candidate.last_ts, ts)
            candidate.count += 1
