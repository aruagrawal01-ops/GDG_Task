from datetime import datetime, timedelta, timezone

from sensor_service.engine import Reading, Rule, RuleEngine, Window

BASE = datetime(2026, 10, 12, 10, 0, tzinfo=timezone.utc)


def at(seconds, value, sensor="s1", reading_type="temperature"):
    return Reading(sensor, reading_type, value, BASE + timedelta(seconds=seconds))


def single_rule(**fields):
    return Rule(**{"id": 1, "name": "hot", "sensor_type": "temperature", "sensor_id": None,
                   "kind": "single", "operator": ">", "threshold": 30.0, **fields})


def window_rule(**fields):
    return Rule(**{"id": 2, "name": "avg hot", "sensor_type": "temperature", "sensor_id": None,
                   "kind": "window", "operator": ">", "threshold": 28.0, "window_seconds": 600,
                   "aggregation": "avg", "min_samples": 1, **fields})


def engine_with(*rules):
    engine = RuleEngine()
    engine.set_rules(rules)
    return engine


def test_single_rule_breach_and_no_breach():
    engine = engine_with(single_rule())
    assert engine.evaluate([at(0, 29.9)]) == []
    [candidate] = engine.evaluate([at(0, 34.5)])
    assert (candidate.rule_id, candidate.sensor_id, candidate.first_value) == (1, "s1", 34.5)
    assert candidate.message == "temperature 34.5 > 30"


def test_single_rule_breaches_in_one_batch_collapse_per_sensor():
    engine = engine_with(single_rule())
    candidates = engine.evaluate([at(0, 31), at(1, 35), at(2, 20), at(3, 33, sensor="s2")])
    by_sensor = {c.sensor_id: c for c in candidates}
    assert by_sensor["s1"].count == 2
    assert (by_sensor["s1"].first_value, by_sensor["s1"].last_value) == (31, 35)
    assert by_sensor["s2"].count == 1


def test_rule_scoped_to_sensor_and_type():
    engine = engine_with(single_rule(sensor_id="s2"))
    assert engine.evaluate([at(0, 50), at(0, 50, sensor="s2", reading_type="occupancy")]) == []
    assert len(engine.evaluate([at(0, 50, sensor="s2")])) == 1


def test_window_average_crosses_threshold():
    engine = engine_with(window_rule())
    assert engine.evaluate([at(0, 20), at(60, 30)]) == []
    [candidate] = engine.evaluate([at(120, 40)])
    assert candidate.last_value == 30.0
    assert "3 samples" in candidate.message


def test_window_respects_min_samples():
    engine = engine_with(window_rule(min_samples=3))
    assert engine.evaluate([at(0, 50), at(1, 50)]) == []
    assert len(engine.evaluate([at(2, 50)])) == 1


def test_window_evicts_old_readings():
    engine = engine_with(window_rule())
    engine.evaluate([at(0, 100)])
    assert engine.evaluate([at(700, 20)]) == []


def test_window_aggregations():
    for aggregation, threshold, expected in (("max", 39, 40), ("min", 9, 10), ("sum", 59, 60), ("count", 2, 3)):
        engine = engine_with(window_rule(aggregation=aggregation, threshold=threshold))
        [candidate] = engine.evaluate([at(0, 10), at(1, 40), at(2, 10)])
        assert candidate.last_value == expected


def test_window_out_of_order_and_too_old_readings():
    window = Window(600)
    assert window.add(1000, 1.0)
    assert window.add(900, 2.0)
    assert [p[0] for p in window.points] == [900, 1000]
    assert not window.add(400, 3.0)
    assert window.values() == [2.0, 1.0]


def test_changed_rule_rebuilds_window_from_history():
    rule = window_rule(version=1)
    engine = RuleEngine()
    engine.set_rules([rule])
    engine.evaluate([at(0, 10)])
    assert engine.window_count == 1

    history = [("s1", (BASE + timedelta(seconds=s)).timestamp(), 50.0) for s in (0, 10)]
    engine.set_rules([window_rule(version=2)], load_history=lambda r: history)
    [candidate] = engine.evaluate([at(20, 50)])
    assert "3 samples" in candidate.message


def test_unchanged_rule_keeps_window():
    engine = RuleEngine()
    engine.set_rules([window_rule(version=1)])
    engine.evaluate([at(0, 10)])
    engine.set_rules([window_rule(version=1)], load_history=lambda r: [])
    [candidate] = engine.evaluate([at(1, 50)])
    assert candidate.last_value == 30.0
