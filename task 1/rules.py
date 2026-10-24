import operator

OPERATORS = {">": operator.gt, ">=": operator.ge, "<": operator.lt, "<=": operator.le}


def create_alert(cur, rule, sensor_id, value):
    cur.execute(
        """
        INSERT INTO alerts (rule_id, sensor_id, value) VALUES (%s, %s, %s)
        ON CONFLICT (rule_id, sensor_id) WHERE status <> 'resolved' DO NOTHING
        """,
        (rule["id"], sensor_id, value),
    )
    return cur.rowcount


def window_average(cur, rule, sensor_id, latest_time):
    cur.execute(
        """
        SELECT AVG(value) AS average FROM readings
        WHERE sensor_id = %s AND type = %s
          AND timestamp > %s - make_interval(mins => %s) AND timestamp <= %s
        """,
        (sensor_id, rule["type"], latest_time, rule["window_minutes"], latest_time),
    )
    return cur.fetchone()["average"]


def check_rules(cur, readings):
    cur.execute("SELECT * FROM rules WHERE enabled")
    rules = cur.fetchall()
    alerts_created = 0

    for rule in rules:
        compare = OPERATORS[rule["operator"]]
        matching = [r for r in readings if r["type"] == rule["type"]]

        if rule["window_minutes"] is None:
            for reading in matching:
                if compare(reading["value"], rule["threshold"]):
                    alerts_created += create_alert(cur, rule, reading["sensor_id"], reading["value"])
        else:
            latest = {}
            for reading in matching:
                sensor_id = reading["sensor_id"]
                if sensor_id not in latest or reading["timestamp"] > latest[sensor_id]:
                    latest[sensor_id] = reading["timestamp"]
            for sensor_id, latest_time in latest.items():
                average = window_average(cur, rule, sensor_id, latest_time)
                if average is not None and compare(average, rule["threshold"]):
                    alerts_created += create_alert(cur, rule, sensor_id, average)

    return alerts_created
