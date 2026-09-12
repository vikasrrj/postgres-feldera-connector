#!/usr/bin/env python3
"""Live harness for the postgres-cdc connector.

Drives a real PostgreSQL -> connector -> Feldera stack against a reference
model:
  - fault <point>   : crash-and-recover matrix (Phase 4)
  - fuzz [ops]      : random op sequence vs a reference table model (Phase 3)
  - smoke           : quick single-sync sanity check
  - setup           : idempotently create the test table, publication, pipeline

Requires (defaults match the created harness stack):
  - a throwaway PostgreSQL reachable at 127.0.0.1:5544 (password 'postgres')
  - Feldera at :8080 with a compiled, running 'cdc_test' pipeline
"""

import os
import random
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.parse
import json

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONNECTOR = os.environ.get("CONNECTOR_BIN", os.path.join(REPO, "target", "release", "postgres-cdc"))

PG_PORT = os.environ.get("HARNESS_PG_PORT", "5544")
PG_URL = os.environ.get("HARNESS_PG_URL", f"host=127.0.0.1 port={PG_PORT} user=postgres password=postgres")
FELDERA = os.environ.get("HARNESS_FELDERA_URL", "http://localhost:8080")
PIPELINE = os.environ.get("HARNESS_PIPELINE", "cdc_test")

PUBLICATION = "cdc_test_pub"
TABLE = "stock"
WORK_DIR = "/tmp/postgres-cdc-harness"

SEED = []

OP_POOL = [
    ("roll", "23", None),
    ("alice", "41", "hello"),
    ("bob", "7", "h\xe9llo 'quoted'"),
    ("carol", "12", None),
    ("dave", "55", "tab\there / back\\\\slash"),
    ("eve", "3", "\xe9\xfc\xf1"),
    ("frank", "99", "trailing space "),
    ("grace", "0", ""),
]


def pg(sql: str) -> str:
    proc = subprocess.run(
        ["docker", "exec", "harness-pg", "psql", "-U", "postgres", "-d", "postgres", "-qAt", "-c", sql],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed: {proc.stderr.strip()}\n  sql: {sql}")
    return proc.stdout


def feldera_query(sql: str):
    url = f"{FELDERA}/v0/pipelines/{PIPELINE}/query?sql={urllib.parse.quote(sql)}&format=json"
    with urllib.request.urlopen(url) as response:
        body = response.read().decode()
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def feldera_rows():
    rows = feldera_query(f"SELECT id, qty, note FROM {TABLE}")
    return sorted((int(r["id"]), int(r["qty"]), r.get("note")) for r in rows)


def pg_rows():
    out = pg(f"SELECT COALESCE(json_agg(json_build_object('id', id, 'qty', qty, 'note', note) ORDER BY id), '[]'::json)::text FROM {TABLE}")
    payload = json.loads(out.strip())
    rows = []
    for row in payload:
        rows.append((int(row["id"]), int(row["qty"]), row["note"]))
    return sorted(rows)


def create_feldera_pipeline():
    try:
        urllib.request.urlopen(f"{FELDERA}/v0/pipelines/{PIPELINE}", timeout=5)
        return
    except urllib.error.HTTPError:
        pass
    code = ("CREATE TABLE stock (id INT NOT NULL, qty INT NOT NULL, note VARCHAR, PRIMARY KEY (id)) "
            "WITH ('materialized' = 'true');")
    request = urllib.request.Request(
        f"{FELDERA}/v0/pipelines",
        data=json.dumps({"name": PIPELINE, "description": "harness", "program_code": code,
                         "runtime_config": {"workers": 1}}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(request, timeout=10)
    pg("SELECT 1")  # no-op ensure db reachable


class Connector:
    def __init__(self, env, slot, state_path):
        self.env = dict(os.environ)
        self.env.update({
            "POSTGRES_URL": env.get("POSTGRES_URL", PG_URL),
            "TABLES": TABLE,
            "PG_PUBLICATION": PUBLICATION,
            "PG_SLOT": slot,
            "SNAPSHOT_STATE_PATH": state_path,
            "FELDERA_URL": FELDERA,
            "FELDERA_PIPELINE": PIPELINE,
            "SNAPSHOT_BATCH_ROWS": env.get("SNAPSHOT_BATCH_ROWS", "1000"),
            "BATCH_MAX_ROWS": env.get("BATCH_MAX_ROWS", "1000"),
            "BATCH_LINGER_MS": "0",
        })
        if env.get("CRASH_AT"):
            self.env["CRASH_AT"] = env["CRASH_AT"]
            self.env["CRASH_AFTER_BATCHES"] = env.get("CRASH_AFTER_BATCHES", "1")
        self.process = None
        self.lines = []
        self._reader = None

    def start(self):
        os.makedirs(WORK_DIR, exist_ok=True)
        self.process = subprocess.Popen(
            [CONNECTOR], env=self.env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self):
        for line in self.process.stdout:
            self.lines.append(line.rstrip())

    def wait_log(self, needle, deadline):
        while time.monotonic() < deadline:
            if any(needle in line for line in self.lines):
                return True
            if self.process.poll() is not None:
                return False
            time.sleep(0.1)
        return False

    def wait_exit(self, deadline):
        while time.monotonic() < deadline:
            code = self.process.poll()
            if code is not None:
                return code
            time.sleep(0.1)
        return None

    def stop(self, timeout=20):
        if self.process is None or self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()

    def log(self):
        return "\n".join(self.lines[-25:])


def fresh_run():
    run_id = int(time.time())
    slot = f"ct_{run_id}"
    state_path = os.path.join(WORK_DIR, f"{slot}.json")
    return run_id, slot, state_path


def reset(slot, state_path):
    pg(f"SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}');")
    try:
        os.remove(state_path)
    except FileNotFoundError:
        pass
    pg(f"TRUNCATE {TABLE};")


def drop_slot(slot):
    try:
        pg(f"SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}');")
    except RuntimeError:
        pass


def wait_synced(model, connector, deadline):
    last = None
    stable = 0
    while time.monotonic() < deadline:
        if connector.process.poll() is not None:
            return False, last
        current = feldera_rows()
        if current == model:
            stable += 1
            last = current
            if stable >= 2:
                return True, current
        else:
            stable = 0
            last = current
        time.sleep(0.5)
    return False, last


def assert_equals(actual, expected, label):
    if actual != expected:
        print(f"FAIL {label}: expected {expected}, got {actual}")
        return False
    print(f"ok {label}: {len(actual)} rows consistent")
    return True


def cmd_fault(point, after_batches=1):
    run_id, slot, state_path = fresh_run()
    print(f"### fault {point} batch {after_batches} (run {run_id})")
    reset(slot, state_path)

    for i, (_, qty, note) in enumerate(OP_POOL[:6]):
        if note is None:
            note_sql = "NULL"
        else:
            note_sql = "'" + note.replace("'", "''") + "'"
        pg("INSERT INTO %s (id,qty,note) VALUES (%d,%s,%s);" % (TABLE, 5 + i, qty, note_sql))
    crasher = Connector({
        "CRASH_AT": point,
        "CRASH_AFTER_BATCHES": str(after_batches).lower(),
    }, slot, state_path)
    if point == "during-snapshot":
        crasher.env["SNAPSHOT_BATCH_ROWS"] = "4"

    crasher.start()
    crashed = False
    if point == "during-snapshot":
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            code = crasher.process.poll()
            if code is not None:
                crashed = code == 70
                break
            time.sleep(0.2)
    else:
        if not crasher.wait_log("Streaming from slot", time.monotonic() + 60):
            print("connector never reached the streaming phase")
            print(crasher.log())
            sys.exit(1)
        workload = [(90, 500, "line1\nline2"), (91, 501, None), (92, 502, "'"), (93, 503, None), (94, 504, "x")]
        ops = [("insert", id_, qty, note) for id_, qty, note in workload]
        for id_, qty, note in workload:
            note_sql = "NULL" if note is None else "'" + note.replace("'", "''") + "'"
            pg("INSERT INTO %s (id,qty,note) VALUES (%d,%s,%s);" % (TABLE, id_, qty, note_sql))
        code = crasher.wait_exit(time.monotonic() + 60)
        crashed = code == 70

    crasher.stop()
    if not crashed:
        print("FAIL: connector did not crash with the expected exit code 70")
        print(crasher.log())
        sys.exit(1)
    print(f"crashed as expected (rc 70); state file = {state_path}")

    if point == "during-snapshot":
        for id_, qty, note in [(95, 600, None), (96, 601, "after")]:
            note_sql = "NULL" if note is None else "'" + note.replace("'", "''") + "'"
            pg("INSERT INTO %s (id,qty,note) VALUES (%d,%s,%s);" % (TABLE, id_, qty, note_sql))

    model = pg_rows()
    recover = Connector({}, slot, state_path)
    recover.start()
    ok, final = wait_synced(model, recover, time.monotonic() + 90)
    recover.stop()
    print(crasher.log())
    great = assert_equals(final, model, f"{point} recovery") and ok
    drop_slot(slot)
    os.remove(state_path)
    print(f"### fault {point}: {'PASS' if great else 'FAIL'}")
    sys.exit(0 if great else 1)


def cmd_fuzz(ops=40, seed=None):
    rng = random.Random(seed)
    run_id, slot, state_path = fresh_run()
    print(f"### fuzz seed={seed or rng.randrange(10**9)} ops={ops} (run {run_id})")
    reset(slot, state_path)
    model = {}
    max_model = 0

    def next_op():
        ids = list(range(0, 150))
        free = [i for i in ids if i not in model]
        choice = rng.random()
        if not model or choice < 0.42:
            if not free:
                ids += [len(model) + 300]
            i = rng.choice(free or ids)
            return ("insert", i, *random_row(rng))
        if choice < 0.64:
            key = rng.choice(list(model))
            return ("update", key, *random_row(rng))
        if choice < 0.92:
            return ("delete", rng.choice(list(model)), None, None)
        return ("truncate", None, None, None)

    def random_row(rng):
        return rng.randint(0, 999), rng.choice(OP_POOL)[2]

    connector = Connector({"SNAPSHOT_BATCH_ROWS": "1000"}, slot, state_path)
    connector.start()
    if not connector.wait_log("Streaming from slot", time.monotonic() + 60):
        print("connector never reached streaming")
        print(connector.log())
        sys.exit(1)

    for n in range(ops):
        op = next_op()
        kind = op[0]
        if kind == "truncate":
            pg("BEGIN; TRUNCATE %s; COMMIT;" % TABLE)
            model.clear()
        elif kind == "insert":
            _, id_, qty, note = op
            note_sql = "NULL" if note is None else "'" + note.replace("'", "''") + "'"
            pg("INSERT INTO %s (id,qty,note) VALUES (%d,%s,%s);" % (TABLE, id_, qty, note_sql))
            model[id_] = (qty, note)
        elif kind == "update":
            _, id_, qty, note = op
            note_sql = "NULL" if note is None else "'" + note.replace("'", "''") + "'"
            pg("UPDATE %s SET qty=%s, note=%s WHERE id=%d;" % (TABLE, qty, note_sql, id_))
            model[id_] = (qty, note)
        else:
            _, id_, _, _ = op
            pg("DELETE FROM %s WHERE id=%d;" % (TABLE, id_))
            del model[id_]
        if len(model) > max_model:
            max_model = len(model)

    expected = sorted((i, q, n) for i, (q, n) in model.items())
    ok, final = wait_synced(expected, connector, time.monotonic() + 120)
    connector.stop()
    pg_ok = assert_equals(pg_rows(), expected, "pg matches model")
    feldera_ok = assert_equals(final, expected, "feldera matches model") and ok
    drop_slot(slot)
    print(f"### fuzz: {'PASS' if pg_ok and feldera_ok else 'FAIL'}; model size {len(model)}, max reached {max_model}")
    sys.exit(0 if pg_ok and feldera_ok else 1)


def cmd_smoke():
    run_id, slot, state_path = fresh_run()
    reset(slot, state_path)
    for i, (_, qty, note) in enumerate(OP_POOL[:3]):
        note_sql = "NULL" if note is None else "'" + note.replace("'", "''") + "'"
        pg("INSERT INTO %s (id,qty,note) VALUES (%d,%s,%s);" % (TABLE, 100 + i, qty, note_sql))
    connector = Connector({}, slot, state_path)
    connector.start()
    if not connector.wait_log("Streaming from slot", time.monotonic() + 60):
        print(connector.log())
        sys.exit(1)
    model = pg_rows()
    ok, final = wait_synced(model, connector, time.monotonic() + 60)
    connector.stop()
    assert_equals(final, model, "smoke sync")
    drop_slot(slot)
    sys.exit(0 if ok and final == model else 1)


def cmd_setup():
    pg("CREATE TABLE IF NOT EXISTS %s (id int primary key, qty int not null default 0, note text);" % TABLE)
    pg("DROP PUBLICATION IF EXISTS cdc_test_pub;")
    pg("CREATE PUBLICATION cdc_test_pub FOR TABLE %s WITH (publish = 'insert, update, delete, truncate');" % TABLE)
    create_feldera_pipeline()
    print("setup complete")


def main():
    action = sys.argv[1] if len(sys.argv) > 1 else "smoke"
    if action == "setup":
        cmd_setup()
    elif action == "smoke":
        cmd_smoke()
    elif action == "fault":
        if len(sys.argv) < 3:
            print("usage: harness.py fault <during-snapshot|before-send|after-send|after-durable|after-ack> [after_batches]")
            sys.exit(2)
        cmd_fault(sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else 1)
    elif action == "faultall":
        for point in ("during-snapshot", "before-send", "after-send", "after-durable", "after-ack"):
            try:
                cmd_fault(point, 1)
            except SystemExit as exit:
                if exit.code != 0:
                    sys.exit(exit.code)
    elif action == "fuzz":
        ops = int(sys.argv[2]) if len(sys.argv) > 2 else 40
        seed = int(sys.argv[3]) if len(sys.argv) > 3 else None
        cmd_fuzz(ops, seed)
    else:
        print("unknown action; use setup, smoke, fault, faultall, fuzz")
        sys.exit(2)


if __name__ == "__main__":
    main()