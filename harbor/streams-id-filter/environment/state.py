"""SQLite outbox: a local transaction, deliberately separate from Kafka."""
import sqlite3
import sys

action, path, number = sys.argv[1:]
with sqlite3.connect(path) as db:
    db.execute("PRAGMA synchronous=FULL")
    db.execute("CREATE TABLE IF NOT EXISTS outbox(id INTEGER PRIMARY KEY, sent INTEGER NOT NULL DEFAULT 0)")
    if action == "init":
        db.executemany("INSERT OR IGNORE INTO outbox(id) VALUES (?)", [(i,) for i in range(int(number))])
    elif action == "pending":
        row = db.execute("SELECT sent FROM outbox WHERE id=?", (int(number),)).fetchone()
        print(int(row is not None and row[0] == 0))
    elif action == "mark":
        db.execute("UPDATE outbox SET sent=1 WHERE id=?", (int(number),))
        print(1)
    else:
        raise ValueError(action)
