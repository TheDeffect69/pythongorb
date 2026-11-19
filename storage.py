"""SQLite connections and transactional, versioned schema migrations."""

import sqlite3
from pathlib import Path


MIGRATIONS = Path(__file__).resolve().parent / "migrations"


def connect(db_path, *, timeout=30, check_same_thread=True):
    connection = sqlite3.connect(db_path, timeout=timeout, check_same_thread=check_same_thread)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize(db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    connection = connect(db_path)
    try:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version TEXT PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        for migration in sorted(MIGRATIONS.glob("*.sql")):
            if connection.execute(
                "SELECT 1 FROM schema_migrations WHERE version = ?", (migration.name,)
            ).fetchone():
                continue
            sql = ""
            for line in migration.read_text(encoding="utf-8-sig").splitlines(keepends=True):
                # Execute one complete statement at a time to preserve the transaction.
                for char in line:
                    sql += char
                    if char == ";" and sqlite3.complete_statement(sql):
                        connection.execute(sql)
                        sql = ""
            if sql.strip():
                connection.execute(sql)
            connection.execute("INSERT INTO schema_migrations(version) VALUES (?)", (migration.name,))
        connection.commit()
    finally:
        connection.close()
