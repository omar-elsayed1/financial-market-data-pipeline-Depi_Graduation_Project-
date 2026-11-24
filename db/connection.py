"""Database connection helpers (plain psycopg2, works on any SQLAlchemy version)."""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as PgConnection

load_dotenv()


def get_connection() -> PgConnection:
    """Open a new connection using the DB_* environment variables.

    Locally DB_HOST is "localhost". Inside Docker (Airflow) docker-compose
    sets DB_HOST to "postgres" automatically.

    Returns:
        An open psycopg2 connection. The caller must close it.
    """
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )


@contextmanager
def transaction() -> Iterator[PgConnection]:
    """Run a block of SQL as ONE all-or-nothing transaction.

    On success the changes are committed. If anything raises an error, all
    changes made inside the block are rolled back, so the database is never
    left half-written. The connection is always closed at the end.

    Yields:
        An open connection. Use ``with conn.cursor() as cur:`` inside.
    """
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
