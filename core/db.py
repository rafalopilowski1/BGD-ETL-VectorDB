import logging
from collections.abc import Generator
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from core.config import DATABASE_URL

log = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    """Create a new database connection."""
    conn = psycopg2.connect(DATABASE_URL)
    return conn


@contextmanager
def get_cursor(
    commit: bool = True,
) -> Generator[psycopg2.extras.DictCursor, None, None]:
    """Context manager that yields a cursor and optionally commits on exit.

    Usage:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
    """
    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def check_connection() -> bool:
    """Verify database connectivity."""
    try:
        with get_cursor() as cur:
            cur.execute("SELECT 1")
            return True
    except Exception as e:
        log.error("Database connection failed: %s", e)
        return False
