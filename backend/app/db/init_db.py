from app.db.migrations import upgrade_database


def init_db() -> None:
    """Apply versioned migrations before the application accepts requests."""

    upgrade_database()
