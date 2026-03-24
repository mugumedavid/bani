# Architecture Decision Log

Entries are logged whenever a deviation from the architecture spec is made,
a library choice is evaluated, a requirement is simplified, or any decision
is made that a future contributor would want to understand.

---

<!-- Use the template below for new entries.

## DECISION-NNN: <Short title>

- **Date:** YYYY-MM-DD
- **Status:** accepted | superseded | rejected
- **Context:** Why this decision was needed.
- **Decision:** What was decided.
- **Alternatives considered:** What else was evaluated and why it was rejected.
- **Consequences:** What changes as a result.

-->

## DECISION-001: PyMySQL as MySQL driver

- **Date:** 2026-03-24
- **Status:** accepted
- **Context:** The MySQL connector needs a Python driver that supports MySQL 5.x through 9.0, server-side cursors for large tables, and works as a pure-Python package with no C extension requirement.
- **Decision:** Use PyMySQL as the MySQL driver.
- **Alternatives considered:** mysql-connector-python (Oracle's official driver) was considered. While it has official support, it has a more complex installation due to C extensions, licensing concerns (GPLv2 with FOSS exception), and historically less reliable server-side cursor support. PyMySQL is pure Python, MIT-licensed, broadly compatible with MySQL 5.5+ through 9.0, and has well-tested SSCursor support.
- **Consequences:** PyMySQL is added as a dependency. The connector uses `pymysql.cursors.SSCursor` for memory-efficient streaming of large tables and `executemany` for efficient batch writes.
