moodle-trace-reader
===================

A web interface and REST API to extract knowledge from moodle logs


How to create the database
==========================

To create the database, using postgres:

```
createdb -U postgres -E unicode moodle-trace-reader
```

To restore the database schema from the dump:

```
pg_restore -Fc -f empty_db.dump moodle-trace-reader
```