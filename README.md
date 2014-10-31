moodle-trace-reader
===================

A small, specialised ETL, written in Node.js to transform moodle logs into traces. Traces can then be accessed and analyzed by dimensions.

Logs are pulled from the mySQL database, transformed, and injected into a postgres database with a new schema.


How to create the database
--------------------------

To create the database, using postgres:

```
createdb -U postgres -E unicode moodle-trace-reader
```

To restore the database schema from the dump:

```
pg_restore -Fc -f empty_db.dump moodle-trace-reader
```
