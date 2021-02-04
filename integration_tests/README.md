This directory contains integration tests for omnata-push.

```
dbt deps
dbt seed
dbt run --full-refresh --vars 'drop-omnata-task-tables: true'
dbt test
```