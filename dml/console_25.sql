select *
from sys.tables
where schema_id in (
    select schema_id
    from sys.schemas
    where name = 'itknocks_migration'
    )
