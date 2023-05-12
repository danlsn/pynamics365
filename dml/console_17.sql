select
    *
from
    [itknocks_migration].opportunities
where
    status_reason = 'Alteryx Acquisition';

select distinct status
from [itknocks_migration].opportunities
