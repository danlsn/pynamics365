use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_OpportunitiesWithoutAccount
go

create view [itknocks_migration].vw_OpportunitiesWithoutAccount as
(
select o.topic, o.account
from [itknocks_migration].opportunities as o
where o.account not in (select account_name
                         from [itknocks_migration].accounts)
and o.account is not null
    )
go

