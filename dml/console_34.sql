use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_LeadsWithoutParentAccount
go

create view [itknocks_migration].vw_LeadsWithoutParentAccount as
(
select l.first_name, l.last_name, l.parent_account_for_lead
from [itknocks_migration].leads as l
where l.parent_account_for_lead not in (select account_name
                                    from [itknocks_migration].accounts as a
)
  and l.parent_account_for_lead is not null
    )
go

