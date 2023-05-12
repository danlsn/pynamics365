use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_AccountsWithoutPrimaryContact
go

create view [itknocks_migration].vw_AccountsWithoutPrimaryContact as
(
select a.account_name, a.primary_contact
from [itknocks_migration].accounts as a
where a.primary_contact not in (select full_name
                            from [itknocks_migration].contacts as c
                            where c.company_name = a.account_name)
and a.primary_contact is not null
    )
go

