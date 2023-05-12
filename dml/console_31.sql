use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_ContactsWithoutAccount
go

create view [itknocks_migration].vw_ContactsWithoutAccount as
(
select c.first_name, c.middle_name, c.last_name, c.company_name
from [itknocks_migration].contacts as c
where c.company_name not in (select account_name
                             from [itknocks_migration].accounts)
                             and c.company_name is not null)
go

