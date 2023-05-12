use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_AccountsWithoutOriginatingLead
go

create view [itknocks_migration].vw_AccountsWithoutOriginatingLead as
(
select a.account_name, a.originating_lead
from [itknocks_migration].accounts as a
where a.originating_lead not in (select case
                                            when l.first_name is null then l.last_name
                                            when l.last_name is null then l.first_name
                                            else l.first_name + ' ' + l.last_name
                                            end as full_name
                                 from [itknocks_migration].leads as l
                                 where l.company_name = a.account_name)
  and a.originating_lead is not null)
go

