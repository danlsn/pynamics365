select distinct ll.originating_lead as left_full_name,
                lr.first_name,
                lr.last_name,
                lr.parent_contact_for_lead,
                lr.parent_account_for_lead
from [itknocks_migration].vw_AccountsWithoutOriginatingLead as ll
         inner join
     [itknocks_migration].leads as lr
     on lower(case
                  when lr.first_name is null then lr.last_name
                  when lr.last_name is null then lr.first_name
                  else lr.first_name + ' ' + lr.last_name
         end) = lower(ll.originating_lead)
where lr.first_name is not null
  and lr.last_name is not null;
drop view [itknocks_migration].vw_AccountsWithoutOriginatingLead
go
create view [itknocks_migration].vw_AccountsWithoutOriginatingLead as
(
select *
from [itknocks_migration].accounts as a
where a.originating_lead in (select case
                                        when l.first_name is null then l.last_name
                                        when l.last_name is null then l.first_name
                                        else l.first_name + ' ' + l.last_name
                                        end as full_name
                             from [itknocks_migration].leads as l
                             where l.company_name = a.account_name)
  and a.originating_lead is not null )
GO
use MIPCRM_Sandbox
go
drop view [itknocks_migration].vw_LeadsWithoutParentAccount
go
create view [itknocks_migration].vw_LeadsWithoutParentAccount as
(
select *
from [itknocks_migration].leads as l
where l.parent_account_for_lead not in (select account_name
                                        from [itknocks_migration].accounts as a)
  and l.parent_account_for_lead is not null )
go
with missing_accounts_from_leads as (select distinct parent_account_for_lead,
                                                     upper(replace(parent_account_for_lead,
                                                                   ' ',
                                                                   '')) as parent_account_for_lead_upper
                                     from [itknocks_migration].vw_LeadsWithoutParentAccount
                                     where parent_account_for_lead is not null),
     distinct_accounts as (select distinct account_name,
                                           upper(replace(account_name,
                                                         ' ',
                                                         '')) as account_name_upper
                           from [itknocks_migration].accounts
                           where account_name is not null)
select *
from missing_accounts_from_leads as m
         inner join
     distinct_accounts as d
     on m.parent_account_for_lead = d.account_name
where m.parent_account_for_lead is not null
  and d.account_name is not null
go
select *
from [itknocks_migration].accounts
where account_name in (
    'Western Sydney University'
    )
