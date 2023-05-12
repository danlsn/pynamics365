-- Create view vw_AccountsWithoutPrimaryContact as drop view [itknocks_migration].vw_AccountsWithoutPrimaryContact go  create view [itknocks_migration].vw_AccountsWithoutPrimaryContact as ( select
        *
        from [itknocks_migration].accounts as a
where
  a.primary_contact not in (
    select
      full_name
    from
      [itknocks_migration].contacts as c
    where
      c.company_name = a.account_name
  )
  and a.primary_contact is not null
)
go
drop
    view [itknocks_migration].vw_ContactsWithoutAccount
go
create view [itknocks_migration].vw_ContactsWithoutAccount as
(
select *
from [itknocks_migration].contacts as c
where c.company_name not in (select account_name
                             from [itknocks_migration].accounts)
  and c.company_name is not null
    );
drop
    view [itknocks_migration].vw_AccountsWithoutOriginatingLead
go
create view [itknocks_migration].vw_AccountsWithoutOriginatingLead as
(
select *
from [itknocks_migration].accounts as a
where a.originating_lead not in (select case
                                            when l.first_name is null then l.last_name
                                            when l.last_name is null then l.first_name
                                            else l.first_name + ' ' + l.last_name end as full_name
                                 from [itknocks_migration].leads as l
                                 where l.company_name = a.account_name)
    );

drop view [itknocks_migration].vw_ContactsWithoutOriginatingLead
go
create view [itknocks_migration].vw_ContactsWithoutOriginatingLead as
(
select *
from [itknocks_migration].contacts as c
where c.originating_lead not in (select case
                                            when l.first_name is null then l.last_name
                                            when l.last_name is null then l.first_name
                                            else l.first_name + ' ' + l.last_name end as full_name
                                 from [itknocks_migration].leads as l
                                 where l.parent_contact_for_lead = c.full_name)
and c.originating_lead is not null
    );
create view [itknocks_migration].vw_LeadsWithoutParentAccount as
(
select *
from [itknocks_migration].leads as l
where l.company_name not in (select account_name
                             from [itknocks_migration].accounts)
    );
create view [itknocks_migration].vw_ContactsWithoutOriginatingLead as
(
select *
from [itknocks_migration].contacts as c
where c.originating_lead not in (select case
                                            when l.first_name is null then l.last_name
                                            when l.last_name is null then l.first_name
                                            else l.first_name + ' ' + l.last_name end as full_name
                                 from [itknocks_migration].leads as l
                                 where l.parent_contact_for_lead = c.full_name)
    );
create view [itknocks_migration].vw_LeadsWithoutParentContact as
(
select *
from [itknocks_migration].leads as l
where l.parent_contact_for_lead not in (select full_name
                                        from [itknocks_migration].contacts as c
                                        where c.originating_lead = case
                                                                       when l.first_name is null then l.last_name
                                                                       when l.last_name is null then l.first_name
                                                                       else l.first_name + ' ' + l.last_name end)
    );
create view [itknocks_migration].vw_OpportunitiesWithoutAccount as
(
select *
from [itknocks_migration].opportunities as o
where o.account not in (select account_name
                        from [itknocks_migration].accounts)
    );
create view [itknocks_migration].vw_OpportunitiesWithoutContact as
(
select *
from [itknocks_migration].opportunities as o
where o.contact not in (select full_name
                        from [itknocks_migration].contacts as c)
    );
create view [itknocks_migration].vw_OpportunitiesWithoutOriginatingLead as
(
select *
from [itknocks_migration].opportunities as o
where o.originating_lead not in (select case
                                            when l.first_name is null then l.last_name
                                            when l.last_name is null then l.first_name
                                            else l.first_name + ' ' + l.last_name end as full_name
                                 from [itknocks_migration].leads as l)
    );
create view [itknocks_migration].vw_LeadsWithoutSourceCampaign as
(
select *
from [itknocks_migration].leads as l
where l.source_campaign not in (select name
                                from [itknocks_migration].campaigns)
    );
create view [itknocks_migration].vw_OpportunitiesWithoutSourceCampaign as
(
select *
from [itknocks_migration].opportunities as o
where o.source_campaign not in (select name
                                from [itknocks_migration].campaigns)
    );
