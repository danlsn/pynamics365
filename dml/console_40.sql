use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_OpportunitiesWithoutSourceCampaign
go

create view [itknocks_migration].vw_OpportunitiesWithoutSourceCampaign as
(
select o.topic, o.source_campaign
from [itknocks_migration].opportunities as o
where o.source_campaign not in (select name
                            from [itknocks_migration].campaigns)
  and o.source_campaign is not null    )

go

create view [itknocks_migration].vw_MissingContacts as (
select *
from (with missing_account_contacts as (select primary_contact
                                        from [itknocks_migration].vw_AccountsWithoutPrimaryContact),
           missing_lead_contacts as (select parent_contact_for_lead
                                     from [itknocks_migration].vw_LeadsWithoutParentContact),
           missing_opportunity_contacts as (select contact
                                            from [itknocks_migration].vw_OpportunitiesWithoutContact),
           all_missing_contacts as (select primary_contact as contact
                                    from missing_account_contacts
                                    union all
                                    select *
                                    from missing_lead_contacts
                                    union all
                                    select *
                                    from missing_opportunity_contacts)
      select distinct contact
      from all_missing_contacts) alias);


create view [itknocks_migration].vw_MissingAccounts as (
select distinct contact
from (
    select primary_contact as contact
    from [itknocks_migration].vw_AccountsWithoutPrimaryContact
    union all
    select parent_contact_for_lead as contact
    from [itknocks_migration].vw_LeadsWithoutParentContact
    union all
    select contact
    from [itknocks_migration].vw_OpportunitiesWithoutContact
     ) alias
where contact is not null);


create view [itknocks_migration].vw_MissingLeads as (
select distinct lead
from (
    select originating_lead as lead
    from [itknocks_migration].vw_AccountsWithoutOriginatingLead
    union all
    select originating_lead as lead
    from [itknocks_migration].vw_ContactsWithoutOriginatingLead
    union all
    select originating_lead as lead
    from [itknocks_migration].vw_OpportunitiesWithoutOriginatingLead
     ) alias
     )


