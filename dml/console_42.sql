create view [itknocks_migration].vw_MissingAccounts as (
select distinct account
from (
    select company_name as account
    from [itknocks_migration].vw_ContactsWithoutAccount
    union all
    select parent_account_for_lead as account
    from [itknocks_migration].vw_LeadsWithoutParentAccount
    union all
    select account
    from [itknocks_migration].vw_OpportunitiesWithoutAccount
     ) as t
where account is not null
)
