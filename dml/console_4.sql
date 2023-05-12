-- Get count of distinct emailaddress from [staging].leads
select emailaddress1,
       count(emailaddress1)
from [staging].leads
group by emailaddress1
having count(emailaddress1) > 1
order by count(emailaddress1) desc;

select count(*) from [staging].leads;

-- Get all email addresses from [staging].leads
-- with an _ after the @ symbol
select emailaddress1,
         count(emailaddress1)
from [staging].leads
where emailaddress1 like '%@%_%'
group by emailaddress1;

/*
  Retrieve all Leads that have _x??w_ in their email address
  where ?? is any two digits and w is any letter
*/
select emailaddress1,
         count(emailaddress1)
from [staging].leads
where emailaddress1 like '%@%_%[0-9][0-9][a-zA-Z]_%'
group by emailaddress1
order by count(emailaddress1) desc;

/*
  Retrieve all leads that match @mip.com.au and @thedataschool.com.au
*/
select emailaddress1,
         count(emailaddress1)
from [staging].leads
where emailaddress1 like '%@mip.com.au%'
   or emailaddress1 like '%@thedataschool.com.au%'
group by emailaddress1
order by count(emailaddress1) desc;

-- Leads with Alteryx or Salesforce in their email address
select emailaddress1,
         count(emailaddress1)
from [staging].leads
where emailaddress1 like '%@%alteryx%'
   or emailaddress1 like '%@%salesforce%'
   or emailaddress1 like '%@%tableau%'
   or emailaddress1 like '%@%microsoft%'
   or emailaddress1 like '%@%google%'
    or emailaddress1 like '%@%amazon%'
   or emailaddress1 like '%@%.aws.%'
    or emailaddress1 like '%@%oracle%'
    or emailaddress1 like '%@%ibm%'
    or emailaddress1 like '%@%qlik%'
    or emailaddress1 like '%@%sas.com'
   or emailaddress1 like '%@%snowflake%'
    or emailaddress1 like '%@%wherescape%'
   or emailaddress1 like '%@%collibra%'
group by emailaddress1;


/*
  Retrieve all distinct email addresses from [staging].leads
  First lowercase the email address
  Replace _x000D_ with nothing
*/
with most_recent as (
    select emailaddress1, _masterid_value, merged,
           max(modifiedon) as most_recent
    from [staging].leads
    group by emailaddress1, _masterid_value, merged
)

select distinct lower(replace(emailaddress1, '_x000D_', '')) as clean_emailaddress1,
                count(emailaddress1)
from most_recent
where _masterid_value is null
    and merged = 0
group by lower(replace(emailaddress1, '_x000D_', ''))
order by clean_emailaddress1;

-- Leads with alias email addresses
select emailaddress1,
         count(emailaddress1)
from [staging].leads
where emailaddress1 like '%+%@%'
group by emailaddress1
order by count(emailaddress1) desc;

-- Leads with throwaway email addresses

with most_recent as (
    select emailaddress1,
           max(modifiedon) as most_recent
    from [staging].leads
    group by emailaddress1
)
select emailaddress1,
         count(emailaddress1)
from most_recent
where emailaddress1 like '%@%mailinator.com'
   or emailaddress1 like '%@%maildrop.cc'
   or emailaddress1 like '%@%guerrilla%'
   or emailaddress1 like '%@%grr.la'
   or emailaddress1 like '%@%yopmail.com'
   or emailaddress1 like '%@%sharklasers.com'
   or emailaddress1 like '%@%10minute%'
group by emailaddress1
order by count(emailaddress1) desc;


-- Create a view of leads ie. max(modifiedon) for leadid
create or alter view [staging].[vw_most_recent_leads] as
select leads.*
from [staging].leads leads
inner join (
    select leadid,
           max(modifiedon) as most_recent
    from [staging].leads
    group by leadid
) most_recent
on leads.leadid = most_recent.leadid
and leads.modifiedon = most_recent.most_recent;



