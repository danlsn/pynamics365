with leads_w_parent_contact as (select *, concat(first_name, ' ', last_name) as full_name
                                from [itknocks_migration].leads as l
                                where l.parent_contact_for_lead is not null),
     contacts_w_originating_lead as (select *
                                     from [itknocks_migration].contacts as c
                                     where c.originating_lead is not null)
select *
from contacts_w_originating_lead as cwol
         full outer join leads_w_parent_contact as lwp on (lwp.full_name = cwol.originating_lead
    or cwol.full_name = lwp.parent_contact_for_lead) and cwol.email = lwp.email;

create or alter view [itknocks_migration].vwLeadContactMatch as
with leads_w_parent_contact as (select first_name,
                                       last_name,
                                       email,
                                       concat(first_name, ' ', last_name) as full_name,
                                       parent_contact_for_lead
                                from [itknocks_migration].leads as l
                                where l.parent_contact_for_lead is not null),
     contacts_w_originating_lead as (select first_name, middle_name, last_name, email, full_name, originating_lead
                                     from [itknocks_migration].contacts as c
                                     where c.originating_lead is not null)

select case
           when originating_lead is null and parent_contact_for_lead is not null then 'Missing Contact'
           when originating_lead is not null and parent_contact_for_lead is null then 'Missing Lead'
           else null end as issue,
       cwol.full_name    as contact_full_name,
       lwp.full_name     as lead_full_name,
       originating_lead,
       parent_contact_for_lead,

       cwol.first_name   as contact_first_name,
       cwol.last_name    as contact_last_name,
       lwp.first_name    as lead_first_name,
       middle_name       as lead_middle_name,
       lwp.last_name     as lead_last_name,
       cwol.email        as contact_email,
       lwp.email         as lead_email
from contacts_w_originating_lead as cwol
         full outer join leads_w_parent_contact as lwp on (lwp.full_name = cwol.originating_lead
    or cwol.full_name = lwp.parent_contact_for_lead) or cwol.email = lwp.email;
