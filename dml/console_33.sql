use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_ContactsWithoutOriginatingLead
go

create view [itknocks_migration].vw_ContactsWithoutOriginatingLead as
(
select c.first_name, c.middle_name, c.last_name, c.full_name, c.originating_lead
from [itknocks_migration].contacts as c
where c.originating_lead not in (select case
                                            when l.first_name is null then l.last_name
                                            when l.last_name is null then l.first_name
                                            else l.first_name + ' ' + l.last_name end as full_name
                                 from [itknocks_migration].leads as l
                                 where l.parent_contact_for_lead = c.full_name)
and c.originating_lead is not null
    )
go

