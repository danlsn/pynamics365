use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_LeadsWithoutParentContact
go

create view [itknocks_migration].vw_LeadsWithoutParentContact as
(
select l.first_name, l.last_name, l.parent_contact_for_lead
from [itknocks_migration].leads as l
where l.parent_contact_for_lead not in (select full_name
                                    from [itknocks_migration].contacts as c
                                    where c.originating_lead = case
                                                                   when l.first_name is null then l.last_name
                                                                   when l.last_name is null then l.first_name
                                                                   else l.first_name + ' ' + l.last_name
                                        end)
and l.parent_contact_for_lead is not null
    )
go

