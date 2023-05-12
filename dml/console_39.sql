use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_OpportunitiesWithoutOriginatingLead
go

create view [itknocks_migration].vw_OpportunitiesWithoutOriginatingLead as
(
select o.topic, o.originating_lead
from [itknocks_migration].opportunities as o
where o.originating_lead not in (select case
                                        when l.first_name is null then l.last_name
                                        when l.last_name is null then l.first_name
                                        else l.first_name + ' ' + l.last_name
                                        end as full_name
                             from [itknocks_migration].leads as l
)
and o.originating_lead is not null
    )
go

