use MIPCRM_Sandbox
go

drop view [itknocks_migration].vw_LeadsWithoutSourceCampaign
go

create view [itknocks_migration].vw_LeadsWithoutSourceCampaign as
(
select l.first_name, l.last_name, l.source_campaign
from [itknocks_migration].leads as l
where l.source_campaign not in (select name
                            from [itknocks_migration].campaigns)
    )
go

