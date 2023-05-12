select subject, count(*)
from [staging].lead
where subject like 'MIP%'
group by subject


drop view [import_report].vw_ContactErrors
go

create view [import_report].vw_ContactErrors as
    (
select distinct ier.additionalinfo, c.*
from [migration].contact as c
inner join ( select distinct [legacyid]
from [import_report].vw_UnsuccessfulImports as ui
where ui.targetentityname = 'contact') as ui
on c.[(Do Not Modify) Contact] = ui.[legacyid]
left join [import_report].import_error_report as ier
on c.[(Do Not Modify) Contact] = ier.[legacyid]
    where ier.[legacyid] is not null and
            ier.logphasecode_FormattedValue = 'Import Create'
)

drop view [import_report].vw_AccountErrors
go


create view [import_report].vw_AccountErrors as
(
select distinct ier.additionalinfo, a.*
from [migration].account as a
         inner join ( select distinct [legacyid]
                      from [import_report].vw_UnsuccessfulImports as ui
                      where ui.targetentityname = 'account') as ui
                    on a.[(Do Not Modify) Account] = ui.[legacyid]
         left join [import_report].import_error_report as ier
                   on a.[(Do Not Modify) Account] = ier.[legacyid]
where ier.[legacyid] is not null and
        ier.logphasecode_FormattedValue = 'Import Create'
    )

drop view [import_report].vw_LeadErrors
go

create view [import_report].vw_LeadErrors as
(
select distinct ier.additionalinfo, l.*
from [migration].lead as l
         inner join ( select distinct [legacyid]
                      from [import_report].vw_UnsuccessfulImports as ui
                      where ui.targetentityname = 'lead') as ui
                    on l.[(Do Not Modify) Lead] = ui.[legacyid]
         left join [import_report].import_error_report as ier
                   on l.[(Do Not Modify) Lead] = ier.[legacyid]
where ier.[legacyid] is not null and
      ier.logphasecode_FormattedValue = 'Import Create'
    )

drop view [import_report].vw_SuccessfulImports
go

create view [import_report].vw_SuccessfulImports as
(
select [targetentityname], [legacyid]
from [import_report].import_report
where errortype != 0
    )
GO


use MIPCRM_Sandbox
go

alter view [import_report].vw_UnsuccessfulImports as
    (
        select distinct l.targetentityname,
                        l._importfileid_value_FormattedValue,
                        l.linenumber,
                        l.legacyid,
                        l._createdby_value,
                        l._createdby_value_FormattedValue,
                        l._createdby_value_lookuplogicalname,
                        l._createdonbehalfby_value,
                        l._importdataid_value,
                        l._importdataid_value_FormattedValue,
                        l._importdataid_value_associatednavigationproperty,
                        l._importdataid_value_lookuplogicalname,
                        l._importfileid_value,
                        l._importfileid_value_associatednavigationproperty,
                        l._importfileid_value_lookuplogicalname,
                        l._modifiedby_value,
                        l._modifiedby_value_FormattedValue,
                        l._modifiedby_value_lookuplogicalname,
                        l._modifiedonbehalfby_value,
                        l._ownerid_value,
                        l._ownerid_value_FormattedValue,
                        l._ownerid_value_associatednavigationproperty,
                        l._ownerid_value_lookuplogicalname,
                        l._owningbusinessunit_value,
                        l._owningbusinessunit_value_FormattedValue,
                        l._owningbusinessunit_value_associatednavigationproperty,
                        l._owningbusinessunit_value_lookuplogicalname,
                        l._owningteam_value,
                        l._owninguser_value,
                        l._owninguser_value_lookuplogicalname,
                        l.additionalinfo,
                        l.columnvalue,
                        l.createdon,
                        l.createdon_FormattedValue,
                        l.errordescription,
                        l.errornumber,
                        l.errornumber_FormattedValue,
                        l.headercolumn,
                        l.importlogid,
                        l.linenumber_FormattedValue,
                        l.logphasecode,
                        l.logphasecode_FormattedValue,
                        l.modifiedon,
                        l.modifiedon_FormattedValue,
                        l.sequencenumber,
                        l.sequencenumber_FormattedValue,
                        l.statecode,
                        l.statecode_FormattedValue,
                        l.statuscode,
                        l.statuscode_FormattedValue,
                        latest_log.latest_log
        from [import_report].import_error_report as l
                 left outer join
             [import_report].vw_SuccessfulImports as r
             on l.[targetentityname] = r.[targetentityname]
                 and l.[legacyid] = r.[legacyid]
                 inner join
             (select first_value(l.importlogid) over (partition
                 by
                 l.legacyid
                 order by
                     cast(l.createdon as nvarchar(100)) desc) as latest_log
              from [import_report].import_error_report as l
                       left outer join
                   [import_report].vw_SuccessfulImports as r
                   on l.[targetentityname] = r.[targetentityname]
                       and l.[legacyid] = r.[legacyid]
              where r.[legacyid] is null
                and l.[logphasecode_FormattedValue] = 'Import Create') as latest_log
             on l.importlogid = latest_log.latest_log
        where r.[legacyid] is null
          and l.[logphasecode_FormattedValue] = 'Import Create'
          and latest_log.latest_log is not null )
go

use MIPCRM_Sandbox
go

alter view [import_report].vw_ContactErrors as
    (
        select distinct ier.additionalinfo, c.*
        from [migration].contact as c
                 inner join ( select distinct [legacyid]
                              from [import_report].vw_UnsuccessfulImports as ui
                              where ui.targetentityname = 'contact') as ui
                            on c.[(Do Not Modify) Contact] = ui.[legacyid]
                 left join [import_report].import_error_report as ier
                           on c.[(Do Not Modify) Contact] = ier.[legacyid]
        where ier.[legacyid] is not null and
                ier.logphasecode_FormattedValue = 'Import Create'
    )
go

