with successful_leads as (select [legacyid]
                          from [import_report].vw_SuccessfulImports
                          where [targetentityname] = 'lead')
select *
from [deduped].lead_deduped as l
where l.[(Do Not Modify) Lead] not in (select [legacyid]
                                       from successful_leads)
drop view [import_report].vw_UnsuccessfulImports
go
create view [import_report].vw_UnsuccessfulImports as
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
     (select first_value(l.[_importdataid_value]) over (partition
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
     on l.[_importdataid_value] = latest_log.latest_log
where r.[legacyid] is null
  and l.[logphasecode_FormattedValue] = 'Import Create'
  and latest_log.latest_log is not null
