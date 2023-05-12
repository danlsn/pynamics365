select distinct [targetentityname], [legacyid]
from [import_report].import_error_report
where [legacyid] is not null


create view [import_report].vw_SuccessfulImports as
    (
        select [targetentityname], [legacyid]
        from [import_report].import_report
        where [haserror] = 0
    )

drop view if exists [import_report].vw_UnsuccessfulImports
go

create view [import_report].vw_UnsuccessfulImports as
    (
        select distinct [targetentityname], [legacyid]
        from [import_report].import_report as ir
        where [legacyid] not in (
            select [legacyid]
            from [import_report].import_report
            where [haserror] = 0
            and [targetentityname] = ir.[targetentityname]
            )
    )

