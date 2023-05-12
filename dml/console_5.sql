use MIPCRM_Sandbox
go

create view [staging].[vw_most_recent_accounts] as
    select acc.*
    from [staging].accounts acc
    inner join (
        select
            accountid,
            max(modifiedon) as modifiedon
        from [staging].accounts
        group by accountid
    ) acc2
    on acc.accountid = acc2.accountid
    and acc.modifiedon = acc2.modifiedon
go

