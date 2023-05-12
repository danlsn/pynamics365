create view [migration].vw_AccountsWithMissingPrimaryContact as
    (
    select *
    from [migration].account as a
    where a.[Primary Contact] is not null and
          a.[Primary Contact] not in (
        select case
            when c.[First Name] is null and
                 c.[Middle Name] is null and
                 c.[Last Name] is not null
            then c.[Last Name]
            when c.[First Name] is null and
                 c.[Middle Name] is not null and
                 c.[Last Name] is not null
            then c.[Middle Name] + ' ' + c.[Last Name]
            when c.[First Name] is not null and
                 c.[Middle Name] is null and
                 c.[Last Name] is not null
            then c.[First Name] + ' ' + c.[Last Name]
            when c.[First Name] is not null and
                 c.[Middle Name] is not null and
                 c.[Last Name] is not null
            then c.[First Name] + ' ' + c.[Middle Name] + ' ' + c.[Last Name]
            else null end as [Full Name]
        from [migration].contact as c
        )
    )
