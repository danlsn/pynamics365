create view [migration].vw_ContactsWithMissingAccounts as
    (
    select *
    from [migration].contact as c
    where c.[Company Name] is not null and
          c.[Company Name] not in (
              select a.[Account Name]
              from [migration].account as a
            )
    )
go

drop view if exists [migration].vw_AccountsWithMissingOriginatingLeads
go

create view [migration].vw_AccountsWithMissingOriginatingLeads as
    (
    select *
    from [migration].account as a
    where a.[Originating Lead] is not null and
          a.[Originating Lead] not in (select l.[Full Name]
                                       from [migration].vw_FullNameLeads as l)
    )


drop view if exists [migration].vw_FullNameLeads
go

create view [migration].vw_FullNameLeads as
    (
    select case when l.[First Name] is not null and l.[Last Name] is not null
                then l.[First Name] + ' ' + l.[Last Name]
                when l.[First Name] is not null and l.[Last Name] is null
                then l.[First Name]
                when l.[First Name] is null and l.[Last Name] is not null
                then l.[Last Name]
                else null
                end as [Full Name],
            l.[First Name] as [First Name],
            l.[Last Name] as [Last Name],
            l.[Company Name] as [Company Name],
            l.[Email] as [Email],
            l.[Business Phone] as [Business Phone],
            l.[Mobile Phone] as [Mobile Phone]
    from [migration].lead as l
    where l.[First Name] is not null or l.[Last Name] is not null
    )


drop view if exists [migration].vw_FullNameContacts
go

create view [migration].vw_FullNameContacts as
    (
    select case when c.[First Name] is not null and c.[Middle Name] is not null and c.[Last Name] is not null
                then c.[First Name] + ' ' + c.[Middle Name] + ' ' + c.[Last Name]
                when c.[First Name] is not null and c.[Middle Name] is not null and c.[Last Name] is null
                then c.[First Name] + ' ' + c.[Middle Name]
                when c.[First Name] is not null and c.[Middle Name] is null and c.[Last Name] is not null
                then c.[First Name] + ' ' + c.[Last Name]
                when c.[First Name] is not null and c.[Middle Name] is null and c.[Last Name] is null
                then c.[First Name]
                when c.[First Name] is null and c.[Middle Name] is not null and c.[Last Name] is not null
                then c.[Middle Name] + ' ' + c.[Last Name]
                when c.[First Name] is null and c.[Middle Name] is not null and c.[Last Name] is null
                then c.[Middle Name]
                when c.[First Name] is null and c.[Middle Name] is null and c.[Last Name] is not null
                then c.[Last Name]
                else null
                end as [Full Name],
            c.[First Name] as [First Name],
            c.[Middle Name] as [Middle Name],
            c.[Last Name] as [Last Name],
            c.[Company Name] as [Company Name],
            c.[Originating Lead] as [Originating Lead],
            c.[Email] as [Email],
            c.[Business Phone] as [Business Phone],
            c.[Mobile Phone] as [Mobile Phone]
    from [migration].contact as c
    where c.[First Name] is not null or c.[Middle Name] is not null or c.[Last Name] is not null
    )
go


drop view if exists [migration].vw_LeadsWithoutCompanyName

create view [migration].vw_LeadsWithoutCompanyName as
    (
    select *
    from [migration].lead as l
    where l.[Company Name] is not null and
          l.[Company Name] not in (
              select a.[Account Name]
              from [migration].account as a
            )
    )
