select distinct [Email], count(*) as [Count], max([(Do Not Modify) Modified On]) as max_modified_dated
from [migration].lead as l
where [Email] is not null
group by [Email]
order by [Count] desc


select distinct [(Do Not Modify) Lead], count(*) as [Count]
from [migration].[_lead_failed_validation_records] as l
group by [(Do Not Modify) Lead]
order by [Count] desc


-- Get Leads that are not contacts
select distinct [First Name], [Last Name], Email
from [migration].lead as l
where l.[Email] not in (
    select distinct [Email]
    from [migration].contact as c
    where c.[Email] is not null
    )


select [First Name], [Middle Name], [Last Name], [Email], [Company Name], count(*) as [Count]
from [migration].contact as c
where [Email] is not null
group by [First Name], [Middle Name], [Last Name], [Company Name], [Email]
having count(*) > 1
order by [Count] desc


select CONCAT([First Name], IIF([Last Name] is not null, ' ', null), [Last Name]) as [Full Name], [First Name], null as [Middle Name], [Last Name], [Email], [Company Name], count(*) as [Count]
from [migration].lead as l
where [Email] is not null and
      [Company Name] in (
        select distinct [Account Name]
        from [migration].account as a
        where a.[Account Name] is not null
        )
group by [First Name], [Last Name], [Company Name], [Email]
having count(*) > 1
order by [Count] desc

