drop view [deduped].vw_AccountNames
go

create view [deduped].vw_AccountNames as
(
select distinct [Account Name]
from [deduped].account as a
where [Account Name] is not null
    )
go

drop view [deduped].vw_ParentAccountNames
go

create view [deduped].vw_ParentAccountNames as
(
select distinct [Parent Account]
from [deduped].account as a
where [Parent Account] is not null
    )
go


drop view [deduped].vw_ContactFullNames
go

create view [deduped].vw_ContactFullNames as
(
select case
           when [First Name] is null and [Middle Name] is null and [Last Name] is null then null
           when [First Name] is null and [Middle Name] is null and [Last Name] is not null then [Last Name]
           when [First Name] is null and [Middle Name] is not null and [Last Name] is not null
               then [Middle Name] + ' ' + [Last Name]
           when [First Name] is not null and [Middle Name] is not null and [Last Name] is null
               then [First Name] + ' ' + [Middle Name]
           when [First Name] is not null and [Middle Name] is null and [Last Name] is not null
               then [First Name] + ' ' + [Last Name]
           else [First Name] + ' ' + [Middle Name] + ' ' + [Last Name]
           end as [Contact Full Name]
from [deduped].contact
    )
go

drop view [deduped].vw_LeadFullNames
go

create view [deduped].vw_LeadFullNames as
(
select case
           when [First Name] is null and [Last Name] is not null then [Last Name]
           when [First Name] is not null and [Last Name] is null then [First Name]
           else [First Name] + ' ' + [Last Name]
           end as [Lead Full Name]
from [deduped].lead as l
    )
go


drop view [deduped].vw_AccountPrimaryContacts
go

create view [deduped].vw_AccountPrimaryContacts as
(
select [Primary Contact]
from [deduped].account as a
where [Primary Contact] is not null
    )
go

drop view [deduped].vw_AccountParentAccounts
go

create view [deduped].vw_AccountParentAccounts as
(
select [Parent Account]
from [deduped].account as a
where [Parent Account] is not null
    )
go

drop view [deduped].vw_ContactOriginatingLeads
go

create view [deduped].vw_ContactOriginatingLeads as
(
select [Originating Lead]
from [deduped].contact as c
where [Originating Lead] is not null
    )
go

drop view [deduped].vw_ContactParentAccounts
go

create view [deduped].vw_ContactParentAccounts as
(
select distinct [Company Name]
from [deduped].contact as c
where [Company Name] is not null
    )
