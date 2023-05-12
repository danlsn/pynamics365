drop view [migration].vw_MissingContactsFromAccounts
go


create view [migration].vw_MissingContactsFromAccounts as
(
select a."Account Name", a."Primary Contact"
from [migration].account as a
where a."Primary Contact" not in (select case
                                             when c."Middle Name" is null then
                                                case
                                                when c."First Name" is null then c."Last Name"
                                                when c."Last Name" is null then c."First Name"
                                                else c."First Name" + ' ' + c."Last Name" end
                                                else c."First Name" + ' ' + c."Middle Name" + ' ' + c."Last Name" end as "Full Name"
                                  from [migration].contact as c
                                  where c.[Company Name] = a.[Account Name])
  and a.[Primary Contact] is not null
    )
GO

use MIPCRM_Sandbox
go

create view [migration].vw_MissingParentAccountsFromContacts as
(
select c."First Name", c."Middle Name", c."Last Name", c."Company Name"
from [migration].contact as c
where c."Company Name" not in (select a."Account Name"
                               from [migration].account as a
                               where a."Account Name" = c."Company Name")
  and c."Company Name" is not null
    )

GO

create view [migration].vw_MissingOriginatingLeadsFromAccounts as
(
select a."Account Name", a."Originating Lead"
from [migration].account as a
where a."Originating Lead" not in (select case
                                              when l."First Name" is null then l."Last Name"
                                              when l."Last Name" is null then l."First Name"
                                              else l."First Name" + ' ' + l."Last Name" end as "Full Name"
                                   from [migration].lead as l
                                   where case
                                             when l."First Name" is null then l."Last Name"
                                             when l."Last Name" is null then l."First Name"
                                             else l."First Name" + ' ' + l."Last Name" end = a."Originating Lead")
  and a."Originating Lead" is not null
    )

