select count(*)
from [staging].leads as l
where l.firstname is null
   or l.emailaddress1 is null
   or l.lastname is null
   or l.telephone1 is null
   or l.jobtitle is null
   or l.companyname is null
go

-- Count of leads with telephone numbers that don't start with +
select count(*)
from [staging].leads as l
where l.telephone1 not like '+%'
go



