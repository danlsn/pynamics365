select count(*)
from [migration].lead as l
where [First Name] is null or
      [Last Name] is null


select l.[statuscode_FormattedValue], count(*)
from [staging].lead as l
where (l.firstname is null or
       l.lastname is null)
group by l.[statuscode_FormattedValue]

select l.[First Name], l.[Last Name], l.Email
from [migration].lead as l
where l.[First Name] is null or
        l.[Last Name] is null
