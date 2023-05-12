select l.*
from [migration].lead as l
inner join [migration].contact as c
on l.Email = c.Email and
   left(l.[First Name], 1) = left(c.[First Name], 1) and
    left(l.[Last Name], 1) = left(c.[Last Name], 1)


select l.[Email], count(*)
from [migration].lead as l
where l.Email is not null
group by l.[Email]
having count(*) > 1
order by count(*) desc


select l.[First Name], l.[Last Name], l.[Email], count(*)
from [migration].lead as l
where l.[Email] is not null
group by l.[First Name], l.[Last Name], l.[Email]
having count(*) > 1
order by count(*) desc
