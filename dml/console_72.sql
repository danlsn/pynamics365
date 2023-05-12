select distinct [Owner], count(*) as [Count]
from [migration].contact
where [Owner] like 'MIP Support'
group by [Owner]
having count(*) > 1

