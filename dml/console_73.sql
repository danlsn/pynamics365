SELECT [Topic], count(*) as [Count]
from [migration].opportunity as o
group by [Topic]
having count(*) > 1



