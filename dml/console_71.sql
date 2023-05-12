select [Full Name], count(*) as [Count]
from [migration].vw_FullNameLeads
group by [Full Name]
having count(*) > 1
