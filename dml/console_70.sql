select [Account Name], count(*) as count
from [migration].account
group by [Account Name]
having count(*) > 1


select [Account Name], count(*) as count
from [migration].account
group by [Account Name]
having count(*) > 1
