select [Account Name], count(*) as Count
from [deduped].account
group by [Account Name]
having count(*) > 1
