select [Originating Lead]
from [migration].contact as a
where [Originating Lead] like '%,%'


select [Originating Lead]
from [migration].opportunity as a
where [Originating Lead] like '%,%'


select [Last Name]
from [migration].lead as l
where [Last Name] like '%,%'

select [First Name], [Last Name], [Email]
from [migration].lead as l
where [First Name] like '%@%' or
        [Last Name] like '%@%'
