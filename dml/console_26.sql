select *
from [model].EntityAttributes
where LogicalName like '%modifiedby%'
and EntityLogicalName = 'account'
