select sl.*, acc.name as account_name,
from [salesloft].people as sl
left join [salesloft].accounts as acc
on sl.[account.id] = acc.id
where sl.crm_id is null
go


select 'Not in CRM', count(*) as count
from [salesloft].accounts as sla
where crm_id is null
union all
select 'In CRM', count(*) as count
from [salesloft].accounts as sla
where crm_id is not null


select sla.*, ma.*
from [salesloft].accounts as sla
inner join [migration].account as ma
on lower(sla.name) = lower(ma.[Account Name])
where sla.crm_id is null


