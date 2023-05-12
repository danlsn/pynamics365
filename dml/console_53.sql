select sla.id as sl_account_id, crma.accountid as crm_account_id
from [cleaned].salesloft_accounts as sla
inner join [cleaned].crm_account as crma
on  lower(sla.name) = lower(crma.name) or
    lower(sla.domain) = lower(crma.websiteurl)

