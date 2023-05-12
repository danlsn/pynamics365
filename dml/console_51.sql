select *
from [migration].contact as crm
where exists (
    select *
    from [salesloft].people as sl
    where sl.first_name = crm.[First Name] and
            sl.last_name = crm.[Last Name] and
            sl.email_address = crm.[Email]
          )
go


select sl.id,
       sl.first_name,
       sl.last_name,
       sl.email_address,
       sl.phone,
       sl.home_phone,
       sl.mobile_phone,
       sl.crm_id,
       sl.crm_object_type,
       crm.[First Name],
       crm.[Last Name],
       crm.Email,
       crm.[Business Phone],
       crm.[Home Phone],
       crm.[Mobile Phone]
from [salesloft].people as sl
         inner join [migration].contact as crm
                    on sl.email_address = crm.[Email] and
                       sl.crm_object_type = 'contact'
go

select sl.id,
       sl.first_name,
       sl.last_name,
       sl.email_address,
       sl.phone,
       sl.home_phone,
       sl.mobile_phone,
       sl.crm_id,
       sl.crm_object_type,
       crm.[First Name],
       crm.[Last Name],
       crm.[Email],
       crm.[Business Phone],
       crm.[Home Phone],
       crm.[Mobile Phone]
from [salesloft].people as sl
inner join [migration].lead as crm
on lower(sl.first_name) = lower(crm.[First Name]) and
    lower(sl.last_name) = lower(crm.[Last Name]) and
    sl.email_address = crm.[Email] and
   sl.crm_object_type = 'lead'
go


select crm.*,
       sl.*
from [staging].contact as crm
inner join [salesloft].people as sl
on (lower(sl.first_name) = lower(crm.firstname) and
    lower(sl.last_name) = lower(crm.lastname)) or
    lower(sl.email_address) = lower(crm.emailaddress1) and
   sl.crm_object_type = 'contact'
where crm_id is null


