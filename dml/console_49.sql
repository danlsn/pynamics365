select distinct [Status Reason]
from [migration].lead
where [Status Reason] is not null


select distinct [Status Reason]
from [migration].opportunity
where [Status Reason] is not null


select distinct [Potential Customer]
from [migration].opportunity

select distinct [Forecast category]
from [migration].opportunity

select distinct [Pipeline Phase]
from [migration].opportunity


select count(*)
from [staging].account
where address1_stateorprovince is null or industrycode is null or name is null


select count(*)
from [staging].contact
where telephone1 is null and mobilephone is not null

select count(*)
from [staging].contact
where telephone1 is null


select count(*)
from [staging].contact
where (telephone1 is not null or mobilephone is not null) and
      firstname is not null and lastname is not null and
      jobtitle is not null and emailaddress1 is not null
