select mip_notes
from [staging].opportunity as o



select o.description, o.mip_notes
from [staging].opportunity as o
where opportunityid = '51e4eee3-79a6-ed11-aad1-000d3a8560e6'
