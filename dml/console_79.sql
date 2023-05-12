select cdi_type_FormattedValue, count(*) as count
from [staging].cdi_emailevents
group by cdi_type_FormattedValue
