select distinct mip_optype_FormattedValue
from [staging].opportunity
where mip_optype_FormattedValue is not null
order by mip_optype_FormattedValue
