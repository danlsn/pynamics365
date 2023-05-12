select error_type, error_field, option_set_values, error_value, count(*) as count
from [migration]._opportunity_failed_validation_records
where error_type = 'option_set'
group by error_type, error_field, option_set_values, error_value
order by option_set_values
