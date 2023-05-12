select distinct error_value
from [migration].[_account_failed_validation_records]
where error_value is not null and
      error_field = 'Industry'
