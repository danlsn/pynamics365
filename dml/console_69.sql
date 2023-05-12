select [(Do Not Modify) Opportunity], count(*) as [Count]
from [migration].[_opportunity_failed_validation_records] as a
group by [(Do Not Modify) Opportunity]
