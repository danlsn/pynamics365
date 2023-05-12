-- Get all opportunities by year (modifiedon), return count and other fields
-- Modifiedon as year
select count(*) as count, year(modifiedon) as year, month(modifiedon) as month, mip_rating_FormattedValue as rating
from [staging].opportunities
where mip_rating_FormattedValue = 'Commit'
group by year(modifiedon), month(modifiedon), mip_rating_FormattedValue
order by year(modifiedon) desc, month(modifiedon) desc


