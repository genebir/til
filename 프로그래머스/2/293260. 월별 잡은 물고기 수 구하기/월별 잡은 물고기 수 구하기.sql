-- 코드를 작성해주세요
select count(1) as FISH_COUNT, month(time) as MONTH
from fish_info
group by MONTH
order by MONTH