-- 코드를 작성해주세요

select id,length
from fish_info
where length is not null
order by 2 desc,1
limit 10;