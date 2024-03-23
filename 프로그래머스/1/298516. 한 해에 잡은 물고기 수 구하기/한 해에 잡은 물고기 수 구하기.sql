-- 코드를 작성해주세요

select count(1) as FISH_COUNT
from (
select year(time)
    from fish_info
    where year(time) = '2021'
) t