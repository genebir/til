-- 코드를 작성해주세요
select count(1) as FISH_COUNT, t.fish_name as FISH_NAME
from
(select a.fish_type, b.fish_name
from fish_info a
left join fish_name_info b
on a.fish_type = b.fish_type
) t
group by t.fish_name
order by FISH_COUNT desc