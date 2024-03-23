select count(1) as FISH_COUNT
from 
(select a.fish_type, b.fish_name
from fish_info a
left join fish_name_info b
on a.fish_type = b.fish_type
where b.fish_name in ('BASS', 'SNAPPER')
) tb