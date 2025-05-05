-- 10 самых ранних групп по дате регистрации
with top_10_groups as (
    select distinct hk_group_id, registration_dt
    from DWH.h_groups
    order by registration_dt asc
    limit 10
	),
-- кол-во уникальных пользователей по 10 самым ранним группам, оставивших 1+ комментарий
user_group_comments as (
	select
		luga.hk_group_id as hk_group_id,
		count(distinct sa.hk_l_user_group_activity) as cnt_users_with_comments
	from DWH.l_user_group_activity as luga
	inner join DWH.s_activities as sa
		on luga.hk_l_user_group_activity = sa.hk_l_user_group_activity
	where sa.event = 'comment'
	and luga.hk_group_id in (select hk_group_id from top_10_groups)
	group by luga.hk_group_id
	),
-- кол-во уникальных пользователей, однажды вступивших в 10 самых ранних групп
user_group_log as (
	select
		luga.hk_group_id as hk_group_id,
		count(distinct sa.hk_l_user_group_activity) as cnt_joined_users
	from DWH.l_user_group_activity as luga
	inner join DWH.s_activities as sa
		on luga.hk_l_user_group_activity = sa.hk_l_user_group_activity
	where sa.event = 'join'
	and luga.hk_group_id in (select hk_group_id from top_10_groups)
	group by luga.hk_group_id
	)
-- конверсия активности пользователей в 10 самых ранних группах
select
	ugl.hk_group_id as hk_group_id,   -- id группы
	ugl.cnt_joined_users as cnt_joined_users,   -- кол-во однажды вступивших пользователей
	ugc.cnt_users_with_comments as cnt_users_with_comments,   -- кол-во пользователей, оставивших 1+ комментарий
	round(ugc.cnt_users_with_comments / nullif(ugl.cnt_joined_users, 0), 2) as group_conversion   -- конверсия активности пользователей
from user_group_comments ugc
inner join user_group_log ugl
	on ugc.hk_group_id = ugl.hk_group_id
order by group_conversion desc;