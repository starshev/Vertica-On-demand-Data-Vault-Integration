-- DDL сателлита с логами активностей

drop table if exists DWH.s_activities;

create table DWH.s_activities (
	hk_l_user_group_activity bigint constraint fk_s_comments_l_user_group_activity   -- fk на линк связей пользователей и групп
	references DWH.l_user_group_activity (hk_l_user_group_activity),
	event varchar(6),   -- активность
	event_dt timestamp,   -- время активности
	load_dt timestamp,   -- время загрузки
	load_src varchar(20)   -- источник
	)
order by event_dt   -- сортируем по времени активностей
segmented by hk_l_user_group_activity all nodes   -- сегментируем по fk
partition by event_dt::date   -- партиции по дням
group by calendar_hierarchy_day(event_dt::date, 3, 2);