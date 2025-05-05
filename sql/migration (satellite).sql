-- загрузка атрибутов активностей в сателлит

insert into DWH.s_activities (
	hk_l_user_group_activity,
	event,
	event_dt,
	load_dt,
	load_src
	)
select distinct
	luga.hk_l_user_group_activity,   -- fk на линк связей пользователей и групп
	gl.event,   -- активность
	gl.datetime,   -- время активности
	now() as load_dt,   -- время загрузки 
	's3' as load_src   -- источник
from STG.group_log as gl
left join DWH.h_users as hu 
	on gl.user_id = hu.user_id
left join DWH.h_groups as hg
	on gl.group_id = hg.group_id
left join DWH.l_user_group_activity as luga
	on luga.hk_group_id = hg.hk_group_id and luga.hk_user_id = hu.hk_user_id;