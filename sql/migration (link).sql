-- загрузка новых связей групп и пользователей в линк

insert into DWH.l_user_group_activity (
	hk_l_user_group_activity,
	hk_user_id,
	hk_group_id,
	load_dt,
	load_src
	)
select distinct
	hash(hu.hk_user_id, hg.hk_group_id),   -- хешируем составной pk
	hu.hk_user_id,   -- fk на хаб пользователей
	hg.hk_group_id,   -- fk на хаб групп
	now() as load_dt,   -- время загрузки
	's3' as load_src   -- источник
from STG.group_log as gl
left join DWH.h_users as hu on gl.user_id = hu.user_id   -- left для проверки, что все ключи хабов на месте
left join DWH.h_groups as hg on gl.group_id = hg.group_id   -- left для проверки, что все ключи хабов на месте
where hash(hu.hk_user_id, hg.hk_group_id) not in (select hk_l_user_group_activity from DWH.l_user_group_activity);   -- фильтруем новые связи