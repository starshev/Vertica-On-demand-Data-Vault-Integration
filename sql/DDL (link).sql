-- DDL таблицы-линка с активностями

drop table if exists DWH.l_user_group_activity;

create table DWH.l_user_group_activity (
	hk_l_user_group_activity bigint primary key,   -- хешированный pk
	hk_user_id bigint not null constraint fk_l_admin_user references DWH.h_users (hk_user_id),   -- fk на хаб пользователей
	hk_group_id bigint not null constraint fk_l_admin_groups references DWH.h_groups (hk_group_id),   -- fk на хаб групп
	load_dt timestamp,   -- время загрузки
	load_src varchar(20)   -- источник
	)
order by load_dt   -- сортируем по датам загрузки
segmented by hk_user_id all nodes   -- сегментируем по юзерам (для колокации юзеров и активностей на кластере)
partition by load_dt::date   -- партиции по дням
group by calendar_hierarchy_day(load_dt::date, 3, 2);