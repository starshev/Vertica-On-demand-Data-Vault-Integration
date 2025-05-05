-- DDL таблицы логов активностей

drop table if exists STG.group_log;

create table STG.group_log (
	group_id int not null,   -- бизнес-ключ группы
	user_id int not null,   -- бизнес-ключ пользователя
	event varchar(20),   -- активность
	datetime timestamp,   -- время активности
	foreign key (group_id) references STG.groups(id),   -- время загрузки
	foreign key (user_id) references STG.users(id)   -- источник
)
order by group_id, user_id   -- сортируем по ключам джоинов
segmented by hash(group_id) all nodes   -- сегментируем по группам (обеспечиваем колокацию групп + логов на кластере)
partition by datetime::date   -- партиции по дням и возможностью анализа MoM / YoY за 2 года
group by calendar_hierarchy_day(datetime::date, 3, 2);