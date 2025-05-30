## On-demand Vertica-based Data Vault Integration

### Task  
Build a pipeline for loading a CSV file with social network groups' activity logs from an S3 bucket and transporting it into the STG layer of the Vertica-based DWH (using Airflow). Enhance the DDS layer by adding a new link and satellite according to the Data Vault architecture. Write a query to address a one-time analytical task: calculate the conversion rate of group members into active participants (left 1+ comments), focusing on the 10 oldest communities.

### Skills  
ELT pipeline development with Airflow & Python clients for S3 and Vertica, writing DDL scripts for Vertica-based DWH with MPP considerations (segmentation, sorting, partitioning), Data Vault modeling, writing SQL migration scripts with MPP considerations, writing and testing migration scripts, addressing business question (calculating user activity conversion), solution documentation.

## Интеграция новых данных в Data Vault на базе Vertica

### Задача  
Построить пайплайн для загрузки csv-файла с логами активностей в группах социальной сети из бакета S3 и его доставки в STG-слой хранилища в Vertica (используя Airflow). Дополнить DDS-слой хранилища новым линком и сателлитом в соответствии с архитектурой Data Vault. Написать запрос для ответа на разовый аналитический вопрос бизнеса: вычислить конверсию участников 10 самых ранних групп в активных участников (оставивших 1+ комментарий).

### Навыки  
Разработка ELT-пайплайна с использованием Airflow и Python-клиентов для S3 и Vertica, написание DDL-скриптов для DWH на базе Vertica с учетом MPP (сегментация, сортировка, партиции), моделирование данных согласно архитектурному стилю Data Vault, написание SQL-скриптов для миграции данных с учетом MPP, написание и тестирование SQL-скриптов миграции, предоставление ответа на аналитический вопрос бизнеса (вычисление конверсии активности пользователей), документирование решения.
