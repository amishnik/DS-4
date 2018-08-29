--создаю таблицы в базе
CREATE TABLE IF NOT EXISTS clients (
   client_name varchar, clientid int, gender varchar, birthdate date);

CREATE TABLE IF NOT EXISTS Activitystatus (
ActivityName varchar, Activityid int, Status varchar, Statuscode int, State varchar, Statecode int);

CREATE TABLE IF NOT EXISTS Activities (
Activitydate timestamp, Activityid int, UniqueId int, Statuscode int, Client_id int, Direction boolean, OwnerName varchar);

CREATE TABLE IF NOT EXISTS Activityid (
ActivityName varchar, Activityid int);

CREATE TABLE IF NOT EXISTS Transactions (
Date timestamp, Sum decimal, Client_id int);

--импортирую данные из файлов в таблицы
\copy clients FROM '/data/clients.csv' DELIMITER ',' CSV HEADER;

\copy Activitystatus FROM '/data/activitystatus.csv' DELIMITER ',' CSV HEADER;

\copy Activities FROM '/data/activities.csv' DELIMITER ',' CSV HEADER;

\copy Activityid FROM '/data/activityid.csv' DELIMITER ',' CSV HEADER;

\copy Transactions FROM '/data/transactions.csv' DELIMITER ',' CSV HEADER;

--+столбец в таблицу клиенты с возрастной группой
ALTER TABLE clients ADD COLUMN age int, ADD COLUMN agegroup varchar;

--вычисляем возраст и возрастные группы клиентов
UPDATE clients SET age=extract(year FROM age(birthdate::timestamp));

UPDATE clients SET agegroup = 
CASE WHEN age<25 THEN 'до 25'
	WHEN age BETWEEN 25 AND 35 THEN 'от 25 до 35'
	WHEN age BETWEEN 35 AND 45 THEN 'от 35 до 45'
	WHEN age BETWEEN 45 AND 55 THEN 'от 45 до 55'
	WHEN age BETWEEN 55 AND 65 THEN 'от 55 до 65'
	ELSE 'более 65'
	END;


--строим представление с данными о том какие возрастные группы пополнили счета, какое кол-во клиентов пополняли счета
SELECT agegroup AS "Возрастная группа", SUM(sum) as total, COUNT(sum) AS "Количество транзакций", (SUM(sum) / COUNT(sum))::decimal(10,2) AS sum_tr, COUNT(DISTINCT client_id) AS client_num,
--среднее число транзакций на клиента
CASE WHEN COUNT(sum) > 0 THEN (COUNT(sum)::decimal(4,2) / COUNT(DISTINCT Client_id)::decimal(4,2))::decimal(4,2) ELSE 0 END AS avg_tr,
COUNT(clientid) AS total_client_num 
FROM transactions t RIGHT JOIN clients c ON t.client_id=c.clientid 
GROUP BY agegroup ORDER BY total DESC NULLS LAST;

-- строим представление по коммуникациям с клиентами
WITH target_clients AS (SELECT clientid, SUM(sum) AS total, MAX(date) AS last_trandate FROM clients c INNER JOIN transactions t ON c.clientid=t.client_id GROUP BY clientid) 
SELECT clientid, MAX(total), COUNT(activityid) AS total_comm_num, MAX(Activitydate) AS final_comm, to_char(MAX(last_trandate), 'YYYY-MM-DD') AS last_trandate 
INTO TEMP ttarget_clients
FROM target_clients tc 
JOIN activities a ON tc.clientid=a.client_id 
GROUP BY clientid;

--добавляю столбец для учета успешной коммуникации
ALTER TABLE activities ADD COLUMN StatusName text;
--проставляю идентификатор для статуса коммуникации, на основе которого будет идет расчет кол-ва успешных коммуникаций
UPDATE activities a SET StatusName = Statecode FROM activitystatus s WHERE a.Statuscode=s.Statuscode;

	--в новую таблицу целевых коммуникаций (target_comm) записываем данные по клиентам совершившим целевое действие, добавляем к клиентам %% успешных коммуникаций
	
	SELECT clientid, MAX(max), MAX(total_comm_num) as total_comm_num, MAX(Activitydate) AS final_comm, MAX(last_trandate) AS last_trandate
	, SUM(CASE WHEN StatusName='1' THEN 1 ELSE 0 END) AS success_comm_num
	--смотрим соотношение числа успешных коммуникаций к неуспешным
	, (SUM(CASE WHEN StatusName='1' THEN 1 ELSE 0 END)::decimal / MAX(total_comm_num)::decimal)*100 AS comm_diff 
	INTO TABLE target_comm
	FROM ttarget_clients ttc JOIN activities a ON ttc.clientypetid=a.client_id 
	GROUP BY clientid ORDER BY comm_diff DESC, max DESC;

--из таблицы целевых коммуникаций беру данные о проценте успешных коммуникаций и накладываю на таблицу возрастных групп клиентов
SELECT AVG(comm_diff) AS avg_diff, (CASE WHEN MAX(max) > 0 THEN SUM(max) ELSE 0 END) AS total, agegroup 
FROM target_comm tc LEFT JOIN clients c ON tc.clientid=c.clientid GROUP BY agegroup ORDER BY avg_diff DESC, total DESC;

--так как клиенты в возрастной группе от 35 до 45 имею максимальное сальдо по операциям ввода.вывода денежных средств
--и клиентв в озрастной группе от 35 до 45 имеют лучшее соотношение числа успешных коммуникаций к общему числу коммуникаций
--анализируем всех клиентов из возрастной группы и выделяем список клиентов у кого общее число коммуникаций ниже чем у сегмента выполнивших целевое действие
--выбираем таких клиентов в отдельный список с тем чтобы провести по ним дополнитльные коммуникации
--DROP TABLE clients_to_comm;
WITH clients35_45 AS (
SELECT clientid FROM clients WHERE agegroup = 'от 35 до 45' EXCEPT SELECT clientid FROM target_comm)
SELECT c3.clientid, COUNT(activityid), (SELECT AVG(total_comm_num) FROM target_comm tc JOIN clients c ON tc.clientid=c.clientid WHERE agegroup ='от 35 до 45') AS avg 
	, SUM(CASE WHEN StatusName='1' THEN 1 ELSE 0 END) AS success_comm_num
	--смотрим соотношение числа успешных коммуникаций к неуспешным
	, (SUM(CASE WHEN StatusName='1' THEN 1 ELSE 0 END)::decimal(2) / COUNT(activityid)::decimal(2))*100 AS comm_diff 
--INTO TABLE clients_to_comm
FROM clients35_45 c3 JOIN activities a ON c3.clientid=a.client_id
GROUP BY c3.clientid --HAVING COUNT(activityid) < (SELECT AVG(total_comm_num) FROM target_comm tc JOIN clients c ON tc.clientid=c.clientid WHERE agegroup ='от 35 до 45') 
ORDER BY comm_diff DESC;

--создаю представление с клиентами с кем нужно связаться на основе таблицы clients_to_comm, клиенты с наилучшим отношением успешных коммуникаций к общему числу коммуникаций в 
--приоритете
CREATE OR REPLACE VIEW clients_to_communicate 
AS SELECT clientid AS Клиент
, count AS Количество_коммуникаций
, avg::numeric(2) AS Целевое_количество
, success_comm_num AS Число_успешных_коммуникаций
, comm_diff AS Успешность_коммуникаций
 FROM clients_to_comm WHERE count < avg;

--выгружаю в csv-файл итог запроса
 \copy (SELECT * FROM clients_to_communicate) to '/data/clients_to_communicate.csv' DELIMITER ',' HEADER CSV;

--создаю представение в форме отчета по общему сальду операций ввода.вывода денежных средств клиентами в рзрезе возрастных групп клиентов
CREATE OR REPLACE VIEW transaction_report
AS SELECT agegroup AS "Возрастная группа"
, SUM(sum) as "Сальдо ввода/вывода"
, COUNT(sum) AS "Количество транзакций"
, (SUM(sum) / COUNT(sum))::decimal(10,2) AS "Среняя сумма транзакции"
, COUNT(DISTINCT client_id) AS "Число целевых клиентов",
CASE WHEN COUNT(sum) > 0 THEN (COUNT(sum)::decimal(4,2) / COUNT(DISTINCT Client_id)::decimal(4,2))::decimal(4,2) ELSE 0 END AS "Среднее число транзакций на клиента"
,COUNT(clientid) AS "Общее число клиентов у группе"
FROM transactions t RIGHT JOIN clients c ON t.client_id=c.clientid 
GROUP BY agegroup ORDER BY "Сальдо ввода/вывода" DESC NULLS LAST;


