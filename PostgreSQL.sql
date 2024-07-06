/* 
Выведите на экран названия и адреса компаний-заказчиков, которые находятся во Франции (англ. France).
*/
SELECT company_name,
        address
FROM northwind.customers
WHERE country = 'France'

/*
Выведите на экран имена и фамилии всех сотрудников из Лондона (англ. London)
*/
SELECT first_name,
       last_name
FROM northwind.employees
WHERE city = 'London'

/*
Выведите на экран имена и фамилии сотрудников из Лондона (англ. London), чей домашний номер телефона заканчивается на 8.
*/
SELECT first_name,
       last_name
FROM northwind.employees
WHERE city = 'London'
  AND home_phone LIKE '%8'

/*
Выведите на экран список уникальных названий городов, начинающихся на San, в которых оформили хотя бы один заказ после 16 июля 1996 года. 
Отсортируйте таблицу в лексикографическом порядке по убыванию.
*/
SELECT DISTINCT(ship_city)
FROM northwind.orders
WHERE order_date::date > '1996-07-16'
  AND ship_city LIKE 'San%'
ORDER BY ship_city DESC

/*
Выведите всю информацию о сотрудниках, отсортировав записи в порядке убывания их даты рождения.
*/
SELECT *
FROM northwind.employees
ORDER BY birth_date::date DESC

/* 
Выведите всю информацию из первых 100 записей таблицы заказов, отсортированных по стране доставки в лексикографическом порядке по возрастанию.
*/
SELECT *
FROM northwind.orders
ORDER BY ship_country 
LIMIT 100

/*
Используя таблицу с заказами, выведите количество уникальных идентификаторов клиентов (поле customer_id), которые совершили хотя бы один заказ.
*/
SELECT COUNT(DISTINCT(customer_id))
FROM northwind.orders

/*
Для всех товаров, у которых указан поставщик, выведите пары с названием товара и названием компании-поставщика этого товара.
*/
SELECT p.product_name,
       s.company_name
FROM northwind.suppliers AS s
LEFT JOIN northwind.products AS p ON p.supplier_id=s.supplier_id


/*
Выведите среднюю цену товаров каждой категории из таблицы products. Округлите среднее до двух знаков после запятой.
*/
SELECT category_id, 
       ROUND(AVG(unit_price::numeric), 2)
FROM northwind.products 
GROUP BY category_id

/*
Выведите уникальные названия всех стран, в которые было отправлено более 10 заказов. 
Отсортируйте вывод по названию страны в лексикографическом порядке по убыванию.
*/
SELECT ship_country
FROM northwind.orders
GROUP BY ship_country
HAVING COUNT(ship_country) > 10
ORDER BY ship_country DESC

/*
Отберите страны, в которых оформили больше 30 заказов, и выведите количество заказов в этих странах. 
Результаты отсортируйте по названию страны в лексикографическом порядке. 
*/
SELECT ship_country,
       COUNT(ship_country) 
FROM northwind.orders
GROUP BY ship_country
HAVING COUNT(ship_country) > 30
ORDER BY ship_country

/*
Выведите на экран названия товаров с ценой выше среднего среди всех представленных позиций в таблице.
*/
SELECT product_name
FROM northwind.products
WHERE unit_price > (SELECT AVG(unit_price) FROM northwind.products)

/*
Выведите названия товаров с ценой ниже средней среди всех представленных товаров или равной ей.
*/
SELECT product_name
FROM northwind.products
WHERE unit_price < (SELECT AVG(unit_price) FROM northwind.products)

/*
Выведите на экран идентификаторы заказов и для каждого из них — его суммарную стоимость с учётом всех товаров, включённых в заказ, и их количества, но без учёта
скидки. Не округляйте получившиеся значения. 
*/
WITH tab AS (
    SELECT order_id,
       unit_price * quantity AS order_total
    FROM northwind.order_details)

SELECT order_id,
       SUM(order_total)
FROM tab
GROUP BY order_id

/*
Выведите на экран идентификаторы заказов и для каждого из них — суммарную стоимость заказа с учётом всех заказанных товаров и их количества с учётом скидки. 
Получившиеся значения округлите до ближайшего целого числа. Отсортируйте выдачу по возрастанию идентификаторов заказов.
*/
SELECT order_id,
       ROUND(SUM(unit_price * quantity * (1-discount))) AS order_total_with_discount
FROM northwind.order_details
GROUP BY order_id
ORDER BY order_id

/*
Выведите информацию о каждом товаре:
    его идентификатор из таблицы с товарами;
    его название из таблицы с товарами;
    название его категории из таблицы категорий;
    описание его категории из таблицы категорий.
Таблицу отсортируйте по возрастанию идентификаторов товаров.
*/
SELECT p.product_id,
       p.product_name,
       c.category_name,
       c.description
FROM northwind.products AS p
LEFT JOIN northwind.categories AS c ON c.category_id=p.category_id
ORDER BY p.product_id

/*
Для каждого месяца каждого года посчитайте уникальных пользователей, оформивших хотя бы один заказ в этот месяц. Значение месяца приведите к типу date.
*/
SELECT date_trunc('month', order_date)::date AS month,
       COUNT(DISTINCT(customer_id))
FROM northwind.orders
GROUP BY date_trunc('month', order_date)::date
ORDER BY month

/*
Для каждого года из таблицы заказов посчитайте суммарную выручку с продаж за этот год. Используйте детальную информацию о заказах. 
Не забудьте учесть скидку (поле discount) на товар. Результаты отсортируйте по убыванию значения выручки.
*/
SELECT EXTRACT(YEAR FROM o.order_date),
       SUM(od.unit_price * od.quantity * (1-od.discount)) AS total_price
FROM northwind.order_details AS od
LEFT JOIN northwind.orders AS o ON o.order_id=od.order_id
GROUP BY EXTRACT(YEAR FROM o.order_date)
ORDER BY total_price DESC

/*
Выведите названия компаний-покупателей, которые совершили не менее двух заказов в 1996 году. Отсортируйте вывод по полю с названиями компаний в 
лексикографическом порядке по возрастанию.
*/
SELECT c.company_name
FROM northwind.customers AS c
LEFT JOIN northwind.orders AS o ON o.customer_id=c.customer_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1996
GROUP BY c.company_name
HAVING count(o.customer_id) >= 2
ORDER BY c.company_name

/*
Выведите названия компаний-покупателей, которые совершили более пяти заказов в 1997 году. 
Отсортируйте вывод по полю с названиями компаний в лексикографическом порядке по убыванию
*/
SELECT c.company_name
FROM northwind.customers AS c
LEFT JOIN northwind.orders AS o ON o.customer_id=c.customer_id
WHERE EXTRACT(YEAR FROM o.order_date) = 1997
GROUP BY c.company_name
HAVING count(o.customer_id) > 5
ORDER BY c.company_name DESC

/*
Выведите среднее количество заказов компаний-покупателей за период с 1 января по 1 июля 1998 года. Округлите среднее до ближайшего целого числа. 
В расчётах учитывайте только те компании, которые совершили более семи покупок за всё время, а не только за указанный период.
*/
WITH
temp_tab AS (
    SELECT c.customer_id, COUNT(o.order_id) AS count
    FROM northwind.customers AS c 
    JOIN northwind.orders AS o ON c.customer_id=o.customer_id
    WHERE c.customer_id IN (
        SELECT customer_id
        FROM northwind.orders
        GROUP BY customer_id
        HAVING COUNT(order_id) > 7
    )
      AND o.order_date BETWEEN '1998-01-01' AND '1998-07-01' 
    GROUP BY c.customer_id
)

/*
Выведите на экран названия компаний-покупателей, которые хотя бы раз оформили более одного заказа в день. Для подсчёта заказов используйте поле order_date. 
Отсортируйте названия компаний в лексикографическом порядке по возрастанию. 
*/
WITH daily_orders AS ( 
  SELECT 
    c.company_name, 
    o.order_date, 
    COUNT(o.order_id) AS num_orders 
  FROM 
    northwind.customers AS c 
    LEFT JOIN northwind.orders AS o ON c.customer_id = o.customer_id 
  GROUP BY 
    c.company_name, 
    o.order_date 
) 

SELECT company_name
FROM daily_orders
WHERE num_orders > 1
GROUP BY company_name 
ORDER BY company_name

/*
Выведите города, в которые отправляли заказы не менее 10 раз. Названия городов отсортируйте в лексикографическом порядке по убыванию. 
*/
SELECT ship_city
FROM northwind.orders
GROUP BY ship_city
HAVING COUNT(order_id) >= 10
ORDER BY ship_city DESC

/*
Выведите города, в которые отправляли заказы не более 12 раз. Названия городов отсортируйте в лексикографическом порядке по возрастанию.
*/
SELECT ship_city
FROM northwind.orders
GROUP BY ship_city
HAVING COUNT(order_id) <= 12
ORDER BY ship_city

/*
На сколько процентов ежемесячно менялось количество заказов в компании Northwind с 1 апреля по 1 декабря 1997 года? Отобразите таблицу со следующими полями:
    номер месяца;
    количество заказов в месяц;
    процент, который показывает, насколько изменилось количество заказов в текущем месяце по сравнению с предыдущим.

Если заказов стало меньше, значение процента должно быть отрицательным, если больше — положительным. Округлите значение процента до двух знаков после запятой. 

Отсортируйте таблицу по возрастанию значения месяца. Напомним, что при делении одного целого числа на другое в PostgreSQL в результате получится целое число, 
округлённое до ближайшего целого вниз. Чтобы этого избежать, переведите делимое в тип numeric.
*/
WITH tab AS (
SELECT EXTRACT(MONTH FROM order_date) AS creation_month,
       COUNT(order_id) AS orders_count	
FROM northwind.orders
WHERE order_date BETWEEN '1997-04-01' AND '1997-12-01'
GROUP BY EXTRACT(MONTH FROM order_date)
ORDER BY EXTRACT(MONTH FROM order_date)
)

SELECT *,
       ROUND(((orders_count::numeric / LAG(orders_count) OVER (ORDER BY creation_month)) - 1) * 100, 2) AS percentage
FROM tab

/*
На сколько процентов ежегодно менялось количество заказов в Northwind с 1996 по 1998 годы. Отобразите таблицу со следующими полями:
    Число года.
    Количество заказов за год.
    Процент, округлённый до целого числа, который показывает, насколько изменилось количество заказов в текущем году по сравнению с предыдущим. Для 1996 года 
    выведите значение NULL.
*/
WITH tab AS (
SELECT EXTRACT(YEAR FROM order_date) AS creation_year,
       COUNT(order_id) AS orders_count	
FROM northwind.orders
WHERE order_date BETWEEN '1996-01-01' AND '1998-12-31'
GROUP BY EXTRACT(YEAR FROM order_date)
ORDER BY EXTRACT(YEAR FROM order_date)
)

SELECT *,
       ROUND(((orders_count::numeric / LAG(orders_count) OVER (ORDER BY creation_year)) - 1) * 100) AS percentage
FROM tab

/*
Рассчитайте аналог Retention Rate по неделям для компаний-заказчиков. Объедините компании в когорты по неделе их первого заказа (поле order_date). 
Возвращение определяйте по наличию заказа в течение текущей недели. 
Перед тем как выделить неделю из даты, приведите значения к типу timestamp. Значение Retention Rate округлите до двух знаков после запятой.
*/
WITH init_tab AS (
SELECT customer_id,
       MIN(DATE_TRUNC('week', order_date::timestamp)) OVER (PARTITION BY customer_id) AS cohort_dt,
       DATE_TRUNC('week', order_date::timestamp) AS purchase_date
FROM northwind.orders
ORDER BY cohort_dt
)

SELECT cohort_dt, 
       purchase_date, 
       COUNT(DISTINCT customer_id) AS users_cnt,
       MAX(COUNT(DISTINCT customer_id)) OVER (PARTITION BY cohort_dt) AS cohort_users_cnt,
       ROUND(COUNT(DISTINCT customer_id)::numeric / MAX(COUNT(DISTINCT customer_id)::numeric) OVER (PARTITION BY cohort_dt) * 100, 2) AS retention_rate
FROM init_tab 
GROUP BY cohort_dt, purchase_date

/*
Рассчитайте аналог Retention Rate по месяцам для компаний-заказчиков. Объедините компании в когорты по месяцу их первого заказа (поле order_date). 
Возвращение определяйте по наличию заказа в текущем месяце. 
Перед тем как выделить неделю из даты, приведите значения к типу timestamp. Значение Retention Rate округлите до двух знаков после запятой. 
*/
WITH init_tab AS (
SELECT customer_id,
       MIN(DATE_TRUNC('month', order_date::timestamp)) OVER (PARTITION BY customer_id) AS cohort_dt,
       DATE_TRUNC('month', order_date::timestamp) AS purchase_date
FROM northwind.orders
ORDER BY cohort_dt
)

SELECT cohort_dt, 
       purchase_date, 
       COUNT(DISTINCT customer_id) AS users_cnt,
       MAX(COUNT(DISTINCT customer_id)) OVER (PARTITION BY cohort_dt) AS cohort_users_cnt,
       ROUND(COUNT(DISTINCT customer_id)::numeric / MAX(COUNT(DISTINCT customer_id)::numeric) OVER (PARTITION BY cohort_dt) * 100, 2) AS retention_rate
FROM init_tab 
GROUP BY cohort_dt, purchase_date

/*
Рассчитайте аналог Retention Rate по годам для компаний-покупателей Northwind. Объедините пользователей в когорты по году их первого заказа (поле order_date). 
Возвращение определяйте по наличию заказа в текущем году. 
Перед тем как выделить неделю из даты, приведите значения к типу timestamp. Значение Retention Rate округлите до двух знаков после запятой.
*/
WITH init_tab AS (
SELECT customer_id,
       MIN(DATE_TRUNC('year', order_date::timestamp)) OVER (PARTITION BY customer_id) AS cohort_dt,
       DATE_TRUNC('year', order_date::timestamp) AS purchase_date
FROM northwind.orders
ORDER BY cohort_dt
)

SELECT cohort_dt, 
       purchase_date, 
       COUNT(DISTINCT customer_id) AS users_cnt,
       MAX(COUNT(DISTINCT customer_id)) OVER (PARTITION BY cohort_dt) AS cohort_users_cnt,
       ROUND(COUNT(DISTINCT customer_id)::numeric / MAX(COUNT(DISTINCT customer_id)::numeric) OVER (PARTITION BY cohort_dt) * 100, 2) AS retention_rate
FROM init_tab 
GROUP BY cohort_dt, purchase_date

/*
Для каждой компании, которая оформила хотя бы два заказа, выведите:
    дату оформления второго заказа (поле order_date), округлённую до месяца;
    идентификатор компании, оформившей заказ (поле customer_id).

Строки отсортируйте по значению в поле с идентификаторами в лексикографическом порядке по убыванию.
*/
WITH ranged_ids AS (
    SELECT 
        customer_id,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_rank
    FROM northwind.orders
)

SELECT DATE_TRUNC('month', order_date)::date AS second_purchase, 
       customer_id
FROM ranged_ids
WHERE order_rank = 2
ORDER BY customer_id DESC

/*
Для каждого месяца с июля 1996 года по май 1998 года посчитайте конверсию в процентах. Найдите количество уникальных компаний-заказчиков в текущем месяце. 
Разделите его на общее количество компаний-заказчиков Northwind, которые оформили хотя бы один заказ за всё предыдущее время, включая текущий месяц. Под уникальностью компании в этой задаче подразумевается отсутствие повторов в выборке.
В итоговой таблице должны быть следующие поля:
    дата первого числа текущего месяца;
    количество компаний-заказчиков в текущий месяц;
    общее количество компаний-заказчиков за всё предыдущее время, включая текущий месяц;
    отношение количества покупателей за текущий месяц к общему количеству покупателей.

При делении не забудьте привести значения к вещественному типу real, тогда после деления вы получите вещественное, а не целое число. 
Не забудьте умножить результат на 100 и округлить значение до двух знаков после запятой. Перед округлением приведите результат в процентах к типу numeric, 
чтобы округление прошло без ошибок.
*/
WITH 
-- уникальных компаний в месяц (не встречавшихся ранее)
uniq_customers_month AS (
SELECT 
    m.month,
    COUNT(DISTINCT o.customer_id) AS customers_per_month
FROM 
    (SELECT DISTINCT DATE_TRUNC('month', order_date)::date AS month 
     FROM northwind.orders) AS m
    LEFT JOIN (
        SELECT 
            DATE_TRUNC('month', order_date)::date AS month,
            customer_id,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY DATE_TRUNC('month', order_date)::date) AS row_num
        FROM northwind.orders
    ) AS o
    ON m.month = o.month AND o.row_num = 1
GROUP BY m.month
ORDER BY m.month
),

-- суммирование уникальных компаний без повторений по месяцам
total_customers_tab AS (
    SELECT month,
           SUM(customers_per_month) OVER (ORDER BY month) AS total_customers
    FROM uniq_customers_month
),

unique_customers AS (
    SELECT DATE_TRUNC('month', order_date)::date AS month,
           COUNT(DISTINCT customer_id) AS customers_this_month
    FROM northwind.orders
    GROUP BY DATE_TRUNC('month', order_date)::date
)

SELECT total_customers_tab.month,
       unique_customers.customers_this_month,
       total_customers_tab.total_customers,
       ROUND(((customers_this_month::real / total_customers::real) * 100)::numeric, 2) AS conversion
FROM total_customers_tab
JOIN unique_customers ON total_customers_tab.month=unique_customers.month