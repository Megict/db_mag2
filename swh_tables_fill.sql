-- заполненние справочника мастеров
-- данные из таблицы craft_market_craftsmans
insert into dwh.d_craftsmans 
	(craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, CURRENT_TIMESTAMP from source3.craft_market_craftsmans) 
on conflict (craftsman_id)
	do update set ( craftsman_name, craftsman_address, craftsman_email, load_dttm) = 
		( excluded.craftsman_name, excluded.craftsman_address, excluded.craftsman_email, excluded.load_dttm) -- обновляем старые записи
;
-- данные из таблицы craft_market_masters_products
insert into dwh.d_craftsmans 
	(craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, CURRENT_TIMESTAMP from source2.craft_market_masters_products) 
on conflict (craftsman_id)
	do update set ( craftsman_name, craftsman_address, craftsman_email, load_dttm) = 
		( excluded.craftsman_name, excluded.craftsman_address, excluded.craftsman_email, excluded.load_dttm) -- обновляем старые записи
;
-- данные из таблицы craft_market_wide
insert into dwh.d_craftsmans 
	(craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select distinct craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, CURRENT_TIMESTAMP from source1.craft_market_wide) 
on conflict (craftsman_id)
	do update set ( craftsman_name, craftsman_address, craftsman_email, load_dttm) = 
		( excluded.craftsman_name, excluded.craftsman_address, excluded.craftsman_email, excluded.load_dttm) -- обновляем старые записи
;
-----------------------------------------------------
-- заполненние справочника покупателей
-- данные из таблицы craft_market_customers
insert into dwh.d_customers 
	(customer_id, customer_name, customer_address, customer_birthday, customer_email, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select customer_id, customer_name, customer_address, customer_birthday, customer_email, CURRENT_TIMESTAMP from source3.craft_market_customers) 
on conflict (customer_id)
	do update set ( customer_name, customer_address, customer_email, load_dttm) = 
		( excluded.customer_name, excluded.customer_address, excluded.customer_email, excluded.load_dttm) -- обновляем старые записи
;
-- данные из таблицы craft_market_orders_customers
insert into dwh.d_customers 
	(customer_id, customer_name, customer_address, customer_birthday, customer_email, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select customer_id, customer_name, customer_address, customer_birthday, customer_email, CURRENT_TIMESTAMP from source2.craft_market_orders_customers) 
on conflict (customer_id)
	do update set ( customer_name, customer_address, customer_email, load_dttm) = 
		( excluded.customer_name, excluded.customer_address, excluded.customer_email, excluded.load_dttm) -- обновляем старые записи
;
-- данные из таблицы craft_market_wide
insert into dwh.d_customers 
	(customer_id, customer_name, customer_address, customer_birthday, customer_email, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select distinct customer_id, customer_name, customer_address, customer_birthday, customer_email, CURRENT_TIMESTAMP from source1.craft_market_wide) 
on conflict (customer_id)
	do update set ( customer_name, customer_address, customer_email, load_dttm) = 
		( excluded.customer_name, excluded.customer_address, excluded.customer_email, excluded.load_dttm) -- обновляем старые записи
;
-----------------------------------------------------
-- заполненние справочника продуктов
-- данные из таблицы craft_market_orders
insert into dwh.d_products 
	(product_id, product_name, product_description, product_type, product_price, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select product_id, product_name, product_description, product_type, product_price, CURRENT_TIMESTAMP from source3.craft_market_orders) 
on conflict (product_id)
	do update set ( product_name, product_description, product_price, load_dttm) = 
		( excluded.product_name, excluded.product_description, excluded.product_price, excluded.load_dttm) -- обновляем старые записи
;
-- данные из таблицы craft_market_masters_products
insert into dwh.d_products 
	(product_id, product_name, product_description, product_type, product_price, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select product_id, product_name, product_description, product_type, product_price, CURRENT_TIMESTAMP from source2.craft_market_masters_products) 
on conflict (product_id)
	do update set ( product_name, product_description, product_price, load_dttm) = 
		( excluded.product_name, excluded.product_description, excluded.product_price, excluded.load_dttm) -- обновляем старые записи
;
-- данные из таблицы craft_market_masters_products
insert into dwh.d_products 
	(product_id, product_name, product_description, product_type, product_price, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select distinct product_id, product_name, product_description, product_type, product_price, CURRENT_TIMESTAMP from source1.craft_market_wide) 
on conflict (product_id)
	do update set ( product_name, product_description, product_price, load_dttm) = 
		( excluded.product_name, excluded.product_description, excluded.product_price, excluded.load_dttm) -- обновляем старые записи
;
-----------------------------------------------------
-- заполненние справочника заказов
-- данные из таблицы markets_orders
insert into dwh.f_orders
	(order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, CURRENT_TIMESTAMP from source3.craft_market_orders)
on conflict (order_id) 
	do update set ( order_completion_date, order_status, load_dttm) = 
		( excluded.order_completion_date, excluded.order_status, excluded.load_dttm)
    where dwh.f_orders.order_status != excluded.order_status
;
-- данные из таблицы market_orders_customers
insert into dwh.f_orders
	(order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, CURRENT_TIMESTAMP from source2.craft_market_orders_customers)
on conflict (order_id) 
	do update set ( order_completion_date, order_status, load_dttm) = 
		( excluded.order_completion_date, excluded.order_status, excluded.load_dttm)
    where dwh.f_orders.order_status != excluded.order_status
;
-- данные из таблицы craft_market_wide
insert into dwh.f_orders
	(order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, CURRENT_TIMESTAMP from source1.craft_market_wide)
on conflict (order_id) 
	do update set ( order_completion_date, order_status, load_dttm) = 
		( excluded.order_completion_date, excluded.order_status, excluded.load_dttm)
    where dwh.f_orders.order_status != excluded.order_status
;