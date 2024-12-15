-- заполненние справочника мастеров
insert into dwh.d_craftsmans 
	(craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, CURRENT_TIMESTAMP from source3.craft_market_craftsmans) 
on conflict (craftsman_id)
	do nothing -- пропускаем старые записи

-- заполненние справочника покупателей
insert into dwh.d_customers 
	(customer_id, customer_name, customer_address, customer_birthday, customer_email, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select customer_id, customer_name, customer_address, customer_birthday, customer_email, CURRENT_TIMESTAMP from source3.craft_market_customers) 
on conflict (customer_id)
	do nothing -- пропускаем старые записи

-- заполненние справочника продуктов
insert into dwh.d_products 
	(product_id, product_name, product_description, product_type, product_price, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select product_id, product_name, product_description, product_type, product_price, CURRENT_TIMESTAMP from source2.craft_market_masters_products) 
on conflict (product_id)
	do nothing -- пропускаем старые записи

-- заполненние справочника заказов
insert into dwh.f_orders
	(order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
	OVERRIDING SYSTEM VALUE 
	(select order_id, product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, CURRENT_TIMESTAMP from source3.craft_market_orders) 
on conflict (order_id) 
	do update set ( order_completion_date, order_status, load_dttm) = 
		( excluded.order_completion_date, excluded.order_status, excluded.load_dttm)
    where dwh.f_orders.order_status != excluded.order_status