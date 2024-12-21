insert into dwh.craftsman_report_datamart 
	(craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email,
	craftsman_money, platform_money, count_order, avg_price_order, avg_age_customer, median_time_order_completed,
	count_order_created, count_order_in_progress, count_order_delivery, count_order_done, count_order_not_done, report_period, top_product_category) 
select a.craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, man_money, our_money, order_count,
		avg_price, avg_c_age, median_order_time, coalesce(created_c, 0 ), coalesce(delivery_c, 0 ), coalesce(in_p_c, 0 ), coalesce(done_c, 0 ), coalesce(not_done_c, 0 ), to_char(CURRENT_TIMESTAMP, 'YYYY-MM'), top_product
from ((select craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email from dwh.d_craftsmans 
		where dwh.d_craftsmans.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) ) as a
		join 
		(select craftsman_id, sum(product_price)*0.9 as "man_money", sum(product_price)*0.1 as "our_money", count(order_id) as "order_count",
			sum(product_price) / count(product_price) as "avg_price", 
				sum(date_part('year', CURRENT_TIMESTAMP) - date_part('year', customer_birthday)) / 
				count(date_part('year', CURRENT_TIMESTAMP) - date_part('year', customer_birthday)) as "avg_c_age",
				percentile_cont(0.5) within group (order by order_completion_date - order_created_date) as "median_order_time"
			from dwh.f_orders 
				join dwh.d_products on dwh.f_orders.product_id = dwh.d_products.product_id 
				join dwh.d_customers on dwh.f_orders.customer_id  = dwh.d_customers.customer_id
			where order_created_date > CURRENT_TIMESTAMP - (3 * interval '1 year') -- используется три года, т.к. данных за последний месяц нет
			and ( -- подгружаем данные из новых записей
				dwh.f_orders.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) 
			 or dwh.d_products.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) 
			 or dwh.d_customers.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart)
			)
			group by craftsman_id) as b
		on a.craftsman_id = b.craftsman_id
			full outer join 
			(select craftsman_id, count(order_id) as "created_c"
				from dwh.f_orders
				where order_status = 'created'
				and order_created_date > CURRENT_TIMESTAMP - (3 * interval '1 year')
				and dwh.f_orders.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) -- подгружаем данные из новых записей
				group by craftsman_id) as c 
		on a.craftsman_id = c.craftsman_id
			full outer join 
			(select craftsman_id, count(order_id) as "delivery_c"
				from dwh.f_orders
				where order_status = 'delivery'
				and order_created_date > CURRENT_TIMESTAMP - (3 * interval '1 year')
				and dwh.f_orders.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) -- подгружаем данные из новых записей
				group by craftsman_id) as d 
		on a.craftsman_id = d.craftsman_id
			full outer join 
			(select craftsman_id, count(order_id) as "in_p_c"
				from dwh.f_orders
				where order_status = 'in progress'
				and order_created_date > CURRENT_TIMESTAMP - (3 * interval '1 year')
				and dwh.f_orders.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) -- подгружаем данные из новых записей
				group by craftsman_id) as e 
		on a.craftsman_id = e.craftsman_id
			full outer join 
			(select craftsman_id, count(order_id) as "done_c"
				from dwh.f_orders
				where order_status = 'done'
				and order_created_date > CURRENT_TIMESTAMP - (3 * interval '1 year')
				and dwh.f_orders.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) -- подгружаем данные из новых записей
				group by craftsman_id) as f 
		on a.craftsman_id = f.craftsman_id
			full outer join 
			(select craftsman_id, count(order_id) as "not_done_c"
				from dwh.f_orders
				where order_status != 'done'
				and order_created_date > CURRENT_TIMESTAMP - (3 * interval '1 year')
				and dwh.f_orders.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) -- подгружаем данные из новых записей
				group by craftsman_id) as g 
		on a.craftsman_id = g.craftsman_id
			join
			(select craftsman_id, product_type as "top_product" from
				(select craftsman_id, product_type, count(order_id) as cnt,
						dense_rank() over (partition by craftsman_id order by count(*) desc) as rnk -- используем ранжирование, чтобы определить самый частый продукт
				from dwh.f_orders 
					join dwh.d_products on  dwh.f_orders.product_id = dwh.d_products.product_id 
				where order_created_date > CURRENT_TIMESTAMP - (3 * interval '1 year')
				and (	-- подгружаем данные из новых записей
				   dwh.f_orders.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart) 
				or dwh.d_products.load_dttm::date > (select max(load_dttm) from dwh.load_dates_craftsman_report_datamart)
				)
				group by craftsman_id, product_type -- группируем по мастерам и типам продуктов
				order by craftsman_id, cnt)
			where rnk = 1) as h
		on a.craftsman_id = h.craftsman_id)
	on conflict (craftsman_id, report_period)
		do update set (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email,
					   craftsman_money, platform_money, count_order, avg_price_order, avg_age_customer, median_time_order_completed,
					   count_order_created, count_order_in_progress, count_order_delivery, count_order_done, count_order_not_done, top_product_category) = 
		(excluded.craftsman_name, excluded.craftsman_address, excluded.craftsman_birthday, excluded.craftsman_email,
		 excluded.craftsman_money, excluded.platform_money, excluded.count_order, excluded.avg_price_order, excluded.avg_age_customer, excluded.median_time_order_completed,
		 excluded.count_order_created, excluded.count_order_in_progress, excluded.count_order_delivery, excluded.count_order_done, excluded.count_order_not_done, excluded.top_product_category)
	

;
insert into dwh.load_dates_craftsman_report_datamart (load_dttm) values (CURRENT_TIMESTAMP)