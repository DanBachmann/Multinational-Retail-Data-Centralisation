-- 1) Which countries we currently operate in?
select country_code as country, count(1) as total_number_of_stores from dim_store_details
	where store_type <> 'Web Portal'
	group by country_code order by count(1) desc;
-- ...  and which country now has the most stores?
select country_code,count(1) from dim_store_details
	where store_type <> 'Web Portal'
	group by country_code order by count(1) desc limit 1;

-- 2) Which locations have the most stores?
select locality, count(1) as total_number_of_stores from dim_store_details
	where store_type <> 'Web Portal'
	group by locality order by count(1) desc limit 7;

-- 3) Which months have the most sales?
select
	p.currency,
	ROUND(cast(sum(p.product_price * o.product_quantity) as decimal), 2 ) as total_sales,
	--count(1) as total_orders,
	extract(month from date_timestamp) as month
	from orders_table o
		join dim_date_times dt on dt.date_uuid = o.date_uuid
		join dim_products p on p.product_code = o.product_code
	group by month, p.currency
	order by total_sales desc
	limit 6;

-- 4) Calculate how many products were sold and the amount of sales made for online and offline purchases.
select count(1) as number_of_sales,
	sum(product_quantity) as product_quantity_count,
	case when store_type='Web Portal' then 'Web' else 'Offline' end as location
	from orders_table o
	join dim_store_details s on s.store_code = o.store_code
	group by location;

-- 5) Find out the total and percentage of sales coming from each of the different store types.
with store_totals as (
	select store_type, count(1) as total_sales
		from orders_table o
		join dim_store_details s on s.store_code = o.store_code
group by store_type)
select store_type, total_sales, round(100*total_sales/(select sum(total_sales) from store_totals),2) as percentage_total
	from store_totals
group by store_type,total_sales
order by total_sales desc;

-- 6) which months in which years have had the most sales historically.
SELECT
	p.currency,
	round(cast(sum(p.product_price * o.product_quantity) as decimal), 2 ) as total_sales,
	--count(1) as total_orders,
	extract(year from date_timestamp) as year,
	extract(month from date_timestamp) as month
	from orders_table o
		join dim_date_times dt on dt.date_uuid = o.date_uuid
		join dim_products p on p.product_code = o.product_code
	group by year,month, p.currency
	order by total_sales desc
	limit 10;

-- 7) determine the staff numbers in each of the countries the company sells in.
select sum(staff_numbers) as total_staff_numbers, country_code
	from dim_store_details
	group by country_code
	order by total_staff_numbers desc;

-- 8) which type of store is generating the most sales in Germany.
select
	p.currency,
	round(cast(sum(p.product_price * o.product_quantity) as decimal), 2 ) as total_sales,
	--count(1) as total_orders,
	s.store_type,
	s.country_code
	from orders_table o
		join dim_date_times dt on dt.date_uuid = o.date_uuid
		join dim_products p on p.product_code = o.product_code
		join dim_store_details s on s.store_code = o.store_code
	where s.country_code = 'DE'
	group by s.store_type, s.country_code, p.currency
	order by total_sales;
	
-- 9) Determine the average time taken between each sale grouped by year, the query should return the following information:
with t_time_deltas as (select
					   extract(year from date_timestamp) as year,
	lead(date_timestamp,1)
	over (order by date_timestamp) -date_timestamp as time_delta
	 from dim_date_times
	  )
select year, avg(time_delta) as actual_time_taken from t_time_deltas
	group by year
	order by actual_time_taken desc
	limit 5;
/* getting a different value for seconds for most years - no more than 5 seconds difference from 'expected result'.
	 likely to be due to the date being converted in Python Pandas in my solution vs in SQL */
