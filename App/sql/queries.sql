--1. Show all customers from the `customers` table.
select * from customers c order by c.first_name, c.last_name;

--2. Show all products with a price above 500.
select * from products p where p.price > 500 order by p.price;

--3. List the 10 customers with the most loyalty points.
select * from customers c order by c.loyalty_points desc limit 10;

--4. Show all products in the “Electronics” category.
select * from products p where p.category like '%Electronics%' order by p.product_name, p.supplier;

--5. List all sales made in Lake (`store_location`).
select * from sales s where s.store_location like 'Lake%' order by s.sale_date;

--6. Show customers from the country “France” ordered by `last_name`.
select * from customers c where c.country like '%France%' order by c.last_name;

--7. List products with stock less than 100, ordered by `stock_quantity`.
select * from products p where p.stock_quantity < 100 order by p.stock_quantity;

--8. Show invalid sales.
select * from sales s where s.sale_is_valid = false;

--9. List all customers whose `email` is valid.
select * from customers c where c.is_email_valid = true;

--10. Show the 5 most recent valid sales.
select * from sales s where s.sale_is_valid = true order by s.sale_date desc limit 5;


-- Level 2:
--11. Count the total number of customers per country.
select c.country, count(*) as total_customers from customers c group by c.country order by 2 desc;

--12. Calculate the average loyalty points per country.
select c.country, ROUND(AVG(c.loyalty_points),2) from customers c group by c.country order by 2 desc;

--13. Count the number of products per category.
select p.category, count(p.category) from products p group by p.category order by 2 desc;

--14. Calculate the average price of products per category.
select p.category, ROUND(avg(p.price),2) from products p group by p.category order by 2 desc;

--15. Sum the quantity of products sold by `product_id`.
select s.product_id, sum(s.quantity) as total_quantity from sales s group by product_id order by 2 desc;
/* common mistake: confusing sum with count */

--16. Calculate the total sales value per `customer_id`.
select s.customer_id, SUM(p.price * s.quantity) as total_sale 
from sales s 
left join products p on s.product_id = p.product_id 
where s.sale_is_valid 
group by 1 order by s.customer_id;

--17. Show the number of valid and invalid sales per store (`store_location`).
with valid_sales as (
    select s.store_location, count(1) as valid_sales 
    from sales s where s.sale_is_valid 
    group by 1 
),
invalid_sales as (
    select s.store_location, count(1) as invalid_sales 
    from sales s where s.sale_is_valid = false 
    group by 1
)
select v.store_location, v.valid_sales, i.invalid_sales 
from valid_sales v 
left join invalid_sales i on v.store_location = i.store_location 
order by v.valid_sales desc;

--18. List customers with more than 200 loyalty points.
select c.first_name, c.last_name, c.loyalty_points  
from customers c 
where c.loyalty_points > 200 
order by 3 desc;

--19. Calculate total sales per month (`sale_month`).
select s.sale_month, count(*) 
from sales s 
where s.sale_is_valid 
group by s.sale_month 
order by s.sale_month;

--20. List products with total stock less than 50.
select * from products p where p.stock_quantity < 50;


-- Level 3 – Advanced (JOINs and subqueries)
--21. Show the customer name and product name of all sales (JOIN `sales` + `customers` + `products`).
select concat(c.first_name, ' ', c.last_name) as full_name, p.product_name, s.* 
from sales s 
left join customers c on s.customer_id = c.customer_id 
left join products p on s.product_id = p.product_id;

--22. List all sales with `quantity > 1` showing customer and product names.
select concat(c.first_name, ' ', c.last_name) as full_name, p.product_name, s.* 
from sales s 
left join customers c on s.customer_id = c.customer_id 
left join products p on s.product_id = p.product_id
where s.quantity > 1;

--23. Show customers who bought products in the “Electronics” category.
select distinct c.* 
from sales s 
left join customers c on s.customer_id = c.customer_id 
left join products p on s.product_id = p.product_id
where p.category = 'Electronics'
order by c.customer_id;

--24. List products that were never sold.
with product_sales as (
    select distinct s.product_id from sales s
)
select * 
from products p 
where p.product_id not in (select ps.product_id from product_sales ps);

-- Alternative:
SELECT *
FROM products p
WHERE NOT EXISTS (
    SELECT 1
    FROM sales s
    WHERE s.product_id = p.product_id
);

--25. Show the total amount spent by each customer.
with sales_total as (
    select s.*, p.price * s.quantity as total_sale
    from sales s
    left join products p on s.product_id = p.product_id
    where s.quantity > 0
)
select st.customer_id, sum(total_sale) as total_spent 
from sales_total st
group by 1 
order by 2;

--26. List sales where the customer has less than 100 loyalty points.
select * 
from sales s 
left join customers c on s.customer_id = c.customer_id
where c.loyalty_points < 100;

--27. Show the name of the best-selling product (total quantity).
with best_selling as (
    select s.product_id, SUM(s.quantity) as total_quantity  
    from sales s 
    group by 1  
    order by 2 desc 
    limit 1
)
select p.product_name  
from products p 
where p.product_id = (select bs.product_id from best_selling bs);

-- or

WITH best_selling AS (
    SELECT 
        s.product_id,
        SUM(s.quantity) AS total_quantity
    FROM sales s
    GROUP BY s.product_id
    ORDER BY total_quantity DESC
    LIMIT 1
)
SELECT p.product_name
FROM products p
JOIN best_selling bs ON p.product_id = bs.product_id;

--28. List customers who bought more than 2 different products.
with customer_sales as (
    select distinct s.customer_id, s.product_id from sales s order by s.customer_id 
),
number_sales as (
    select cs.customer_id, count(*) as number_sales from customer_sales cs group by 1
)
select * from customers c 
where c.customer_id in (
    select ns.customer_id from number_sales ns where ns.number_sales > 2
);

-- or 
WITH customer_sales AS (
    SELECT s.customer_id, COUNT(DISTINCT s.product_id) AS num_products
    FROM sales s
    GROUP BY s.customer_id
)
SELECT c.*
FROM customers c
JOIN customer_sales cs ON c.customer_id = cs.customer_id
WHERE cs.num_products > 2;

--29. Show all sales made in the last month with the customer name.
-- selected month 10 and year 2022.
select concat(c.first_name, ' ', c.last_name) as full_name, s.* 
from sales s 
left join customers c on s.customer_id = c.customer_id
where s.sale_month = 10 and s.sale_year = 2022;

--30. List products sold along with the total quantity sold, ordered from highest to lowest.
with product_sales as (
    select s.product_id, sum(s.quantity) total_sales 
    from sales s
    group by 1
)
select p.*, COALESCE(ps.total_sales, 0) AS total_sales 
from products p 
left join product_sales ps on p.product_id = ps.product_id
order by total_sales desc;


-- Level 4 – Expert/Challenge (CTEs, window functions, ranking, KPIs)

--31. Create a ranking of customers by total sales (`ROW_NUMBER()`).
with total_customer as (
	select 
		s.customer_id,
		SUM(p.price * s.quantity) as total_sale 
	from sales s 
	left join products p 
		on s.product_id = p.product_id 
	where s.sale_is_valid 
	group by 1
)
select 
	ROW_NUMBER() OVER (ORDER BY tc.total_sale DESC) AS customer_rank,
	tc.total_sale,
	c.first_name, c.last_name, c.country, c.gender, c.email_clean, c.birth_date_clean, c.loyalty_points
from total_customer tc
left join customers c 
	on tc.customer_id = c.customer_id 
order by customer_rank;

--32. Show the 5 best-selling products.
with total_sales as (
	select 
		s.product_id, 
		count(*) as number_sales
	from sales s 
	group by 1
),
sales_rank as (
	select DENSE_RANK() OVER(order by number_sales desc) as product_rank,
	p.*
	from total_sales ts
	left join products p 
	on ts.product_id = p.product_id
)
select * from sales_rank sr where sr.product_rank <= 5;

--33. Calculate the average sales per customer and list customers above the average.
with total_sales_customer as (
	select c.customer_id, sum(s.quantity*p.price) as total_value_sale 
	from sales s
	left join customers c 
		on s.customer_id = c.customer_id
	left join products p 
		on s.product_id = p.product_id
	where s.sale_is_valid
	group by c.customer_id 
	order by 2 asc
)
select c.*, tsc.total_value_sale  
from total_sales_customer tsc
left join customers c 
	on tsc.customer_id = c.customer_id
where tsc.total_value_sale > (select avg(tsc.total_value_sale) from total_sales_customer tsc) 
order by tsc.total_value_sale;

--34. List customers who made no sales.
select * from customers c 
where c.customer_id not in (
	select distinct s.customer_id from sales s
);

--35. Show the total sales per category using CTEs.
-- simpler version:
select p.category, count(*) as num_sales
from sales s
join products p 
	on s.product_id = p.product_id
group by p.category;

-- or as requested:
WITH sales_with_category AS (
    SELECT 
        s.sale_id,
        p.category
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
)
SELECT 
    category, 
    COUNT(*) AS num_sales
FROM sales_with_category
GROUP BY category;
	
--36. List the products with the lowest and highest price in each category.
with ranked_products as (
    select 
        category,
        product_name,
        price,
        rank() over (partition by category order by price asc) as rank_min,
        rank() over (partition by category order by price desc) as rank_max
    from products
)
select *
from ranked_products
where rank_min = 1 or rank_max = 1
order by category, price;

--37. Calculate total sales per country assuming the purchase location is the same as the customer's country.
with country_sales as (
select c.country, s.quantity * p.price as total_sale 
from sales s 
left join customers c 
	on s.customer_id = c.customer_id
left join products p 
	on s.product_id = p.product_id 
where s.sale_is_valid
and c.country <> 'Unknown'
)
select cs.country, sum(cs.total_sale) as total_sale_by_country
from country_sales cs
group by cs.country
order by 2 desc;

--38. List sales with the highest value per store (`sale_quantity * price`).
with total_value_sales as (
	select s.*, s.quantity * p.price as total_sale
	from sales s 
	left join customers c 
		on s.customer_id = c.customer_id
	left join products p 
		on s.product_id = p.product_id 
	where s.sale_is_valid
),
rank_store as (
select rank() over (partition by tvs.store_location  order by tvs.total_sale desc) as rank_store, tvs.* from total_value_sales tvs
)
select rs.* from rank_store rs where rs.rank_store = 1;

--39. Ranking of customers by number of products purchased (`RANK()` or `DENSE_RANK()`).
with total_customer as (
	select 
		s.customer_id,
		count(1) as number_sales
	from sales s 
	left join products p 
		on s.product_id = p.product_id 
	where s.sale_is_valid 
	group by 1
	order by 2 desc
)
select 
	DENSE_RANK() OVER (ORDER BY tc.number_sales DESC) AS customer_rank,
	c.first_name, c.last_name, c.country, c.gender, c.email_clean, c.birth_date_clean, c.loyalty_points,
	tc.number_sales
from total_customer tc
left join customers c 
	on tc.customer_id = c.customer_id 
order by customer_rank;

--40. Show months with the highest and lowest total sales.
with sales_month as (
select s.sale_month, sum(s.quantity * p.price) as total_sale
from sales s
left join products p 
		on s.product_id = p.product_id
where s.sale_is_valid
group by s.sale_month
),
rank_sales as (
select *, 
rank() over (order by total_sale asc) as rank_min,
rank() over (order by total_sale desc) as rank_max
from sales_month sm
)
select sale_month, total_sale from rank_sales where rank_min = 1 or rank_max = 1;
