SELECT * FROM sales.product;
SELECT * FROM sales.customer;
SELECT * FROM sales.sales;
-- 1.	List all products with their categories and prices.
SELECT * FROM sales.product;

-- 2.	Get names of all customers from the city 'Mumbai'.
select *from customer 
where city="Mumbai";
-- 3.	List all sales records with buy and sell dates.
select sales_id, product_id, customer_id,   buy_date, sell_date from sales;

-- 4.	Find the total number of customers.
select  count(*) AS total_customer
from Customer;
-- 5.	Get the total quantity sold for each product.
select product_id,quantity from sales;

-- 6.	Show the distinct cities where customers live.
select customer_name ,city from customer ;
 
 -- 7.	Get customer names and phones who bought a product.
 select s.product_id,s.quantity,c.customer_name, c.phone 
 from customer c
 join sales  s on s.customer_id=c.customer_id ;

 -- 8.	List all sales where quantity > 2.
SELECT product_id,quantity
 FROM sales
 where quantity >2

 ;
 -- 9.	Get total sales transactions per customer.
 select c.customer_name ,SUM((s.buy_price +s.sell_price)*quantity) as total_Transaction
 from customer c 
 join sales s  on c.customer_id=s.customer_id
 group by c.customer_name
 Order By total_Transaction DESC;
 -- 10.	Show product name and corresponding quantity sold using JOIN
 select p.product_name,s.quantity 
 from product p
 join sales s on p.product_id=s.product_id

 order by  quantity asc; 
                              -- 🔸 30   Advanced SQL Questions
# Aggregation & Grouping
-- 11.	Find the total profit (sell_price - buy_price) for each sale.
select sales_id,(sell_price-buy_price) as profit  from sales;

-- 12.	Calculate the average sell price per product category.
WITH cte1 AS (
    SELECT p.product_id, p.category, s.sell_price 
    FROM product p
    LEFT JOIN sales s ON p.product_id = s.product_id
)
SELECT 
    category,
    SUM(sell_price) * 1.0 / COUNT(sell_price) AS average_sell_price
FROM cte1
GROUP BY category;

-- 13.	Get the total revenue (sell_price × quantity) per product.
SELECT 
    p.product_name,
    SUM(s.quantity * s.sell_price) AS revenue
FROM sales s
JOIN product p ON s.product_id = p.product_id
GROUP BY p.product_name;
-- 14.	Which customer made the highest number of purchases?
SELECT 
    c.customer_name, 
    COUNT(s.sales_id) AS total_purchases
FROM sales s
JOIN customer c ON s.customer_id = c.customer_id
GROUP BY c.customer_name
ORDER BY total_purchases DESC
LIMIT 1;

-- 15.	List products never sold.
SELECT p.product_id, p.product_name
FROM product p
LEFT JOIN sales s ON p.product_id = s.product_id
WHERE s.product_id IS NULL;

-- 16.	Find the top 3 cities by number of customers.
SELECT 
    city, 
    COUNT(customer_id) AS total_customers
FROM customer
GROUP BY city
ORDER BY total_customers DESC
LIMIT 3;


-- 17.	Show the monthly sales volume (based on sell_date).
SELECT 
    DATE_FORMAT(STR_TO_DATE(sell_date, '%m/%d/%Y'), '%Y-%m') AS sale_month,
    SUM(quantity) AS total_sales_volume
FROM sales
GROUP BY DATE_FORMAT(STR_TO_DATE(sell_date, '%m/%d/%Y'), '%Y-%m')
ORDER BY sale_month;
-- 18.	Find the average profit per customer.
SELECT 
    c.customer_id,
    c.customer_name,
    round(AVG((s.sell_price - s.buy_price) * s.quantity),2) AS average_profit
FROM customer c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.customer_name
order by  average_profit desc;


-- 19.	Show product-wise profit margins as percentage.
SELECT 
s.product_id,
p.product_name,
    ROUND(AVG((s.sell_price - s.buy_price) * 100.0 / s.buy_price), 2) AS avg_percentage_profit
FROM product p
JOIN sales s ON p.product_id = s.product_id
GROUP BY p.product_name, s.product_id
ORDER BY avg_percentage_profit DESC;

-- 20.	Find customers who bought more than 3 products in total.
SELECT 
    customer_id, 
    SUM(quantity) AS total_purchase_quantity
FROM sales
GROUP BY customer_id
HAVING total_purchase_quantity> 3
ORDER BY total_purchase_quantity DESC;


                                             -- Joins & Multi-table Queries
-- 21.	Get the product name, customer name, and profit for each sale.
SELECT 
    c.customer_name,
    p.product_name,
    (s.sell_price - s.buy_price) * s.quantity AS total_profit
FROM sales s
JOIN customer c ON s.customer_id = c.customer_id
JOIN product p ON s.product_id = p.product_id;



-- 22.	List customers who bought Electronics products.
SELECT DISTINCT c.customer_name
FROM sales s
JOIN product p ON p.product_id = s.product_id
JOIN customer c ON c.customer_id = s.customer_id
WHERE p.category = 'Electronics';


-- 23.	Find the total profit for each category.
select p.category, sum((s.sell_price-s.buy_price)*quantity) as profit
from sales s
join product p
on p.product_id=s.product_id
group by p.category
order by profit desc;

-- 24.	Get customer name and number of unique products bought.
SELECT 
    c.customer_name,
    COUNT(DISTINCT s.product_id) AS unique_products_bought
FROM customer c
JOIN sales s ON s.customer_id = c.customer_id
GROUP BY c.customer_name;
-- 25.	Find customers who bought more than one product in one sale.
SELECT 
    c.customer_name,
    s.sales_id,
    s.product_id,
    s.quantity
FROM sales s
JOIN customer c ON c.customer_id = s.customer_id
WHERE s.quantity > 1;



 -- 26.	Show sales with price > buy price (from Product).
 SELECT 
    s.sales_id,
    s.product_id,
    s.sell_price,
    p.price AS listed_price
FROM sales s
JOIN product p ON s.product_id = p.product_id
WHERE p.price > s.buy_price;

-- 27.	Find the products bought by customers from 'Delhi'.
select p.product_name
from product p
join sales s
on p.product_id=s.product_id
join customer c
on c.customer_id=s.customer_id
where c.city="Delhi";
-- 28.	List customers who have never made a purchase.
SELECT c.customer_name
FROM customer c
LEFT JOIN sales s ON c.customer_id = s.customer_id
WHERE s.sales_id IS NULL;



-- 29.	Show latest sale for each product.
SELECT s.*
FROM sales s
JOIN (
    SELECT product_id, MAX(sell_date) AS latest_date
    FROM sales
    GROUP BY product_id
) latest_sales
ON s.product_id = latest_sales.product_id AND s.sell_date = latest_sales.latest_date;

-- 30.	Display customer with highest total spending.
select c.customer_name ,sum(s.buy_price*s.quantity)as total_spending
from customer c
join sales s
on c.customer_id=s.customer_id
group by c.customer_name
order by total_spending desc;



                                        -- Subqueries & Window Functions
--  31: Find products with above-average selling price
SELECT 
    p.product_name,
    ROUND(AVG(s.sell_price), 2) AS avg_sell_price
FROM 
    product p
JOIN 
    sales s ON s.product_id = p.product_id
GROUP BY 
    p.product_name
HAVING 
    AVG(s.sell_price) > (
        SELECT AVG(sell_price) 
        FROM sales
    );



--  32: Show profit of each sale and rank them in descending order
SELECT 
    
    product_id,
    (sell_price - buy_price) * quantity AS profit,
    RANK() OVER (ORDER BY (sell_price - buy_price) * quantity DESC) AS profit_rank,
    DENSE_RANK() OVER (ORDER BY (sell_price - buy_price) * quantity DESC) AS profit_dense_rank
FROM 
    sales;


-- 33: List top 2 most sold products per category
WITH cte1 AS (
    SELECT 
        p.category,
        p.product_id,
        p.product_name,
        SUM(s.quantity) AS total_quantity,
        DENSE_RANK() OVER (PARTITION BY p.category ORDER BY SUM(s.quantity) DESC) AS rnk
    FROM 
        sales s
    JOIN 
        product p ON p.product_id = s.product_id
    GROUP BY 
        p.category, p.product_id, p.product_name
)
SELECT 
    category,
    product_name,
    total_quantity
FROM 
    cte1
WHERE 
    rnk <= 2;

-- 34: Find repeat customers (who bought more than once)
SELECT 
    c.customer_id,
    c.customer_name,
    COUNT(s.sales_id) AS total_purchases
FROM 
    sales s
JOIN 
    customer c ON s.customer_id = c.customer_id
GROUP BY 
    c.customer_id, c.customer_name
HAVING 
    COUNT(s.sales_id) > 1;


-- 35.	Show the running total of sales revenue per customer.

SELECT 
    s.customer_id,
    c.customer_name,
    s.sales_id,
    STR_TO_DATE(s.sell_date, '%Y-%m-%d') AS sell_date_converted,
    (s.sell_price * s.quantity) AS sale_revenue,
    SUM(s.sell_price * s.quantity) OVER (
        PARTITION BY s.customer_id
        ORDER BY STR_TO_DATE(s.sell_date, '%Y-%m-%d')
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM 
    sales s
JOIN 
    customer c ON s.customer_id = c.customer_id
ORDER BY 
    s.customer_id, STR_TO_DATE(s.sell_date, '%Y-%m-%d');



-- 36.	Calculate days between buy_date and sell_date per sale.
SELECT  
    sales_id,
    STR_TO_DATE(buy_date, '%m/%d/%Y') AS formatted_buy_date,
    STR_TO_DATE(sell_date, '%m/%d/%Y') AS formatted_sell_date,
    DATEDIFF(
        STR_TO_DATE(sell_date, '%m/%d/%Y'),
        STR_TO_DATE(buy_date, '%m/%d/%Y')
    ) AS days_between
FROM sales;


-- 37.	Get sales that happened in the last 7 days.
SELECT *
FROM sales
WHERE STR_TO_DATE(sell_date, '%m/%d/%Y') >= DATE_SUB(CURDATE(), INTERVAL 7 DAY);

-- 38.	List products where sell price never exceeded listed price.
SELECT 
    p.product_id,
    p.product_name
FROM product p
JOIN (
    SELECT product_id, MAX(sell_price) AS max_sell_price
    FROM sales
    GROUP BY product_id
) s_max ON p.product_id = s_max.product_id
WHERE s_max.max_sell_price <= p.price;

-- 39.	Compare buy and sell dates to find same-day sales.
SELECT 
    sales_id,
    product_id,
    customer_id,
    buy_date,
    sell_date
FROM sales
WHERE STR_TO_DATE(buy_date, '%m/%d/%Y') = STR_TO_DATE(sell_date, '%m/%d/%Y');

-- 40.	Identify best selling product category using revenue.
SELECT 
    p.category,p.product_name,
    SUM(s.sell_price * s.quantity) AS total_revenue
FROM product p
JOIN sales s ON p.product_id = s.product_id
GROUP BY p.category,p.product_name
ORDER BY total_revenue DESC
LIMIT 1;
                                      --                      Complex Logic & CTEs
--  41.	Use a CTE to calculate monthly revenue trend.
WITH monthly_sales AS (
    SELECT 
        DATE_FORMAT(STR_TO_DATE(s.sell_date, '%m/%d/%Y'), '%Y-%m') AS sale_month,
        s.sell_price * s.quantity AS revenue
    FROM sales s
)
SELECT 
    sale_month,
    SUM(revenue) AS total_monthly_revenue
FROM monthly_sales
GROUP BY sale_month
ORDER BY sale_month;


-- 42.	Use a CASE statement to label profit as 'Low', 'Medium', 'High' also count.
WITH cte1 AS (
    SELECT 
        sales_id,
        (sell_price - buy_price) * quantity AS profit,
        CASE 
            WHEN (sell_price - buy_price) * quantity < 20 THEN 'Low'
            WHEN (sell_price - buy_price) * quantity BETWEEN 20 AND 40 THEN 'Medium'
            ELSE 'High'
        END AS profit_label
    FROM sales
)
SELECT 
    profit_label,
    COUNT(*) AS count_label
FROM cte1
WHERE profit_label IN ('High',"Medium", 'Low')
GROUP BY profit_label;




-- 43.	Use a nested query to find customer with most profit generated.
SELECT customer_name, profit
FROM (
    SELECT 
        c.customer_name,
        SUM((s.sell_price - s.buy_price) * s.quantity) AS profit
    FROM sales s
    JOIN customer c ON c.customer_id = s.customer_id
    GROUP BY c.customer_name
) AS customer_profits
ORDER BY profit DESC
LIMIT 1;

-- 44.	Create a temporary table showing total units sold by category.
-- Step 1: Create the temporary table
CREATE TEMPORARY TABLE temp_units_sold_by_category AS
SELECT 
    p.category,
    SUM(s.quantity) AS total_units_sold
FROM product p
JOIN sales s ON p.product_id = s.product_id
GROUP BY p.category;
-- view table
SELECT * FROM temp_units_sold_by_category;

-- 45.	Write a query to segment customers into low, mid, high value based on total spend.
SELECT 
    c.customer_name,
    SUM(s.buy_price * s.quantity) AS total_spend,
    CASE
        WHEN SUM(s.buy_price * s.quantity) < 450 THEN 'Low'
        WHEN SUM(s.buy_price * s.quantity) BETWEEN 450 AND 1500 THEN 'Mid'
        ELSE 'High'
    END AS customer_segment
FROM customer c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_name
ORDER BY total_spend DESC;

-- 46.	Find how many times each product was resold.
SELECT 
    p.product_name,
    COUNT(s.sales_id) AS times_sold
FROM product p
JOIN sales s ON p.product_id = s.product_id
GROUP BY p.product_name
ORDER BY times_sold DESC;

-- 47.	List products where quantity sold per transaction > 2 consistently.
SELECT 
    p.product_id,
    p.product_name
FROM product p
JOIN sales s ON p.product_id = s.product_id
GROUP BY p.product_id, p.product_name
HAVING MIN(s.quantity) > 2;

-- 48.	Find the least profitable product.
SELECT 
    p.product_id,
    p.product_name,
    SUM((s.sell_price - s.buy_price) * s.quantity) AS total_profit
FROM product p
JOIN sales s ON p.product_id = s.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_profit ASC
LIMIT 1;


-- 49.	Use window functions to get cumulative quantity sold per product.
SELECT 
    p.product_id,
    p.product_name,
    s.sales_id,
    s.quantity,
    SUM(s.quantity) OVER (PARTITION BY p.product_id ORDER BY s.sell_date) AS cumulative_quantity
FROM product p
JOIN sales s ON p.product_id = s.product_id
ORDER BY p.product_id, s.sell_date;

/*50.	Generate a report showing:
o	customer_name,
o	product_name,
o	profit %,
o	total quantity bought,
o	days between buy and sell date.*/
SELECT 
    c.customer_name,
    p.product_name,
    ROUND(((s.sell_price - s.buy_price) / s.buy_price) * 100, 2) AS profit_percent,
    s.quantity AS total_quantity_bought,
    DATEDIFF(STR_TO_DATE(s.sell_date, '%m/%d/%Y'), STR_TO_DATE(s.buy_date, '%m/%d/%Y')) AS days_between
FROM sales s
JOIN customer c ON s.customer_id = c.customer_id
JOIN product p ON s.product_id = p.product_id
ORDER BY c.customer_name, p.product_name;




 
 


