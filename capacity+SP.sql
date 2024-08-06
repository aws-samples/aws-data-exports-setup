SELECT DISTINCT
product_product_code
, bill_payer_account_id
, line_item_resource_id
, line_item_usage_account_id
, line_item_availability_zone
, product_instance_type
, product_operating_system
, product_tenancy
, CASE
   WHEN lower(product_usagetype) LIKE '%reservation%' THEN 'Reservation'
   WHEN lower(product_usagetype) LIKE '%dedicatedres%' THEN 'Reservation'
   WHEN lower(product_usagetype) LIKE '%unusedbox%' THEN 'Unused'
   WHEN lower(product_usagetype) LIKE '%unusedded%' THEN 'Unused'
   ELSE '*Error check SQL*' END AS "Reservation Type"
, line_item_line_item_type
, line_item_usage_type
, line_item_usage_start_date
, SUM(line_item_usage_amount) AS "Hourly Usage (Total)"
, SUM(line_item_unblended_cost) AS "Unblended Cost (Total)"
, sum(reservation_effective_cost) as "RI Effective Cost"
, sum(savings_plan_savings_plan_effective_cost) as "SP Effective Cost"
FROM 
customer_all 
WHERE 
(line_item_usage_start_date BETWEEN TIMESTAMP '2020-10-01 00:00:00' AND TIMESTAMP '2021-01-31 23:59:59')
  AND product_product_code = 'AmazonEC2'
  AND line_item_resource_id LIKE '%capacity-reservation%'
GROUP BY 
1,2,3,4,5,6,7,8,9,10,11
Limit
10
-- that should get the OD and SPCovered cost of an ODCR for x timeframe