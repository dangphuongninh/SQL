-- SOURCE: FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
select
  distinct (format_date("%Y%m", (parse_date("%Y%m%d", date)))) as month,
  count(visitId) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where _table_suffix between '0101' and '0331'
group by 1
order by 1;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
select
  trafficSource.source,
  sum(totals.visits) as total_visits,
  sum(totals.bounces) as total_no_of_bounces,
  round(sum(totals.bounces)/sum(totals.visits)*100,8) as bounce_rate
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by 1
order by 2 desc;


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
with month as(
  select
    'Month' as time_type,
    format_date("%Y%m", (parse_date("%Y%m%d", date))) as time,
    trafficSource.source as source,
    sum(totals.totalTransactionRevenue)/1000000 as revenue
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  group by 2, 3
  ),
week as (
  select
    'Week' as time_type,
    format_date("%Y%W", (parse_date("%Y%m%d", date))) as time, 
    trafficSource.source as source,
    sum(totals.totalTransactionRevenue)/1000000 as revenue
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  group by 2, 3
  )
  
select * from month
union all
select * from week
order by revenue desc;


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with purchaser as (
  select
    distinct (format_date("%Y%m", (parse_date("%Y%m%d", date)))) as month,
    round(sum(totals.pageviews)/count(distinct (fullVisitorId)),8) as avg_pageviews_purchase
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions >=1
  group by 1
  ),
non_purchaser as (
  select
    distinct (format_date("%Y%m", (parse_date("%Y%m%d", date)))) as month,
    round(sum(totals.pageviews)/count(distinct (fullVisitorId)),9) as avg_pageviews_non_purchase
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by 1
  )

select
  month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
from purchaser
left join non_purchaser
using (month)
order by 1;


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select
  format_date("%Y%m",(parse_date("%Y%m%d", date))) as month,
  round(sum(totals.transactions)/count(distinct (fullVisitorId)),9) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions >=1
group by 1;


-- Query 06: Average amount of money spent per session
#standardSQL
select
  format_date("%Y%m",(parse_date("%Y%m%d", date))) as month,
  round(sum(totals.totalTransactionRevenue)/count(distinct(visitId)),2) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where totals.transactions is not null
group by 1;


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
with purchaser as (
  select
    distinct (fullVisitorId)
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest (hits) as hits,
    unnest (product) as product
  where v2ProductName = "YouTube Men's Vintage Henley"
  and eCommerceAction.action_type = '6'
  ),
other_product as (
  select
    distinct (fullVisitorId),
    v2ProductName,
    sum(productQuantity) as quantity
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest (hits) as hits,
    unnest (product) as product
  where productRevenue is not null
  and eCommerceAction.action_type = '6'
  group by 1, 2
  )

select
  distinct(other_product.v2ProductName) as other_purchased_products,
  other_product.quantity
from purchaser
left join other_product
using (fullVisitorId)
order by 2 desc;


-- Cách khác:
with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with view as (
  select
    distinct (format_date("%Y%m", (parse_date("%Y%m%d", date)))) as month,
    count(v2ProductName) as num_product_view
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    unnest (hits) as hits,
    unnest (product) as product
  where _table_suffix between '0101' and '0331'
  and eCommerceAction.action_type = '2'
  group by 1
  ),
add as (
  select
    distinct (format_date("%Y%m", (parse_date("%Y%m%d", date)))) as month,
    count(v2ProductName) as num_addtocart
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    unnest (hits) as hits,
    unnest (product) as product
  where _table_suffix between '0101' and '0331'
  and eCommerceAction.action_type = '3'
  group by 1
  ),
purchase as (
  select
    distinct (format_date("%Y%m", (parse_date("%Y%m%d", date)))) as month,
    count(v2ProductName) as num_purchase
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    unnest (hits) as hits,
    unnest (product) as product
  where _table_suffix between '0101' and '0331'
  and eCommerceAction.action_type = '6'
  group by 1
  )

select
  view.month,
  view.num_product_view,
  add.num_addtocart,
  purchase.num_purchase,
  round((add.num_addtocart/view.num_product_view)*100,2) as add_to_cart_rate,
  round((purchase.num_purchase/view.num_product_view)*100,2) as purchase_rate
from view
left join add
on view.month = add.month
left join purchase
on view.month = purchase.month
order by 1;


-- Cách khác:
with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data