--TikTok Rank Snapshot Analysis Queries
--Table: public.tiktok_rank_snapshots
--Key colums assumed:
    -- captured_at, run_id, rank,top_k, product_id, title,rencent_sold _count,
    --totoal_sold_count, sale_amount,price,cpmmission_rate
--====================================

--Query 1  Daily Top-K leaderboard by day
select
   day,
   count(*) as rows,
   count(distinct run_id) as runs,
   min(top_k) as min_top_k,
   max(top_k) as max_top_k
from tiktok_rank_snapshots

group by day order by day desc;



--Query 2. Daily top 10 products
select
  day,
  rank,
  product_id,
  title,
  shop_name,
  recent_sold_count,
  total_sold_count,
  sale_amount,
  price,
  commission_rate
from tiktok_rank_snapshots
where top_k=10
order by day desc, rank asc;


---Query 3 Day-over-day rank change(negative = moved down, positive = moved up)
with daily as (
    select
        day,
        product_id as pid,
        max(title) as title,
        min(rank) as rank
    from tiktok_rank_snapshots
    where top_k =10
    group by day, product_id
),
diffs as (
    select 
        day,
        pid,
        title,
        rank,
        lag(rank) over(partition by pid order by day) as prev_rank
    from daily
)
select 
    day, 
    pid as product_id,
    title,
    prev_rank,
    rank as current_rank,
    case
        when  (prev_rank-rank) > 0 then '+' ||  (prev_rank-rank) ::text
        when  (prev_rank-rank) < 0 then  (prev_rank-rank) ::text
        else '0'
    end as rank_change
from diffs
where prev_rank is not null
order by day desc, abs(prev_rank-rank) desc;






--Query 4. Days on board + best rank +first/last seen

with daily as (
    select
        day,
        product_id as pid,
        max(title) as title,
        min(rank ) as rank
    from tiktok_rank_snapshots
    where top_k =10
    group by day, product_id
    
)
select 
    pid as product_id,
    max(title) as title,
    count(*) as days_on_board,
    min(rank) as best_rank,
    min(day) as first_seen_day,
    max(day) as last_seen_day
from daily
group by pid
order by days_on_board desc, best_rank asc;




---Query 5. Day- over_day sales and revenue change

with daily as(
    select
        day,
        product_id as pid,
        max(title) as title,
        max(recent_sold_count ) as recent_sold_count,
        max(sale_amount) as sale_amount
    from tiktok_rank_snapshots
    where top_k=10
    group by 1, 2
    ),
diffs as (
    select
        day,
        pid,
        title,
        recent_sold_count,
        sale_amount,
        lag(recent_sold_count) over(partition by pid order by day) as prev_recent_sold,
        lag(sale_amount) over (partition by pid order by day) as prev_sale_amount
    from daily
)
select
    day,
    pid as product_id,
    title,
    prev_recent_sold,
    recent_sold_count,
    (recent_sold_count - prev_recent_sold) as sold_change,
    prev_sale_amount,
    sale_amount,
    (sale_amount - prev_sale_amount) as revenue_change
from diffs
where prev_recent_sold is not null
 order by day desc, revenue_change desc nulls last;




---Query 6.  New entries and drop-offs between days

with daily as (
    select distinct
        day,
        product_id as pid,
        title,
        rank
    from tiktok_rank_snapshots
    where top_k = 10
), 
pairs as (
    select
        day,
        lag(day) over(order by day) as prev_day
    from (select distinct day from daily ) d
    
),

entered as (
  select
  p.day,
  d.pid as product_id,
  d.title,
  d.rank,
    1 as sort_key
  from pairs p 
    join daily d on d.day=p.day
  where p.prev_day is not null
  and not exists(
    select 1
    from daily prev
    where prev.day = p.prev_day and prev.pid = d.pid
  )
),
dropped as(
 select
  p.day,
  prev.pid as product_id,
  prev.title,
  prev.rank,
  2 as sort_key
 from pairs p
    join daily prev on prev.day = p.prev_day
 where p.prev_day is not null
 and not exists(
  select 1 
  from daily cur 
    where cur.day =p.day and cur.pid = prev.pid
 ) 
)
select 
    change_type,
    day,
    product_id,
    title,
    rank
    
from (
select 'entered' as change_type, day, product_id, title, rank,sort_key from entered
union all
select 'dropped' as change_type, day,product_id, title,rank, sort_key from dropped
)x
order by day desc, sort_key asc, rank asc;


-- Q7: Time on board more than 1 day (from first collected day to latest day)
with base as (
  select
    day,
    product_id,
    max(title) as title,
    min(rank) as rank,
    max(recent_sold_count) as recent_sold_count
  from tiktok_rank_snapshots
  where top_k = 10
  group by day, product_id
),
summary as (
  select
    product_id,
    max(title) as title,
    count(*) as days_on_board,
    sum(recent_sold_count) as totl_recent_sold_count,
    max(recent_sold_count) as max_recent_sold_count,
    min(rank) as best_rank,
    avg(rank)::numeric(10,2) as avg_rank,
    min(day) as first_seen_day,
    max(day) as last_seen_day
  from base
  group by product_id
)
select *
from summary
where days_on_board > 1
order by days_on_board desc, best_rank asc, avg_rank asc;


--Q8: One-day appearances only
with base as (
  select
    day,
    product_id,
    max(title) as title,
    min(rank) as rank,
    max(recent_sold_count) as recent_sold_count
  from tiktok_rank_snapshots
  where top_k = 10
  group by day, product_id
),
summary as (
  select
    product_id,
    max(title) as title,
    count(*) as days_on_board,
    sum(recent_sold_count) as totl_recent_sold_count,
    
    min(rank) as best_rank,
    avg(rank)::numeric(10,2) as avg_rank,
    min(day) as first_seen_day,
    max(day) as last_seen_day
  from base
  group by product_id
)
select *
from summary
where days_on_board = 1
order by first_seen_day desc, best_rank asc;

-- Q9: Category-level daily trend
SELECT
  day,
  category,
  COUNT(DISTINCT product_id) AS unique_products,
  SUM(recent_sold_count) AS total_recent_sold,
  AVG(rank)::numeric(10,2) AS avg_rank,
  SUM(sale_amount) AS total_revenue
FROM tiktok_rank_snapshots
WHERE top_k = 10
GROUP BY day, category
ORDER BY day DESC, total_recent_sold DESC;


























































