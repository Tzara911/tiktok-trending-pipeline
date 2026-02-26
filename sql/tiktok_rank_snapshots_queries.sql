--TikTok Rank Snapshot Analysis Queries
--Table: public.tiktok_rank_snapshots
--Key colums assumed:
    -- captured_at, run_id, rank,top_k, product_id, title,rencent_sold _count,
    --totoal_sold_count, sale_amount,price,cpmmission_rate
--====================================

--Query 1  Daily Top-K leaderboard by day
select
   date_trunc('day', captured_at) as day,
   count(*) as rows,
   count(distinct run_id) as runs,
   min(top_k) as min_top_k,
   max(top_k) as max_top_k
from tiktok_rank_snapshots

group by 1 order by 1 desc;



--Query 2. Daily top 10 products
select
  date_trunc('day',captured_at) as day,
  rank,
  product_id,
  title,
  shop_name,
  recent_sold_count,
  total_sold_count,
  sale_amount,
  price
from tiktok_rank_snapshots
where top_k=10
order by day desc, rank asc;


---Query 3 Day-over-day rank change(negative = moved up, positive = moved down)
with daily as (
    select
        date_trunc('day',captured_at) as day,
        product_id as pid,
        max(title) as title,
        min(rank) as rank
    from tiktok_rank_snapshots
    where top_k =10
    group by 1, 2
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
    (prev_rank-rank) as rank_change
from diffs
where prev_rank is not null
order by day desc, abs(prev_rank-rank) desc;






--Query 4. Days on board + best rank +first/las seen

with daily as (
    select
        date_trunc('day',captured_at) as day,
        product_id as pid,
        max(title) as title,
        min(rank ) as rank
    from tiktok_rank_snapshots
    where top_k =10
    group by 1, 2
    
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
        date_trunc('day', captured_at) as day,
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
        date_trunc('day',captured_at) as day,
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
  d.pid,
  d.title,
  d.rank
  from pairs p join daily d on d.day=p.day
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
  prev.pid,
  prev.title,
  prev.rank
 from pairs p join daily prev on prev.day = p.prev_day
 where p.prev_day is not null
 and not exists(
  select 1 
  from daily cur where cur.day =p.day and cur.pid = prev.pid
 ) 
)
select 'entered' as change_type, * 
from entered
union all
select 'dropped' as change_type,*
from dropped
order by day desc, change_type, rank;






























































