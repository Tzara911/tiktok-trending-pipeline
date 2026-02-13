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
from public.tiktok_rank_snapshots

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
from public.tiktok_rank_snapshots
where top_k=10
order by day desc, rank asc;
