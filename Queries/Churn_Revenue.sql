-- What is the overall revenue base
SELECT 
SUM(mrr_amount) AS total_mrr,
SUM(arr_amount) AS total_arr
FROM ravenstack_subscriptions;

-- Revenue lost due to churn
select 
round(SUM(mrr_amount)) as MRR_lost,
round(SUM(arr_amount)) as ARR_lost
from (select s.account_id, s.mrr_amount,arr_amount,
row_number() OVER( partition by  s.account_id order by  s.start_date DESC) as rn
from ravenstack_subscriptions s) last_sub
join ravenstack_accounts a
on last_sub.account_id = a.account_id
where rn = 1
and a.churn_flag = 'true';

-- Revenue lost by plan tier
SELECT 
s.plan_tier,
SUM(s.mrr_amount) AS lost_mrr
FROM ravenstack_accounts a
JOIN ravenstack_subscriptions s
ON a.account_id = s.account_id
JOIN (
    SELECT account_id, MAX(start_date) AS last_subscription
    FROM ravenstack_subscriptions
    GROUP BY account_id
) last_sub
ON s.account_id = last_sub.account_id
AND s.start_date = last_sub.last_subscription
WHERE a.churn_flag = 'true'
GROUP BY s.plan_tier
ORDER BY lost_mrr DESC;