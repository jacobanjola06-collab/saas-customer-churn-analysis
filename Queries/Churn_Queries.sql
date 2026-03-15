-- KPI 
select "Total Customer" as measure_name, count(distinct account_id) as measure_value
from ravenstack_accounts
union all
select "Churned Customer" as measure_name,
       count(case when churn_flag = "true" then account_id end) as measure_value
from ravenstack_accounts
union all
select "churn Rate" as measure_name,
       round(count(case when churn_flag = "true" then account_id end) * 100/ count(*)) as measure_value
from  ravenstack_accounts
union all
select "Churned MRR",
round(SUM(mrr_amount))
from (select s.account_id, s.mrr_amount,
row_number() OVER( partition by  s.account_id order by  s.start_date DESC) as rn
from ravenstack_subscriptions s) last_sub
join ravenstack_accounts a
on last_sub.account_id = a.account_id
where rn = 1
and a.churn_flag = 'true'; 


-- yearly churn and retention rate 
select year(signup_date) as years,
       count(*) as total_customer, 
       count(case when churn_flag = "true" then account_id end) as churned_customer, 
       count(case when churn_flag = "true" then account_id end) * 100/ count(*) as churn_rate,
       count(case when churn_flag = "false" then account_id end) * 100/ count(*) as retention_rate
from  ravenstack_accounts
group by years; 

-- Customer segments
-- churn by industry
select industry, count(account_id) churned_customers
from ravenstack_accounts
where churn_flag = "true"
group by industry
order by churned_customers desc;

-- churn by country
select country, count(account_id) churned_customers
from ravenstack_accounts
where churn_flag = "true"
group by country
order by churned_customers desc;

-- trial vs paid customers 
select case when is_trial = "true" then "Trial user"
       else "paid user"
end as customer_type ,
count(*)  as total_customers,
count(case when churn_flag = "true" then account_id end) as churned_customers,
count(case when churn_flag = "true" then account_id end)/ count(*) * 100 as churned_rate
from ravenstack_accounts
group by customer_type;


-- new and returning customers churn 
select case 
       when timestampdiff(year, signup_date, curdate()) <=1 then 	"Newcustomers"
       else "Returning Customer"
end as customer_type,
       count(*) as customer,
       count(case when churn_flag = "true" then account_id end) as churned_customer, 
       count(case when churn_flag = "true" then account_id end) * 100/ count(*) as churn_rate
from ravenstack_accounts
group by customer_type;

-- reason for churn
create view churn_reason  as
select reason_code,c.account_id, count(distinct c.account_id) as churn_customers
from ravenstack_churn_events c 
join ravenstack_subscriptions s
on c.account_id = s.account_id
where churn_flag = "true"
group by reason_code, c.account_id
order by churn_customers desc;

select reason_code, sum(churn_customers) churned_customers
from churn_reason
group by reason_code;

-- which plan customers were on when they churned
select s.plan_tier, count(distinct a.account_id) as churned_customers
     from ravenstack_accounts a
join ravenstack_subscriptions s
on a.account_id = s.account_id
where a.churn_flag = "true"
and s.start_date =( select  max(s2.start_date)
        from ravenstack_subscriptions s2
        where s2.account_id = s.account_id) 
group by s.plan_tier
order by churned_customers desc;

-- What was the customer's last behaviour (upgrade, downgrade or no change) before they churned 
SELECT 
CASE
    WHEN s.upgrade_flag = 'true' THEN 'Upgraded '
    WHEN s.downgrade_flag = 'true' THEN 'Downgraded '
    ELSE 'No change'
END AS last_behaviour,
COUNT(DISTINCT a.account_id) AS total_customers,
COUNT(DISTINCT CASE 
    WHEN a.churn_flag = 'true' THEN a.account_id 
END) AS churned_customers
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
GROUP BY last_behaviour;


-- support ticket vs churn 
-- Do customer who contact support more often churn more 
SELECT 
CASE 
    WHEN ticket_count = 0 THEN 'No Tickets'
    WHEN ticket_count <= 2 THEN '1-2 Tickets'
    ELSE '3+ Tickets'
END AS ticket_volume,
COUNT(*) AS total_customers,
COUNT(CASE WHEN churn_flag = "TRUE" THEN 1 END) AS churned_customers,
ROUND(
COUNT(CASE WHEN churn_flag = "TRUE" THEN 1 END) / COUNT(*) * 100,
2) AS churn_rate
FROM (
    SELECT 
        a.account_id,
        a.churn_flag,
        COUNT(t.ticket_id) AS ticket_count
    FROM ravenstack_accounts a
    LEFT JOIN ravenstack_support_tickets t
    ON a.account_id = t.account_id
    GROUP BY a.account_id, a.churn_flag
) customer_tickets
GROUP BY ticket_volume;

SELECT 
CASE 
    WHEN t.satisfaction_score = 5 THEN '5 Satisfaction'
    WHEN t.satisfaction_score >= 3 THEN '3-4 Satisfaction'
else "No feedbaback"
END AS last_satisfaction,
COUNT(DISTINCT a.account_id) AS customers,
COUNT(DISTINCT CASE WHEN a.churn_flag = "TRUE" THEN a.account_id END) AS churned_customers
FROM ravenstack_accounts a
JOIN ravenstack_support_tickets t
ON a.account_id = t.account_id
JOIN (
    SELECT account_id, MAX(submitted_at) AS last_ticket
    FROM ravenstack_support_tickets
    GROUP BY account_id
) last_ticket
ON t.account_id = last_ticket.account_id
AND t.submitted_at = last_ticket.last_ticket
WHERE t.satisfaction_score IS NOT NULL
GROUP BY last_satisfaction;

-- Churn by billing frequency
SELECT 
s.billing_frequency,
COUNT(DISTINCT a.account_id) AS total_customers,
COUNT(DISTINCT CASE 
    WHEN a.churn_flag = 'true' THEN a.account_id
END) AS churned_customers,
COUNT(DISTINCT CASE 
    WHEN a.churn_flag = 'false' THEN a.account_id
END) AS retained_customers,
ROUND(
COUNT(DISTINCT CASE WHEN a.churn_flag='true' THEN a.account_id END) 
/ COUNT(DISTINCT a.account_id) * 100, 2
) AS churn_rate
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
GROUP BY s.billing_frequency;

-- churn by auto_renew
SELECT 
s.auto_renew_flag,
COUNT(DISTINCT a.account_id) AS total_customers,
cOUNT(DISTINCT CASE 
    WHEN a.churn_flag = 'true' THEN a.account_id
END) AS churned_customers,
COUNT(DISTINCT CASE 
    WHEN a.churn_flag = 'false' THEN a.account_id
END) AS retained_customers,
ROUND(
COUNT(DISTINCT CASE WHEN a.churn_flag='true' THEN a.account_id END) 
/ COUNT(DISTINCT a.account_id) * 100, 2
) AS churn_rate
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
GROUP BY s.auto_renew_flag;
