create view customer_churn_behavior AS
select  a.account_id, a.country,
       a.industry,a.plan_tier,
        a.is_trial,a.churn_flag,
	    s.plan_tier AS last_plan_tier,
       s.billing_frequency, s.auto_renew_flag,
       s.upgrade_flag, s.downgrade_flag,
case
    when coalesce(t.ticket_count,0) = 0 then 'No Tickets'
    when coalesce(t.ticket_count,0) between 1 and 2 then '1-2 Tickets'
    else '3+ Tickets'
end as ticket_volume
from ravenstack_accounts a
left join
( select
        account_id,
        COUNT(ticket_id) AS ticket_count
from ravenstack_support_tickets
group by account_id
) t
on a.account_id = t.account_id
left join
(select *
from(
select subscription_id, account_id,
        plan_tier, billing_frequency,
        auto_renew_flag, upgrade_flag,
		downgrade_flag, start_date,
 row_number() over ( partition by  account_id order by  start_date desc ) as rn
from ravenstack_subscriptions ) ranked
where rn = 1) s
on a.account_id = s.account_id;

create view plan_tier as
select s.plan_tier, a.account_id, count(distinct a.account_id) as churned_customers
from ravenstack_accounts a
join ravenstack_subscriptions s
on a.account_id = s.account_id
where a.churn_flag = "true"
and s.start_date =
   ( select  max(s2.start_date)
from ravenstack_subscriptions s2
where s2.account_id = s.account_id) 
group by s.plan_tier,a.account_id
order by churned_customers desc;

create view customer_behavior_analysis as 
select
a.account_id, a.country,
a.industry, a.churn_flag,
s.plan_tier, s.billing_frequency,
s.auto_renew_flag,
case
    when s.upgrade_flag = 'true' then 'Upgraded'
    when s.downgrade_flag = 'true' then 'Downgraded'
    else 'No Change'
end as last_behavior
from ravenstack_accounts a
left join (
    select *,
row_number() over(partition by  account_id order by  start_date desc, subscription_id desc
) as rn
from ravenstack_subscriptions) s
on a.account_id = s.account_id
and s.rn = 1;


