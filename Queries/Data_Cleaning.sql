-- Data cleaning and preparation
-- ravenstack_accounts

-- Checking for duplicate
SELECT account_id, COUNT(*)
FROM ravenstack_accounts
GROUP BY account_id
HAVING COUNT(*) > 1;

-- removing/ leading and tailing spaces
UPDATE ravenstack_accounts
SET 
account_id = TRIM(account_id),
account_name = TRIM(account_name),
industry = TRIM(industry),
country = TRIM(country),
referral_source = TRIM(referral_source),
plan_tier = TRIM(plan_tier);

-- Standardize industry name
UPDATE ravenstack_accounts
SET industry = upper(industry);

-- ravenstack_churn_event

-- Checking for duplicate
SELECT churn_event_id, COUNT(*)
FROM ravenstack_churn_events
GROUP BY churn_event_id
HAVING COUNT(*) > 1;

-- standerdize churn reason code
UPDATE ravenstack_churn_events
SET reason_code = LOWER(reason_code);

-- validate refund amount
SELECT *
FROM ravenstack_churn_events
WHERE refund_amount_usd < 0;

-- Check reactivation flag consistency
select distinct is_reactivation
from ravenstack_churn_events;

-- ravenstack feature usage

update ravenstack_feature_usage
set usage_id = trim(usage_id);

-- Check Missing Account IDs
SELECT *
FROM ravenstack_feature_usage
WHERE usage_id IS NULL;

-- ravenstack subscription

-- Ensure Subscription Timeline Is Valid
SELECT *
FROM ravenstack_subscriptions
WHERE end_date < start_date;

-- Validate Revenue Metrics
SELECT *
FROM ravenstack_subscriptions
WHERE mrr_amount < 0
OR arr_amount < 0;

-- Standardize Billing Frequency
UPDATE ravenstack_subscriptions
SET billing_frequency = upper(billing_frequency);

-- ravenstack support ticket

select ticket_id, count(*)
from ravenstack_support_tickets
group by ticket_id
having count(*) >1;

-- Validate Ticket Timeline
SELECT *
FROM ravenstack_support_tickets
WHERE closed_at < submitted_at;

-- Validate Satisfaction Score
SELECT *
FROM ravenstack_support_tickets
WHERE satisfaction_score NOT BETWEEN 3 AND 5
AND satisfaction_score IS NOT NULL;