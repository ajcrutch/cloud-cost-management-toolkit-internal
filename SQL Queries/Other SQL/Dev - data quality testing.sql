select sum(cost)
from
  `fr-billing.frsaas.gcp_billing_export_v1_01A3C5_A1CCDD_9573E0`
  where _PARTITIONDATE > date(2025,10,10)
  and date(usage_start_time) = date(2025,10,11);

select sum(total_cost)
from
  `<project_id>.<dataset>.gcp_billing_export_daily_summary`
  where date(usage_start_date) = date(2025,10,11);

-- SELECT 
-- usage_start_date
-- -- invoice.month
-- -- service_description,
-- -- sku_description,
-- -- sum(total_cost) cost,
-- -- sum(total_credits) credits,
-- -- sum(total_discount) discounts,
-- ,sum(total_fcud_legacy) fcud_legacy
-- -- ,count(distinct project_id)
-- --, sum(  (SELECT SUM(amount) FROM UNNEST(billing_export.credits) WHERE type = 'FEE_UTILIZATION_OFFSET' )  ) 
-- FROM `<project_id>.<dataset>.gcp_billing_export_daily_summary` 
-- -- FROM `fr-billing.frsaas.gcp_billing_export_v1_01A3C5_A1CCDD_9573E0` as billing_export
-- -- WHERE _partitiondate >= "2025-08-15"  
-- -- where invoice.month='202509'

-- where EXTRACT(MONTH FROM usage_start_date) = 9
--   AND EXTRACT(YEAR FROM usage_start_date) = 2025
-- group by 1
-- ;

SELECT
EXTRACT(DAY FROM usage_start_time AT TIME ZONE 'America/Los_Angeles')
 , sum((SELECT SUM(amount) FROM UNNEST(billing_export.credits) WHERE type = 'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE')) legacy_fcud
 , sum(cost) cost
FROM
  `fr-billing.frsaas.gcp_billing_export_v1_01A3C5_A1CCDD_9573E0` AS billing_export
WHERE _partitiondate >= "2025-08-15"
and
  EXTRACT(MONTH FROM usage_start_time AT TIME ZONE 'America/Los_Angeles') = 10
  AND EXTRACT(YEAR FROM usage_start_time AT TIME ZONE 'America/Los_Angeles') = 2025
and service.description != 'Invoice'
GROUP BY 1
order by 1 asc

;



SELECT 
usage_start_date
,sum(total_fcud_legacy) fcud_legacy
,sum(total_cost) cost
FROM `<project_id>.<dataset>.gcp_billing_export_daily_summary` 

where EXTRACT(MONTH FROM usage_start_date) = 10
  AND EXTRACT(YEAR FROM usage_start_date) = 2025
  and service_description != 'Invoice'
group by 1
order by 1 asc
;