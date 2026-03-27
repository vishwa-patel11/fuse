-- Reads in client curated tables to produce curated.fh_group and curated.fh_date tables that inform client reporting
-- fh_inner_loop 'rady', 'cy'

/*
    - All the logic for file health is contained in this script
    - The outputs of this script are:
        1. [client]_fh_date  - the date definitions for each report period included
        2. [client]_fh_group - the report period specific FH Group labels for each donor. This is the meat.
*/

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - Start', current_timestamp();


-------------------------------------------------------------------
-- Summarize gift history
-------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW processing_gift_history AS
SELECT
      a.client
    , donor_id
    , donor_key
    , gift_date
    , fiscal_from_date(gift_date, c.fiscal_year_start) as gift_fy
    , YEAR(gift_date) as gift_cy
FROM dev_catalog.{client}.dbo_{client}_gift_silver a
JOIN dev_catalog.metadata.client c ON a.client = c.client
WHERE COALESCE(gift_amount, 0) > 0;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - processing_gift_history complete', current_timestamp();

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'),
       'File Health',
       '{period} - max gift date included: ' || CAST(CAST(MAX(gift_date) AS date) AS string),
       current_timestamp()
FROM processing_gift_history;


---------------------------------------------------------------------------------
-- Find all year combos for this client
---------------------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW processing_years AS
WITH mths AS
(
    SELECT client, date_trunc('month', gift_date) as mth
    FROM processing_gift_history
    GROUP BY client, date_trunc('month', gift_date)
)
SELECT
    g.client,
    period,
    CASE
        WHEN LEFT(labels.period, 2) = 'fy'
            THEN fiscal_from_date(cast(mth as date), c.fiscal_year_start)
        ELSE YEAR(mth)
    END as yr
FROM mths g
JOIN dev_catalog.metadata.client c ON g.client = c.client
CROSS JOIN dev_catalog.metadata.fh_default_time_periods labels
WHERE labels.period = '{period}'
GROUP BY
    g.client,
    period,
    CASE
        WHEN LEFT(labels.period, 2) = 'fy'
            THEN fiscal_from_date(cast(mth as date), c.fiscal_year_start)
        ELSE YEAR(mth)
    END;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - processing_years complete', current_timestamp();


---------------------------------------------------------------------------------
-- Find all years applicable for each donor (each year after their first gift year)
---------------------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW processing_cross_donor_report_periods AS
WITH w_donor_ranges AS
(
    SELECT
          c.client
        , donor_id
        , donor_key
        , fiscal_from_date(MIN(gift_date), c.fiscal_year_start) as first_fiscal_year
        , YEAR(MIN(gift_date)) as first_calendar_year
    FROM processing_gift_history d
    JOIN dev_catalog.metadata.client c ON d.client = c.client
    GROUP BY c.client, donor_id, donor_key, c.fiscal_year_start
)
SELECT
    d.client,
    donor_id,
    donor_key,
    period,
    yr
FROM processing_years r
JOIN w_donor_ranges d
  ON r.client = d.client
 AND r.yr >= CASE
                WHEN period IN ('fy','fytd') THEN first_fiscal_year
                ELSE first_calendar_year
             END;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - processing_cross_donor_report_periods complete', current_timestamp();


-------------------------------------------------------------------
-- Create FH Report Period definitions based on max data available
-------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW year0 AS
SELECT
    client,
    most_recent_complete_month,
    period,
    start_date,
    end_date
FROM dev_catalog.metadata.fh_report_periods
WHERE client = '{client}'
  AND period = '{period}';

-- Take these definitions and find the parallel periods for the past x years

CREATE OR REPLACE TEMP VIEW offsets AS
SELECT
    yr,
    -(MAX(yr) OVER() - yr) as offset
FROM (SELECT DISTINCT yr FROM processing_years) a;

CREATE OR REPLACE TEMP VIEW almost AS
SELECT
      a.client
    , a.most_recent_complete_month
    , a.period
    , add_months(start_date, offset * 12) as start_date
    , add_months(end_date, offset * 12) as end_date
    , b.offset
FROM year0 a
CROSS JOIN offsets b;

CREATE OR REPLACE TEMP VIEW processing_fh_report_period AS
SELECT
      a.client
    , most_recent_complete_month
    , period
    , date_format(start_date, 'MM/dd/yyyy') || ' - ' || date_format(end_date, 'MM/dd/yyyy') as report_period
    , start_date
    , end_date
    , CASE
        WHEN period IN ('fy','fytd') THEN fiscal_from_date(cast(end_date as date), c.fiscal_year_start)
        WHEN period IN ('cy','cytd','r12') THEN YEAR(end_date)
        ELSE NULL
      END as yr
    , offset
    , a.client || '-' || period || ' ending ' || date_format(end_date, 'MM/dd/yyyy') as report_period_id
    , CASE WHEN offset > -5 THEN 1 ELSE 0 END as reportable
FROM almost a
JOIN dev_catalog.metadata.client c ON a.client = c.client;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - processing_fh_report_period complete', current_timestamp();


-------------------------------------------------------
-- For each FH report period, identify the relative gift history
-------------------------------------------------------

CREATE OR REPLACE TEMP VIEW processing_relative_gift_history AS
WITH w_gift_summary AS
(
    SELECT
          h.client
        , h.donor_id
        , h.donor_key
        , rp.period
        , rp.report_period
        , rp.offset
        , rp.yr
    FROM processing_gift_history h
    JOIN processing_fh_report_period rp
      ON h.client = rp.client
     AND rp.period = '{period}'
     AND (
            (rp.period IN ('fy','fytd') AND rp.yr = h.gift_fy)
         OR (rp.period IN ('cy','cytd') AND rp.yr = h.gift_cy)
         OR (rp.period = 'r12' AND h.gift_date BETWEEN rp.start_date AND rp.end_date)
     )
    GROUP BY
          h.client
        , h.donor_id
        , h.donor_key
        , rp.period
        , rp.report_period
        , rp.offset
        , rp.yr
)
SELECT
      a.client
    , a.donor_id
    , a.donor_key
    , rp.report_period
    , rp.report_period_id
    , rp.offset
    , a.period
    , a.yr
    , MAX(CASE WHEN a.yr = g.yr THEN g.yr ELSE NULL END) as gift_0
    , MAX(CASE WHEN g.yr = a.yr-1 THEN g.yr ELSE NULL END) as gift_minus1
    , MAX(CASE WHEN g.yr = a.yr-2 THEN g.yr ELSE NULL END) as gift_minus2
    , MIN(g.yr) as first_gift
    , MAX(CASE WHEN a.yr = g.yr THEN NULL ELSE g.yr END) as last_gift
    , CASE WHEN MAX(CASE WHEN a.yr = g.yr THEN g.yr ELSE NULL END) IS NULL THEN 0 ELSE 1 END as gift_made
FROM processing_cross_donor_report_periods a
JOIN processing_fh_report_period rp
  ON a.client = rp.client
 AND a.period = rp.period
 AND a.yr = rp.yr
LEFT JOIN w_gift_summary g
  ON a.client = g.client
 AND a.donor_key = g.donor_key
 AND a.period = g.period
 AND g.yr <= a.yr
GROUP BY
      a.client
    , a.donor_id
    , a.donor_key
    , rp.report_period
    , rp.offset
    , a.period
    , a.yr
    , rp.report_period_id;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - processing_relative_gift_history complete', current_timestamp();


-------------------------------------------------------------------
-- Create streaks/sessions
-------------------------------------------------------------------

CREATE OR REPLACE TEMP VIEW processing_summary AS
WITH w_fiscal_data AS
(
    SELECT *,
        CASE
            WHEN (gift_made = 1
               AND COALESCE(LEAD(gift_made) OVER(PARTITION BY client, donor_key, period ORDER BY yr DESC), 0) = 0)
            THEN yr ELSE 0 END as streak_st
    FROM processing_relative_gift_history
),
w_streak_starts AS
(
    SELECT *
    FROM w_fiscal_data
    WHERE streak_st <> 0
)
SELECT
      a.client, a.donor_id, a.donor_key
    , a.report_period, a.period, a.report_period_id
    , a.yr, a.offset
    , a.gift_0, a.gift_minus1, a.gift_minus2
    , a.last_gift, a.first_gift, a.gift_made, a.streak_st
    , MAX(CASE WHEN a.gift_made = 1 THEN b.streak_st ELSE NULL END) as first_year_in_streak
FROM w_fiscal_data a
LEFT JOIN w_streak_starts b
  ON a.client = b.client
 AND a.donor_key = b.donor_key
 AND a.period = b.period
 AND a.yr >= b.streak_st
GROUP BY
      a.client, a.donor_id, a.donor_key
    , a.period, a.report_period_id, a.yr
    , a.gift_0, a.gift_minus1, a.gift_minus2
    , a.last_gift, a.first_gift
    , a.gift_made, a.streak_st
    , a.report_period, a.offset;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - processing_summary complete', current_timestamp();


-------------------------------------------------------
-- Translate relative gift history into FH group definitions
-------------------------------------------------------

CREATE OR REPLACE TEMP VIEW processing_fh_group_by_year AS
WITH w_raw AS
(
    SELECT
          b.client, b.period, b.report_period, b.report_period_id, b.yr, b.offset, b.donor_id
        , gift_0, gift_minus1, gift_minus2, last_gift, first_gift, first_year_in_streak, b.donor_key
        , LEAD(first_year_in_streak,1) OVER(PARTITION BY b.client, b.donor_key, b.period ORDER BY b.yr DESC) last_years_streak
        , CASE
            WHEN b.yr = first_gift THEN 'New'
            WHEN b.yr = first_gift+1 THEN 'New Last Year'
            WHEN b.yr = gift_minus1+1 AND b.yr = gift_minus2+2 THEN 'Consecutive Giver'
            WHEN (b.yr - COALESCE(gift_minus1, 0)) > 1 THEN 'Reacquired'
            WHEN gift_minus2 IS NULL AND gift_minus1 IS NOT NULL THEN 'Reacquired Last Year'
            ELSE 'UNKNOWN'
          END fh_group
        , CASE WHEN gift_0 IS NOT NULL AND gift_minus1 IS NOT NULL THEN 1 ELSE 0 END past_2_years
    FROM processing_summary b
)
SELECT *
    , fh_group || CASE
        WHEN fh_group = 'Consecutive Giver' THEN CAST(yr - last_years_streak AS string)
        WHEN fh_group = 'Reacquired' THEN CAST(yr - last_gift AS string)
        WHEN fh_group = 'Reacquired Last Year'
            THEN CAST((gift_minus1 - LEAD(last_gift, 1) OVER(PARTITION BY client, donor_key, period ORDER BY yr DESC)) AS string)
        ELSE '' END as fh_group_detail
    , CASE
        WHEN fh_group IN ('New','Reacquired') THEN 'Pipeline'
        WHEN fh_group IN ('Reacquired Last Year','Consecutive Giver','New Last Year') THEN 'Active'
      END as donor_type
    , CASE
        WHEN fh_group IN ('New','Reacquired') THEN 'Pipeline'
        WHEN fh_group IN ('Reacquired Last Year','New Last Year') THEN '2nd Year'
        WHEN fh_group IN ('Consecutive Giver') THEN 'Consecutive Giver'
      END as donor_subtype
FROM w_raw;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - processing_fh_group_by_year complete', current_timestamp();


-------------------------------------------------------
-- Turn these back into client specific outputs
-------------------------------------------------------

-- [client]_fh_date
DELETE FROM dev_catalog.{client}.dbo_{client}_fh_date_silver WHERE period = '{period}';

INSERT INTO dev_catalog.{client}.dbo_{client}_fh_date_silver
SELECT a.*, d.date, current_timestamp() as data_processed_at, a.client || '-' || date_format(d.date, 'yyyy-MM-dd') as client_date
FROM processing_fh_report_period a
JOIN (
    SELECT *
    FROM dev_catalog.metadata.date
    WHERE YEAR(date) >= YEAR(CURRENT_DATE()) - 6
) d ON d.date BETWEEN a.start_date AND a.end_date
WHERE offset > -6;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - [client]_fh_date complete', current_timestamp();


-- [client]_fh_group
DELETE FROM dev_catalog.{client}.dbo_{client}_fh_group_silver WHERE period = '{period}';

INSERT INTO dev_catalog.{client}.dbo_{client}_fh_group_silver
SELECT client, period, report_period, report_period_id, yr, donor_id,
       fh_group, fh_group_detail, donor_type, donor_subtype, past_2_years, donor_key,
       current_timestamp() as data_processed_at
FROM processing_fh_group_by_year
WHERE offset > -6;

INSERT INTO dev_catalog.metadata.etl2_status
SELECT UPPER('{client}'), 'File Health', '{period} - [client]_fh_group complete', current_timestamp();