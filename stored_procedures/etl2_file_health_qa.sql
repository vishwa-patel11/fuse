-- Runs QA checks for File Health before publishing master tables
-- Values for {client} are injected by the orchestrator

----------------------------------------------------------------------------
-- Prep: Clear existing QA results for this client/dataset
----------------------------------------------------------------------------
DELETE FROM dev_catalog.metadata.etl2_qa_results
WHERE client = '{client}' AND dataset = 'File Health';

----------------------------------------------------------------------------
-- Prep: Create temporary views for QA processing
----------------------------------------------------------------------------

-- FH Group and Gift combined view
CREATE OR REPLACE TEMPORARY VIEW processing_fh_qa AS
SELECT 
    d.client, 
    rp.period, 
    rp.report_period, 
    fh.fh_group, 
    rp.offset, 
    fh.yr, 
    d.donor_id, 
    g.gift_date, 
    g.gift_amount
FROM dev_catalog.{client}.dbo_{client}_donor_silver d 
JOIN (
    SELECT DISTINCT client, period, report_period, start_date, end_date, offset
    FROM dev_catalog.{client}.dbo_{client}_fh_date_silver
) rp ON d.client = rp.client 
JOIN dev_catalog.{client}.dbo_{client}_fh_group_silver fh 
  ON d.client = fh.client 
 AND d.donor_key = fh.donor_key 
 AND rp.period = fh.period 
 AND rp.report_period = fh.report_period
LEFT JOIN dev_catalog.{client}.dbo_{client}_gift_silver g 
  ON fh.client = g.client 
 AND d.donor_key = g.donor_key 
 AND g.gift_date BETWEEN rp.start_date AND rp.end_date;

-- FH Date view
CREATE OR REPLACE TEMPORARY VIEW processing_fh_date_qa AS
SELECT * 
FROM dev_catalog.{client}.dbo_{client}_fh_date_silver;

-- Gift Summary view
CREATE OR REPLACE  TEMPORARY VIEW processing_gift_summary AS
SELECT 
    client,
    CAST(MAX(gift_date) AS date) as mx,
    (YEAR(MAX(gift_date)) - YEAR(MIN(gift_date)) + 1) as max_yrs,
    SUM(CASE WHEN YEAR(gift_date) = YEAR(CURRENT_DATE()) - 1 THEN gift_amount ELSE 0 END) as py_gift_amount
FROM dev_catalog.{client}.dbo_{client}_gift_silver
GROUP BY client;


----------------------------------------------------------------------------
-- QA Check: No report period is longer than a year
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH a AS (
    SELECT 
        client, 
        period, 
        report_period, 
        start_date, 
        end_date, 
        datediff(end_date, start_date) as len, 
        CASE WHEN datediff(end_date, start_date) > 366 THEN 1 ELSE 0 END as fail
    FROM processing_fh_date_qa
    GROUP BY client, period, report_period, start_date, end_date
)
SELECT 
    client, 
    'File Health' as dataset, 
    'No report period is longer than a year' as check,
    array_join(collect_list(period || ': ' || report_period || ' (' || CAST(len AS string) || ' days)'), ', ') as detail,
    CASE WHEN SUM(fail) > 0 THEN 1 ELSE 0 END as fail,
    ts_est() as ts,
    'error' as check_level
FROM a
GROUP BY client;


----------------------------------------------------------------------------
-- QA Check: Number of report periods correct
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH a AS (
    SELECT 
        client, 
        period,
        count(distinct report_period) as number_of_rps,
        (SELECT max_yrs FROM processing_gift_summary) as max_years_of_data
    FROM processing_fh_date_qa
    GROUP BY client, period
)
SELECT 
    client, 
    'File Health' as dataset, 
    'Number of report periods correct' as check,
    array_join(collect_list(period || ': ' || CAST(number_of_rps AS string)), ', ') as detail,
    CASE WHEN SUM(CASE WHEN number_of_rps <> least(6, max_years_of_data) THEN 1 ELSE 0 END) > 0 THEN 1 ELSE 0 END as fail,
    ts_est() as ts,
    'warning' as check_level
FROM a
GROUP BY client;


----------------------------------------------------------------------------
-- QA Check: Start dates correct (CYs)
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH a AS (
    SELECT DISTINCT 
        client, 
        period, 
        report_period, 
        CASE WHEN MONTH(start_date) <> 1 OR DAY(start_date) <> 1 THEN 1 ELSE 0 END as fail, 
        CAST(MONTH(start_date) AS string) as mth_start
    FROM processing_fh_date_qa
    WHERE period LIKE 'c%'
)
SELECT 
    client, 
    'File Health' as dataset, 
    'Start dates correct (CYs)' as check,
    array_join(collect_list(period || ': ' || mth_start), ', ') as detail,
    CASE WHEN SUM(fail) > 0 THEN 1 ELSE 0 END as fail,
    ts_est() as ts,
    'error' as check_level
FROM a
GROUP BY client;


----------------------------------------------------------------------------
-- QA Check: Start dates correct (FYs)
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH a AS (
    SELECT DISTINCT 
        d.client, 
        period, 
        report_period, 
        CASE WHEN (MONTH(start_date) <> c.fiscal_year_start) THEN 1 ELSE 0 END as fail, 
        CAST(MONTH(start_date) AS string) as mth_start
    FROM processing_fh_date_qa d 
    JOIN dev_catalog.metadata.client c ON d.client = c.client 
    WHERE period LIKE 'f%'
)
SELECT 
    client, 
    'File Health' as dataset, 
    'Start dates correct (FYs)' as check,
    array_join(collect_list(period || ': ' || mth_start), ', ') as detail,
    CASE WHEN SUM(fail) > 0 THEN 1 ELSE 0 END as fail,
    ts_est() as ts,
    'error' as check_level
FROM a
GROUP BY client;


----------------------------------------------------------------------------
-- QA Check: End dates correct (*TDs)
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH list AS (
    SELECT DISTINCT 
        d.client, 
        period, 
        report_period, 
        m.mx, 
        end_date, 
        CASE WHEN end_date > m.mx THEN 1 ELSE 0 END as fail
    FROM processing_fh_date_qa d
    JOIN processing_gift_summary m ON d.client = m.client
    WHERE period LIKE '%td'
) 
SELECT 
    client, 
    'File Health' as dataset, 
    'End dates correct (*TDs)' as check,
    array_join(collect_list(period || ' ' || report_period || ': ' || CAST(end_date AS string) || ' end date vs ' || CAST(mx AS string) || ' max gift date'), '  |  ') as detail,
    CASE WHEN SUM(fail) > 0 THEN 1 ELSE 0 END as fail,
    ts_est() as ts,
    'error' as check_level
FROM list
GROUP BY client;


----------------------------------------------------------------------------
-- QA Check: Donors NLY = N(LY)
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH x AS (
    SELECT client, period, report_period, fh_group, offset, count(distinct donor_id) donors
    FROM processing_fh_qa
    WHERE fh_group IN ('New', 'New Last Year')
    GROUP BY client, period, report_period, fh_group, offset
),
y AS (
    SELECT 
        a.*, 
        CASE WHEN (b.donors IS NULL OR a.donors = b.donors) THEN 0 ELSE 1 END as fail
    FROM x a 
    LEFT JOIN x b 
      ON a.period = b.period 
     AND a.offset = b.offset + 1 
     AND b.fh_group = REPLACE(a.fh_group, ' Last Year', '')
    WHERE a.fh_group = 'New Last Year' 
) 
SELECT 
    client, 
    'File Health' as dataset, 
    'Donors NLY = N(LY)' as check,
    array_join(collect_list(period || ' ' || report_period), ', ') as detail,
    CASE WHEN SUM(fail) > 0 THEN 1 ELSE 0 END as fail,
    ts_est() as ts,
    'error' as check_level
FROM y
GROUP BY client;


----------------------------------------------------------------------------
-- QA Check: Gift amount for PY matches gift table
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH fh AS (
    SELECT client, period, yr, SUM(gift_amount) as fh_gift_amount
    FROM processing_fh_qa
    WHERE period = 'cy' AND yr = YEAR(CURRENT_DATE()) - 1
    GROUP BY client, period, yr
)
SELECT 
    fh.client, 
    'File Health' as dataset, 
    'Gift amount for PY matches gift table' as check,
    'FH.Gift_Amount: ' || CAST(fh_gift_amount AS string) || ', Gift.Gift_Amount: ' || CAST(py_gift_amount AS string) as detail,
    IF(ROUND(py_gift_amount, 0) <> ROUND(fh_gift_amount, 0), 1, 0) as fail,
    ts_est() as ts,
    CASE WHEN (py_gift_amount * 1.0 / fh_gift_amount * 1.0 - 1.0) > 0.05 THEN 'error' ELSE 'warning' END as check_level
FROM fh
CROSS JOIN processing_gift_summary gs;


----------------------------------------------------------------------------
-- QA Check: Data has >7 day gap
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH a AS (
    SELECT 
        client, 
        period, 
        report_period, 
        CAST(datediff(gift_date, lag(gift_date) OVER(PARTITION BY period, report_period ORDER BY gift_date)) AS int) as day_diff
    FROM processing_fh_qa
    GROUP BY client, period, report_period, gift_date
),
summary AS (
    SELECT client, period, report_period, SUM(CASE WHEN day_diff > 7 THEN 1 ELSE 0 END) as gaps
    FROM a 
    GROUP BY client, period, report_period
)
SELECT 
    client, 
    'File Health' as dataset, 
    'Data has >7 day gap' as check,
    array_join(collect_list(report_period || ': ' || CAST(gaps AS string) || ' gaps'), ', ') as detail,
    IF(SUM(gaps) > 0, 1, 0) as fail,
    ts_est() as ts,
    'warning' as check_level
FROM summary 
GROUP BY client;


----------------------------------------------------------------------------
-- QA Check: New Universe = New Donors
----------------------------------------------------------------------------
INSERT INTO dev_catalog.metadata.etl2_qa_results
WITH a AS (
    SELECT 
        client, 
        period, 
        report_period,
        count(distinct if(coalesce(gift_amount, 0) > 0, donor_id, NULL)) as donors,
        count(distinct donor_id) as universe
    FROM processing_fh_qa
    WHERE period IN ('r12', 'cy', 'fy') AND fh_group = 'New'
    GROUP BY client, period, report_period
), 
summary AS (
    SELECT client, period, report_period, universe, donors, if(universe <> donors, 1, 0) as mismatches
    FROM a 
)
SELECT 
    client, 
    'File Health' as dataset, 
    'New Universe = New Donors' as check,
    array_join(collect_list(report_period || ': ' || CAST(mismatches AS string) || ' mismatches'), ', ') as detail,
    IF(SUM(mismatches) > 0, 1, 0) as fail,
    ts_est() as ts,
    'warning' as check_level
FROM summary 
GROUP BY client;