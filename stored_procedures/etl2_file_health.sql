-- Reads in client curated tables to produce curated.fh_group and curated.fh_date tables that inform client reporting
-- fh_inner_loop 'rady', 'cy'

CREATE   PROCEDURE dbo.etl2_file_health
(
    @client varchar(5),
    @period varchar(5)
)
AS
BEGIN

/*
    - This sproc is called on loop from File Health.py in databricks
    - All the logic for file health is contained in this sproc
    - The outputs of this sproc are:
        1. [client]_fh_date  - the date definitions for each report period included
        2. [client]_fh_group - the report period specific FH Group labels for each donor. This is the meat.
*/

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - Start', dbo.ts_est();


    -------------------------------------------------------------------
    -- Summarize gift history
    -------------------------------------------------------------------

    EXEC('
        DROP TABLE IF EXISTS tmp.processing_gift_history;

        SELECT
              a.client
            , donor_id
            , donor_key
            , gift_date
            , dbo.fiscal_from_date(gift_date, c.fiscal_year_start) gift_fy
            , YEAR(gift_date) gift_cy
        INTO tmp.processing_gift_history
        FROM curated.' + @client + '_gift a
        JOIN client c ON a.client = c.client
        WHERE COALESCE(gift_amount,0) > 0
    ');

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - processing_gift_history complete', dbo.ts_est();

    INSERT INTO etl2_status
    SELECT UPPER(@client),
           'File Health',
           @period + ' - max gift date included: ' + CAST(CAST(MAX(gift_date) AS date) AS varchar),
           dbo.ts_est()
    FROM tmp.processing_gift_history;


    ---------------------------------------------------------------------------------
    -- Find all year combos for this client
    ---------------------------------------------------------------------------------

    DROP TABLE IF EXISTS tmp.processing_years;

    WITH mths AS
    (
        SELECT client, DATETRUNC(month, gift_date) mth
        FROM tmp.processing_gift_history
        GROUP BY client, DATETRUNC(month, gift_date)
    )
    SELECT
        g.client,
        period,
        CASE
            WHEN LEFT(labels.period,2) = 'fy'
                THEN dbo.fiscal_from_date(mth,c.fiscal_year_start)
            ELSE YEAR(mth)
        END yr
    INTO tmp.processing_years
    FROM mths g
    JOIN client c ON g.client = c.client,
         fh_default_time_periods labels
    WHERE labels.period = @period
    GROUP BY
        g.client,
        period,
        CASE
            WHEN LEFT(labels.period,2) = 'fy'
                THEN dbo.fiscal_from_date(mth,c.fiscal_year_start)
            ELSE YEAR(mth)
        END;

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - processing_years complete', dbo.ts_est();


    ---------------------------------------------------------------------------------
    -- Find all years applicable for each donor (each year after their first gift year)
    ---------------------------------------------------------------------------------

    DROP TABLE IF EXISTS tmp.processing_cross_donor_report_periods;

    WITH w_donor_ranges AS
    (
        SELECT
              c.client
            , donor_id
            , donor_key
            , dbo.fiscal_from_date(MIN(gift_date),c.fiscal_year_start) first_fiscal_year
            , YEAR(MIN(gift_date)) first_calendar_year
        FROM tmp.processing_gift_history d
        JOIN client c ON d.client = c.client
        GROUP BY c.client, donor_id, donor_key, c.fiscal_year_start
    )
    SELECT
        d.client,
        donor_id,
        donor_key,
        period,
        yr
    INTO tmp.processing_cross_donor_report_periods
    FROM tmp.processing_years r
    JOIN w_donor_ranges d
      ON r.client = d.client
     AND r.yr >= CASE
                    WHEN period IN ('fy','fytd') THEN first_fiscal_year
                    ELSE first_calendar_year
                 END;

    PRINT('prep');

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - processing_cross_donor_report_periods complete', dbo.ts_est();


    -------------------------------------------------------------------
    -- Create FH Report Period definitions based on max data available
    -------------------------------------------------------------------

    DROP TABLE IF EXISTS #year0;

    SELECT
        client,
        most_recent_complete_month,
        period,
        start_date,
        end_date
    INTO #year0
    FROM fh_report_periods
    WHERE client = @client
      AND period = @period;

    -- Take these definitions and find the parallel periods for the past x years

    DROP TABLE IF EXISTS #offsets;

    SELECT
        yr,
        -(MAX(yr) OVER() - yr) offset
    INTO #offsets
    FROM (SELECT DISTINCT yr FROM tmp.processing_years) a;

    DROP TABLE IF EXISTS #almost;

    SELECT
          a.client
        , a.most_recent_complete_month
        , a.period
        , DATEADD(year, offset, start_date) start_date
        , DATEADD(year, offset, end_date) end_date
        , b.offset
    INTO #almost
    FROM #year0 a,
         #offsets b;

    DROP TABLE IF EXISTS tmp.processing_fh_report_period;

    SELECT
          a.client
        , most_recent_complete_month
        , period
        , CONVERT(varchar(10), start_date, 101) + ' - ' + CONVERT(varchar(10), end_date, 101) report_period
        , start_date
        , end_date
        , CASE
            WHEN period IN ('fy','fytd') THEN dbo.fiscal_from_date(end_date,c.fiscal_year_start)
            WHEN period IN ('cy','cytd','r12') THEN YEAR(end_date)
            ELSE NULL
          END yr
        , offset
        , a.client + '-' + period + ' ending ' + CONVERT(varchar(10), end_date, 101) report_period_id
        , CASE WHEN offset > -5 THEN 1 ELSE 0 END reportable
    INTO tmp.processing_fh_report_period
    FROM #almost a
    JOIN client c ON a.client = c.client;

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - processing_fh_report_period complete', dbo.ts_est();


    -------------------------------------------------------
    -- For each FH report period, identify the relative gift history
    -- ✅ ONLY CHANGE: r12 matches by gift_date BETWEEN rp.start_date AND rp.end_date
    -------------------------------------------------------

    DROP TABLE IF EXISTS tmp.processing_relative_gift_history;

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
        FROM tmp.processing_gift_history h
        JOIN tmp.processing_fh_report_period rp
          ON h.client = rp.client
         AND rp.period = @period
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
        , MAX(CASE WHEN a.yr = g.yr THEN g.yr ELSE NULL END) gift_0
        , MAX(CASE WHEN g.yr = a.yr-1 THEN g.yr ELSE NULL END) gift_minus1
        , MAX(CASE WHEN g.yr = a.yr-2 THEN g.yr ELSE NULL END) gift_minus2
        , MIN(g.yr) first_gift
        , MAX(CASE WHEN a.yr = g.yr THEN NULL ELSE g.yr END) last_gift
        , CAST(NULL AS int) gift_made
    INTO tmp.processing_relative_gift_history
    FROM tmp.processing_cross_donor_report_periods a
    JOIN tmp.processing_fh_report_period rp
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

    UPDATE tmp.processing_relative_gift_history
    SET gift_made = CASE WHEN gift_0 IS NULL THEN 0 ELSE 1 END;

    PRINT('summary');

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - processing_relative_gift_history complete', dbo.ts_est();


    -------------------------------------------------------------------
    -- Create streaks/sessions
    -------------------------------------------------------------------

    DROP TABLE IF EXISTS tmp.processing_summary;

    WITH w_fiscal_data AS
    (
        SELECT *,
            CASE
                WHEN (gift_made = 1
                   AND COALESCE(LEAD(gift_made) OVER(PARTITION BY client, donor_key, period ORDER BY yr DESC),0) = 0)
                THEN yr ELSE '' END streak_st
        FROM tmp.processing_relative_gift_history
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
        , MAX(CASE WHEN a.gift_made = 1 THEN b.streak_st ELSE NULL END) first_year_in_streak
    INTO tmp.processing_summary
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

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - processing_summary complete', dbo.ts_est();


    -------------------------------------------------------
    -- Translate relative gift history into FH group definitions
    -------------------------------------------------------

    DROP TABLE IF EXISTS tmp.processing_fh_group_by_year;

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
                WHEN (b.yr - COALESCE(gift_minus1,0)) > 1 THEN 'Reacquired'
                WHEN gift_minus2 IS NULL AND gift_minus1 IS NOT NULL THEN 'Reacquired Last Year'
                ELSE 'UNKNOWN'
              END fh_group
            , CASE WHEN gift_0 IS NOT NULL AND gift_minus1 IS NOT NULL THEN 1 ELSE 0 END past_2_years
        FROM tmp.processing_summary b
    )
    SELECT *
        , fh_group + CASE
            WHEN fh_group = 'Consecutive Giver' THEN CAST(yr - last_years_streak AS varchar)
            WHEN fh_group = 'Reacquired' THEN CAST(yr - last_gift AS varchar)
            WHEN fh_group = 'Reacquired Last Year'
                THEN CAST((gift_minus1 - LEAD(last_gift, 1) OVER(PARTITION BY client, donor_key, period ORDER BY yr DESC)) AS varchar)
            ELSE '' END fh_group_detail
        , CASE
            WHEN fh_group IN ('New','Reacquired') THEN 'Pipeline'
            WHEN fh_group IN ('Reacquired Last Year','Consecutive Giver','New Last Year') THEN 'Active'
          END donor_type
        , CASE
            WHEN fh_group IN ('New','Reacquired') THEN 'Pipeline'
            WHEN fh_group IN ('Reacquired Last Year','New Last Year') THEN '2nd Year'
            WHEN fh_group IN ('Consecutive Giver') THEN 'Consecutive Giver'
          END donor_subtype
    INTO tmp.processing_fh_group_by_year
    FROM w_raw;

    PRINT('fh groups');

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - processing_fh_group_by_year complete', dbo.ts_est();


    -------------------------------------------------------
    -- Turn these back into client specific outputs
    -------------------------------------------------------

    DECLARE @insert varchar(250);
    DECLARE @into   varchar(250);
    DECLARE @tbl    varchar(250);
    DECLARE @exists int;
    DECLARE @script varchar(max);

    -- [client]_fh_date

    SET @tbl = @client + '_fh_date';

    SET @exists = (
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = @tbl
          AND table_schema = 'dbo'
    );

    SET @insert = IIF(@exists > 0,
        'delete from ' + @tbl + ' where period = ''' + @period + '''; insert into ' + @tbl,
        ''
    );

    SET @into = IIF(@exists > 0, '', 'into ' + @tbl);

    SET @script = '
' + @insert + '
select a.*, d.date, dbo.ts_est() data_processed_at, CONCAT(a.client,''-'',FORMAT(d.date, ''yyyy-MM-dd'')) client_date
' + @into + '
from tmp.processing_fh_report_period a
join (
    select *
    from date
    where year(date) >= year(getdate()) - 6
) d on d.date between a.start_date and a.end_date
where offset > -6
';

    PRINT(@script);
    EXEC(@script);
    PRINT('report period definitions');

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - [client]_fh_date complete', dbo.ts_est();


    -- [client]_fh_group

    SET @tbl = @client + '_fh_group';

    SET @exists = (
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = @tbl
          AND table_schema = 'dbo'
    );

    SET @insert = IIF(@exists > 0,
        'delete from ' + @tbl + ' where period = ''' + @period + '''; insert into ' + @tbl,
        ''
    );

    SET @into = IIF(@exists > 0, '', 'into ' + @tbl);

    SET @script = '
' + @insert + '
select client, period, report_period, report_period_id, yr, donor_id,
       fh_group, fh_group_detail, donor_type, donor_subtype, past_2_years, donor_key,
       dbo.ts_est() data_processed_at
' + @into + '
from tmp.processing_fh_group_by_year
where offset > -6
';

    PRINT(@script);
    EXEC(@script);
    PRINT('FH x report periods');

    INSERT INTO etl2_status
    SELECT UPPER(@client), 'File Health', @period + ' - [client]_fh_group complete', dbo.ts_est();

END;