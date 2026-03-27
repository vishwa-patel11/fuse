CREATE OR REPLACE TEMPORARY FUNCTION fiscal_from_date(date_val DATE, fym INT)
RETURNS INT
RETURN CASE 
    WHEN fym = 1 THEN YEAR(date_val)
    WHEN MONTH(date_val) < fym THEN YEAR(date_val)
    ELSE YEAR(date_val) + 1
END
