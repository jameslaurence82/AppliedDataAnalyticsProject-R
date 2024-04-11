SELECT DATEDIFF(second, '1970-01-01', COALESCE(MinDate, '2000-01-01')) as LastDT
FROM(
    SELECT MIN(dt) as MinDate
    FROM
    (
        SELECT COALESCE(MAX([FK_DT_Date]), '2000-01-01') as dt
        FROM [dbo].[THALES_STOCK]
        UNION
        SELECT COALESCE(MAX([FK_DT_Date]), '2000-01-01') as dt
        FROM [dbo].[S_P_INDEX]
        UNION
        SELECT COALESCE(MAX([FK_DT_Date]), '2000-01-01') as dt
        FROM [dbo].[EURO_INDEX]
        UNION	
        SELECT COALESCE(MAX([FK_DT_Date]), '2000-01-01') as dt
        FROM [dbo].[FRANCE_INDEX]
    ) AS x
) AS y

---- Delete data from all tables code while SSIS isn't set yet
--DELETE FROM EURO_INDEX;
--DELETE FROM FRANCE_INDEX;
--DELETE FROM S_P_INDEX;
--DELETE FROM THALES_STOCK;
