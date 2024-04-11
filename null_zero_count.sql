DECLARE @TableName NVARCHAR(128) = 'vw_COMBINED_TABLES';
DECLARE @SQL NVARCHAR(MAX) = '';

SELECT @SQL = @SQL + 'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) - COUNT([' + COLUMN_NAME + ']) AS NullCount, SUM(CASE WHEN [' + COLUMN_NAME + '] = 0 THEN 1 ELSE 0 END) AS ZeroCount FROM [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '] UNION ALL '
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @TableName AND DATA_TYPE IN ('int', 'decimal', 'numeric', 'float', 'money', 'smallint', 'tinyint', 'bigint', 'real');

SET @SQL = LEFT(@SQL, LEN(@SQL) - 10); -- Remove the last 'UNION ALL'

EXEC sp_executesql @SQL;


-------------------vw_COMBINED_RSI-------------------------------
--DECLARE @TableName NVARCHAR(128) = 'vw_COMBINED_RSI';
--DECLARE @SQL NVARCHAR(MAX) = '';

--SELECT @SQL = @SQL + 'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) - COUNT([' + COLUMN_NAME + ']) AS NullCount, SUM(CASE WHEN [' + COLUMN_NAME + '] = 0 THEN 1 ELSE 0 END) AS ZeroCount FROM [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '] UNION ALL '
--FROM INFORMATION_SCHEMA.COLUMNS
--WHERE TABLE_NAME = @TableName AND DATA_TYPE IN ('int', 'decimal', 'numeric', 'float', 'money', 'smallint', 'tinyint', 'bigint', 'real');

--SET @SQL = LEFT(@SQL, LEN(@SQL) - 10); -- Remove the last 'UNION ALL'

--EXEC sp_executesql @SQL;

-----------------vw_COMBINED_OBV-------------------------------
--DECLARE @TableName NVARCHAR(128) = 'vw_COMBINED_OBV';
--DECLARE @SQL NVARCHAR(MAX) = '';

--SELECT @SQL = @SQL + 'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) - COUNT([' + COLUMN_NAME + ']) AS NullCount, SUM(CASE WHEN [' + COLUMN_NAME + '] = 0 THEN 1 ELSE 0 END) AS ZeroCount FROM [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '] UNION ALL '
--FROM INFORMATION_SCHEMA.COLUMNS
--WHERE TABLE_NAME = @TableName AND DATA_TYPE IN ('int', 'decimal', 'numeric', 'float', 'money', 'smallint', 'tinyint', 'bigint', 'real');

--SET @SQL = LEFT(@SQL, LEN(@SQL) - 10); -- Remove the last 'UNION ALL'

--EXEC sp_executesql @SQL;

-------------------vw_COMBINED_MACD-------------------------------
--DECLARE @TableName NVARCHAR(128) = 'vw_COMBINED_MACD';
--DECLARE @SQL NVARCHAR(MAX) = '';

--SELECT @SQL = @SQL + 'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) - COUNT([' + COLUMN_NAME + ']) AS NullCount, SUM(CASE WHEN [' + COLUMN_NAME + '] = 0 THEN 1 ELSE 0 END) AS ZeroCount FROM [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '] UNION ALL '
--FROM INFORMATION_SCHEMA.COLUMNS
--WHERE TABLE_NAME = @TableName AND DATA_TYPE IN ('int', 'decimal', 'numeric', 'float', 'money', 'smallint', 'tinyint', 'bigint', 'real');

--SET @SQL = LEFT(@SQL, LEN(@SQL) - 10); -- Remove the last 'UNION ALL'

--EXEC sp_executesql @SQL;

-------------------vw_COMBINED_MA-------------------------------
--DECLARE @TableName NVARCHAR(128) = 'vw_COMBINED_MA';
--DECLARE @SQL NVARCHAR(MAX) = '';

--SELECT @SQL = @SQL + 'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) - COUNT([' + COLUMN_NAME + ']) AS NullCount, SUM(CASE WHEN [' + COLUMN_NAME + '] = 0 THEN 1 ELSE 0 END) AS ZeroCount FROM [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '] UNION ALL '
--FROM INFORMATION_SCHEMA.COLUMNS
--WHERE TABLE_NAME = @TableName AND DATA_TYPE IN ('int', 'decimal', 'numeric', 'float', 'money', 'smallint', 'tinyint', 'bigint', 'real');

--SET @SQL = LEFT(@SQL, LEN(@SQL) - 10); -- Remove the last 'UNION ALL'

--EXEC sp_executesql @SQL;


-------------------vw_COMBINED_BB-------------------------------
--DECLARE @TableName NVARCHAR(128) = 'vw_COMBINED_BB';
--DECLARE @SQL NVARCHAR(MAX) = '';

--SELECT @SQL = @SQL + 'SELECT ''' + COLUMN_NAME + ''' AS ColumnName, COUNT(*) - COUNT([' + COLUMN_NAME + ']) AS NullCount, SUM(CASE WHEN [' + COLUMN_NAME + '] = 0 THEN 1 ELSE 0 END) AS ZeroCount FROM [' + TABLE_SCHEMA + '].[' + TABLE_NAME + '] UNION ALL '
--FROM INFORMATION_SCHEMA.COLUMNS
--WHERE TABLE_NAME = @TableName AND DATA_TYPE IN ('int', 'decimal', 'numeric', 'float', 'money', 'smallint', 'tinyint', 'bigint', 'real');

--SET @SQL = LEFT(@SQL, LEN(@SQL) - 10); -- Remove the last 'UNION ALL'

--EXEC sp_executesql @SQL;
