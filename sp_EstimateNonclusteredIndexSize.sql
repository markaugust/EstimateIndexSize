IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[sp_EstimateNonclusteredIndexSize]') AND type in (N'P', N'PC'))
BEGIN
EXEC dbo.sp_executesql @statement = N'CREATE PROCEDURE [dbo].[sp_EstimateNonclusteredIndexSize] AS'
END
GO
ALTER PROCEDURE [dbo].[sp_EstimateNonclusteredIndexSize]
    @databaseName nvarchar(max) = NULL,
    @schemaName nvarchar(max) = NULL,
    @tableName nvarchar(max) = NULL,
    @keyColumns nvarchar(max) = NULL,
    @includeColumns nvarchar(max) = NULL,
    @isUniqueIndex bit = 0,
    @fillFactor int = NULL,
    @debug bit = 0

AS

--==================================================================
-- Author:  Mark Earleywine
-- GitHub:  https://github.com/markaugust/EstimateIndexSize
-- Version: 2020-08-06
-- Desciption: This will give an estimate of how large a Nonclustered Index will be
--
-- Source of Calculations: https://docs.microsoft.com/en-us/sql/relational-databases/databases/estimate-the-size-of-a-nonclustered-index?view=sql-server-ver15
-- Parameters:
--      @databaseName - Database the index will be created in - Defaults to current DB context
--      @schemaName - Required - Schema the index will be created in
--      @tableName - Required - Table the index will be created in
--      @keyColumns - Required - Comma Separated List of Key Columns to be used
--      @includeColumns - Comma Separated List of Included Columns to be used
--      @isUniqueIndex - Is the index you are creating a Unique Index (e.g. CREATE UNIQUE NONCLUSTERED INDEX...) - Defaults to 0
--      @fillFactor - Fill Factor of the Index - If NULL will get system default fill factor - Defaults to NULL
--      @debug bit - If set to 1 then will display all variables used in calculation to verify against Microsoft documentation
--
-- Notes:
--      The parameters databaseName, schemaName, tableName, keyColumns, and includedColumns can be listed with or without '[]', as seen in the example
--      In my testing, all sizes have currently been slightly over-estimated
-- Example:
--      EXECUTE [dbo].[sp_EstimateNonclusteredIndexSize] @databaseName = 'StackOverflow2010', @schemaName = 'dbo', @tableName = 'Users', @keyColumns = 'LastAccessDate, [Id]', includeColumns = 'Age, [DisplayName], Location'
--==================================================================

SET NOCOUNT ON
--------------------------------
--DECLARE and Set Misc variables
--------------------------------
DECLARE @database_sp_executesql nvarchar(max)
DECLARE @sqlcmd nvarchar(max)
DECLARE @tableType int
DECLARE @isTableIndexUnique int
DECLARE @InfoMessage nvarchar(max)
DECLARE @exsits bit
DECLARE @badColumns nvarchar(max)

DECLARE @stringDelimiter nvarchar(max) = ','

--Step 1 variables
DECLARE @Num_Rows bigint = 0
DECLARE @Num_Key_Cols int = 0
DECLARE @Fixed_Key_Size int = 0
DECLARE @Num_Variable_Key_Cols int = 0
DECLARE @Max_Var_Key_Size int = 0
DECLARE @Index_Null_Bitmap int = 0
DECLARE @Variable_Key_Size int = 0
DECLARE @Index_Row_Size int = 0
DECLARE @Index_Rows_Per_Page int = 0
--Step 2 variables
DECLARE @Num_Leaf_Cols int = 0
DECLARE @Fixed_Leaf_Size int = 0
DECLARE @Num_Variable_Leaf_Cols int = 0
DECLARE @Max_Var_Leaf_Size int = 0
DECLARE @Leaf_Null_Bitmap int = 0
DECLARE @Variable_Leaf_Size int = 0
DECLARE @Leaf_Row_Size int = 0
DECLARE @Leaf_Rows_Per_Page int = 0
DECLARE @Free_Rows_Per_Page int = 0
DECLARE @Num_Leaf_Pages bigint = 0
DECLARE @Leaf_Space_Used bigint = 0
--Step 3 variables
DECLARE @Non_Leaf_Levels int = 0
DECLARE @Num_Index_Pages bigint = 0
DECLARE @Index_Space_Used bigint = 0
DECLARE @TotalSizeBytes bigint = 0

---------------------------------
--Create temp table to hold info
---------------------------------
IF OBJECT_ID('tempdb..#columnsTable') IS NOT NULL
    DROP TABLE #columnsTable

CREATE TABLE #columnsTable (
    columnName nvarchar(max),
    isKeyColumn bit,
    dataType nvarchar(128),
    isVariableLength bit,
    [maxLength] int,
    columnID int,
    isNullable varchar(3),
    avgLength int
)

--------------------------------
--Clean and Check Input Variables
--------------------------------
--Clean Input Variables

--DatabaseName
SET @databaseName = REPLACE(@databaseName, '[', '')
SET @databaseName = REPLACE(@databaseName, ']', '')
--SchemaName
SET @schemaName = REPLACE(@schemaName, '[', '')
SET @schemaName = REPLACE(@schemaName, ']', '')
--TableName
SET @tableName = REPLACE(@tableName, '[', '')
SET @tableName = REPLACE(@tableName, ']', '')
--Key columns
SET @keyColumns = REPLACE(@keyColumns, CHAR(10), '') --Removes any Line Feeds
SET @keyColumns = REPLACE(@keyColumns, CHAR(13), '') --Removes any Carriage Returns
SET @keyColumns = REPLACE(@keyColumns, '[', '')
SET @keyColumns = REPLACE(@keyColumns, ']', '')
WHILE CHARINDEX(@stringDelimiter + ' ', @keyColumns) > 0 SET @keyColumns = REPLACE(@keyColumns, @stringDelimiter + ' ', @stringDelimiter) --Removes spaces after comma
WHILE CHARINDEX(' ' + @stringDelimiter, @keyColumns) > 0 SET @keyColumns = REPLACE(@keyColumns, ' ' + @stringDelimiter, @stringDelimiter) --Removes spaces before comma
SET @keyColumns = LTRIM(RTRIM(@keyColumns))
--Included Columns
SET @includeColumns = REPLACE(@includeColumns, CHAR(10), '') --Removes any Line Feeds
SET @includeColumns = REPLACE(@includeColumns, CHAR(13), '') --Removes any Carriage Returns
SET @includeColumns = REPLACE(@includeColumns, '[', '')
SET @includeColumns = REPLACE(@includeColumns, ']', '')
WHILE CHARINDEX(@stringDelimiter + ' ', @includeColumns) > 0 SET @includeColumns = REPLACE(@includeColumns, @stringDelimiter + ' ', @stringDelimiter) --Removes spaces after comma
WHILE CHARINDEX(' ' + @stringDelimiter, @includeColumns) > 0 SET @includeColumns = REPLACE(@includeColumns, ' ' + @stringDelimiter, @stringDelimiter) --Removes spaces before comma
SET @includeColumns = LTRIM(RTRIM(@includeColumns))


--Make sure SchemaName, TableName, and KeyColumns are provided
IF @schemaName IS NULL or @tableName IS NULL or @keyColumns IS NULL
BEGIN
    RAISERROR('NULL values are not allowed for parameters @schemaName, @tableName, or @keyColumns', 16, 1)
    RETURN
END

IF @fillFactor < 0 OR @fillFactor > 100
BEGIN
    RAISERROR('Fill Factor must be between 0 and 100', 16, 1)
    RETURN
END

--Determine index fill factor
IF @fillFactor IS NULL
BEGIN
    SELECT @fillFactor = CAST(value_in_use as int) FROM sys.configurations WHERE name like 'fill factor%'
END
IF @fillFactor = 0
BEGIN
    SET @fillFactor = 100
END

--Set database context for queries
IF @databaseName IS NULL
BEGIN
    SET @InfoMessage = 'Parameter @databaseName was not provided. Using current database context.'
    RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT
    SET @databaseName = DB_NAME()
END
SET @database_sp_executesql = QUOTENAME(@databaseName) + '.sys.sp_executesql'

--------------------------------------------------------------
--Parse Columns into a table for calculations and manipulation
--------------------------------------------------------------
--Key columns
;WITH KeyColumns (StartPosition, EndPosition, ColumnName) AS
(
    SELECT 1 AS StartPosition,
        ISNULL(NULLIF(CHARINDEX(@stringDelimiter, @keyColumns, 1), 0), LEN(@keyColumns) + 1) AS EndPosition,
        SUBSTRING(@keyColumns, 1, ISNULL(NULLIF(CHARINDEX(@stringDelimiter, @keyColumns, 1), 0), LEN(@keyColumns) + 1) - 1) AS ColumnName
    WHERE @keyColumns IS NOT NULL
    UNION ALL
    SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
        ISNULL(NULLIF(CHARINDEX(@stringDelimiter, @keyColumns, EndPosition + 1), 0), LEN(@keyColumns) + 1) AS EndPosition,
        SUBSTRING(@keyColumns, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@stringDelimiter, @keyColumns, EndPosition + 1), 0), LEN(@keyColumns) + 1) - EndPosition - 1) AS ColumnName
    FROM KeyColumns
    WHERE EndPosition < LEN(@keyColumns) + 1
)
INSERT INTO #columnsTable (columnName, isKeyColumn)
SELECT ColumnName, 1 FROM KeyColumns

--Included Columns
;with IncludedColumns (StartPosition, EndPosition, ColumnName) AS
(
    SELECT 1 AS StartPosition,
        ISNULL(NULLIF(CHARINDEX(@stringDelimiter, @includeColumns, 1), 0), LEN(@includeColumns) + 1) AS EndPosition,
        SUBSTRING(@includeColumns, 1, ISNULL(NULLIF(CHARINDEX(@stringDelimiter, @includeColumns, 1), 0), LEN(@includeColumns) + 1) - 1) AS ColumnName
    where @includeColumns IS NOT NULL
    union all
    SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
        ISNULL(NULLIF(CHARINDEX(@stringDelimiter, @includeColumns, EndPosition + 1), 0), LEN(@includeColumns) + 1) AS EndPosition,
        SUBSTRING(@includeColumns, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(@stringDelimiter, @includeColumns, EndPosition + 1), 0), LEN(@includeColumns) + 1) - EndPosition - 1) AS ColumnName
    FROM IncludedColumns
    WHERE EndPosition < LEN(@includeColumns) + 1
)
INSERT INTO #columnsTable (columnName, isKeyColumn)
SELECT ColumnName, 0 FROM IncludedColumns

---------------------------------------------------
--Make sure things exist
---------------------------------------------------
--Does Database Exists
IF NOT EXISTS (select 1 from sys.databases where [name] = @databaseName)
BEGIN
    SET @InfoMessage = 'Database name provided (' + QUOTENAME(@databaseName) + ') does not exists'
    RAISERROR('%s', 16, 1, @InfoMessage) WITH NOWAIT
    RAISERROR('Exiting...', 10, 1) WITH NOWAIT
    RETURN
END

--Does Schema Exist
SET @exsits = 0
SET @sqlcmd = 'IF EXISTS (select 1 from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME = @ParamSchemaName) BEGIN SET @ParamExistsOUT = 1 END'
EXECUTE @database_sp_executesql @stmt = @sqlcmd, @params = N'@ParamSchemaName nvarchar(max), @ParamExistsOUT bit OUTPUT', @ParamSchemaName = @schemaName, @ParamExistsOUT = @exsits OUTPUT
IF @exsits = 0
BEGIN
    SET @InfoMessage = 'Schema name provided (' + QUOTENAME(@schemaName) + ') does not exists in the database (' + QUOTENAME(@databaseName) + ')'
    RAISERROR('%s', 16, 1, @InfoMessage) WITH NOWAIT
    RAISERROR('Exiting...', 10, 1) WITH NOWAIT
    RETURN
END

--Does Table Exists
SET @exsits = 0
SET @sqlcmd = 'IF EXISTS (select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME = @ParamTableName and TABLE_SCHEMA = @ParamSchemaName) BEGIN SET @ParamExistsOUT = 1 END'
EXECUTE @database_sp_executesql @stmt = @sqlcmd, @params = N'@ParamTableName nvarchar(max), @ParamSchemaName nvarchar(max), @ParamExistsOUT bit OUTPUT', @ParamTableName = @tableName, @ParamSchemaName = @schemaName, @ParamExistsOUT = @exsits OUTPUT
IF @exsits = 0
BEGIN
    SET @InfoMessage = 'Table name provided (' + QUOTENAME(@tableName) + ') does not exists within the schema provided (' + QUOTENAME(@schemaName) + ')'
    RAISERROR('%s', 16, 1, @InfoMessage) WITH NOWAIT
    RAISERROR('Exiting...', 10, 1) WITH NOWAIT
    RETURN
END

--Do All Columns exist
SET @sqlcmd = 'select @ParamBadColumnsOUT = STUFF((select '','' + QUOTENAME(ct.columnName) from #columnsTable ct left join INFORMATION_SCHEMA.COLUMNS c ON c.COLUMN_NAME = ct.columnName and c.TABLE_NAME = @ParamTableName and c.TABLE_SCHEMA = @ParamSchemaName where c.COLUMN_NAME IS NULL for xml path('''')), 1, 1, '''')'
EXECUTE @database_sp_executesql @stmt = @sqlcmd, @params = N'@ParamTableName nvarchar(max), @ParamSchemaName nvarchar(max), @ParamBadColumnsOUT nvarchar(max) OUTPUT', @ParamTableName = @tableName, @ParamSchemaName = @schemaName, @ParamBadColumnsOUT = @badColumns OUTPUT
IF @badColumns IS NOT NULL
BEGIN
    SET @InfoMessage = 'Column names provided (' + @badColumns + ') do not exist within the schema and table provided (' + QUOTENAME(@schemaName) + '.' + QUOTENAME(@tableName) + ')'
    RAISERROR('%s', 16, 1, @InfoMessage) WITH NOWAIT
    RAISERROR('Exiting...', 10, 1) WITH NOWAIT
    RETURN
END

---------------------------------------------------
--Print some info
---------------------------------------------------
SET @InfoMessage = 'Database: ' + QUOTENAME(@databaseName)
RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT
SET @InfoMessage = 'Schema: ' + QUOTENAME(@schemaName)
RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT
SET @InfoMessage = 'Table: ' + QUOTENAME(@tableName)
RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT
SET @InfoMessage = 'Key Columns: ' + @keyColumns
RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT
SET @InfoMessage = 'Included Columns: ' + CASE WHEN @includeColumns IS NULL THEN '(none)' ELSE @includeColumns END
RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT
SET @InfoMessage = 'Fill Factor set to ' + CAST(@fillFactor as nvarchar)
RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT

---------------------------------------------------
--Determine Table type and if Primary Key is unique
---------------------------------------------------
SET @sqlcmd = '
    select TOP 1 @typeOUT = index_id, @isUniqueOUT = is_unique
    from sys.indexes
    where OBJECT_NAME([object_id]) = @tableName AND OBJECT_SCHEMA_NAME([object_id]) = @schemaName
    order by index_id ASC'
EXECUTE @database_sp_executesql @stmt = @sqlcmd, @params = N'@schemaName nvarchar(max), @tableName nvarchar(max), @typeOUT int OUTPUT, @isUniqueOUT int OUTPUT', @schemaName = @schemaName, @tableName = @tableName, @typeOUT = @tableType OUTPUT, @isUniqueOUT = @isTableIndexUnique OUTPUT

----------------------------------
--Update temp table with columnIDs
----------------------------------
SET @sqlcmd = '
    UPDATE #columnsTable
    SET columnID = ORDINAL_POSITION
    from #columnsTable ct
    join INFORMATION_SCHEMA.COLUMNS c
        on ct.columnName = c.COLUMN_NAME and @schemaName = c.TABLE_SCHEMA and @tableName = c.TABLE_NAME
    join master.sys.types t
        on t.name = c.DATA_TYPE'
EXECUTE @database_sp_executesql @stmt = @sqlcmd, @params = N'@schemaName nvarchar(max), @tableName nvarchar(max)', @schemaName = @schemaName, @tableName = @tableName

----------------------------------------------------------------
--Insert any Primary Key Columns that aren't in the list already
--Won't return any columns if table is a heap
----------------------------------------------------------------
SET @sqlcmd = '
    select c.name, NULL, c.column_id
    from sys.index_columns ic
    join sys.columns c on ic.object_id = c.object_id AND ic.column_id = c.column_id
    where index_id = @tableType AND OBJECT_NAME(ic.[object_id]) = @tableName AND OBJECT_SCHEMA_NAME(ic.[object_id]) = @schemaName
        AND ic.column_id NOT IN (select columnID from #columnsTable where isKeyColumn = 1)'
INSERT INTO #columnsTable (columnName, isKeyColumn, columnID)
EXECUTE @database_sp_executesql @stmt = @sqlcmd, @params = N'@schemaName nvarchar(max), @tableName nvarchar(max), @tableType int', @schemaName = @schemaName, @tableName = @tableName, @tableType = @tableType

--------------------------------------------------------------
--Determine column data types and max lengths
--------------------------------------------------------------
SET @sqlcmd = '
UPDATE #columnsTable
SET dataType = c.DATA_TYPE
    , isVariableLength = CASE WHEN c.DATA_TYPE IN (''varchar'', ''nvarchar'', ''text'', ''ntext'', ''varbinary'', ''image'') THEN 1 ELSE 0 END
    , maxLength = CASE WHEN c.CHARACTER_MAXIMUM_LENGTH IS NULL THEN t.max_length ELSE c.CHARACTER_MAXIMUM_LENGTH END
    , isNullable = c.IS_NULLABLE
from #columnsTable ct
join INFORMATION_SCHEMA.COLUMNS c
    on ct.columnName = c.COLUMN_NAME and @schemaName = c.TABLE_SCHEMA and @tableName = c.TABLE_NAME
join master.sys.types t
    on t.name = c.DATA_TYPE'
EXECUTE @database_sp_executesql @stmt = @sqlcmd, @params = N'@schemaName nvarchar(max), @tableName nvarchar(max)', @schemaName = @schemaName, @tableName = @tableName

--------------------------------------------------------------
--Determine avgLength of variable columns
--------------------------------------------------------------
DECLARE @colName nvarchar(max)
DECLARE sql_cursor CURSOR
FOR
    SELECT columnName FROM #columnsTable WHERE isVariableLength = 1
OPEN sql_cursor
FETCH NEXT FROM sql_cursor INTO @colName
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @InfoMessage = 'Determining Average Length of variable length column ' + @colName
    RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT
    SET @sqlcmd = '
        UPDATE #columnsTable
        SET avgLength = (select AVG(DATALENGTH(' + QUOTENAME(@colName) + ')) from ' + QUOTENAME(@schemaName) + '.' + QUOTENAME(@tableName) + ' where ' + QUOTENAME(@colName) + ' IS NOT NULL)
        WHERE columnName = ''' + @colName + ''''
    EXECUTE @database_sp_executesql @stmt = @sqlcmd
    FETCH NEXT FROM sql_cursor INTO @colName
END
CLOSE sql_cursor
DEALLOCATE sql_cursor


---------
--Step 1 Calculate Variables
---------
--Step 1.1 Get Number of rows in table
SET @sqlcmd = 'select @numOut = count(*) from ' + QUOTENAME(@schemaName) + '.' + QUOTENAME(@tableName)
EXECUTE @database_sp_executesql @stmt = @sqlcmd, @params = N'@numOut bigint OUTPUT', @numOut = @Num_Rows OUTPUT

--Step 1.2 Get Key Column info
SELECT @Num_Key_Cols = COUNT(columnName) FROM #columnsTable WHERE isKeyColumn = 1
SELECT @Fixed_Key_Size = ISNULL(SUM([maxLength]),0) FROM #columnsTable WHERE isKeyColumn = 1 AND isVariableLength = 0
SELECT @Num_Variable_Key_Cols = COUNT(columnName) FROM #columnsTable WHERE isKeyColumn = 1 AND isVariableLength = 1
SELECT @Max_Var_Key_Size = ISNULL(SUM([maxLength]),0) FROM #columnsTable WHERE isKeyColumn = 1 AND isVariableLength = 1

--Step 1.3 Account for Data Row Locate if index being created is nonunique
IF @isUniqueIndex = 0
BEGIN
    --if table is a heap
    IF @tableType = 0
    BEGIN
        SET @Num_Key_Cols = @Num_Key_Cols + 1
        SET @Num_Variable_Key_Cols = @Num_Variable_Key_Cols + 1
        SET @Max_Var_Key_Size = @Max_Var_Key_Size + 8
    END
    --if table has clustered index
    IF @tableType = 1
    BEGIN
        SET @Num_Key_Cols = @Num_Key_Cols + (select count(*) from #columnsTable where isKeyColumn IS NULL) + CASE WHEN @isTableIndexUnique = 0 THEN 1 ELSE 0 END
        SET @Fixed_Key_Size = @Fixed_Key_Size + (select ISNULL(sum(maxLength),0) from #columnsTable where isKeyColumn IS NULL AND isVariableLength = 0)
        SET @Num_Variable_Key_Cols = @Num_Variable_Key_Cols + (select count(*) from #columnsTable where isKeyColumn IS NULL AND isVariableLength = 1) + CASE WHEN @isTableIndexUnique = 0 THEN 1 ELSE 0 END
        SET @Max_Var_Key_Size = @Max_Var_Key_Size + (select ISNULL(sum(maxLength),0) from #columnsTable where isKeyColumn IS NULL AND isVariableLength = 1) + CASE WHEN @isTableIndexUnique = 0 THEN 4 ELSE 0 END
    END
END

--Step 1.4 Account for Null Bitmap
IF (select count(*) from #columnsTable where (isKeyColumn = 1 OR isKeyColumn IS NULL) AND isNullable = 'YES') > 0
BEGIN
    SET @Index_Null_Bitmap = CAST(2 + (@Num_Key_Cols + 7)/8 as int)
END

--Step 1.5 Calculate Variable Length size
IF (select count(*) from #columnsTable where (isKeyColumn = 1 or isKeyColumn IS NULL) AND isVariableLength = 1) > 0
BEGIN
    SET @Variable_Key_Size = 2 + (@Num_Variable_Key_Cols * 2) + (@Max_Var_Key_Size * (select sum(avgLength) * 1.0 / sum([maxLength]) from #columnsTable where (isKeyColumn = 1 or isKeyColumn IS NULL) AND isVariableLength = 1)) -- * percentage full for better estimate otherwise this assumes all var sizes are 100% full
END

--Step 1.6 Calculate the row size
SET @Index_Row_Size = @Fixed_Key_Size + @Variable_Key_Size + @Index_Null_Bitmap + 7

--Step 1.7 Calculate number of index rows per page
SET @Index_Rows_Per_Page = 8096/(@Index_Row_Size + 2)

---------
--Step 2 Calculate the Space Used to Store Index Information in the Leaf Level
---------
--Step 2.1
SET @Num_Leaf_Cols = @Num_Key_Cols + (select count(*) from #columnsTable where isKeyColumn = 0)
SET @Fixed_Leaf_Size = @Fixed_Key_Size + (select isnull(sum([maxLength]),0) from #columnsTable where isKeyColumn = 0 and isVariableLength = 0)
SET @Num_Variable_Leaf_Cols = @Num_Variable_Key_Cols + (select count(*) from #columnsTable where isKeyColumn = 0 and isVariableLength = 1)
SET @Max_Var_Leaf_Size  = @Max_Var_Key_Size + (select isnull(sum([maxLength]),0) from #columnsTable where isKeyColumn = 0 and isVariableLength = 1)

--Step 2.2 Account for Data Row Locator
IF @isUniqueIndex = 1
BEGIN
    --Index is over a heap
    IF @tableType = 0
    BEGIN
        SET @Num_Leaf_Cols = @Num_Leaf_Cols + 1
        SET @Num_Variable_Leaf_Cols = @Num_Variable_Leaf_Cols + 1
        SET @Max_Var_Leaf_Size = @Max_Var_Leaf_Size + 8
    END
    --Index is over a clustered index
    IF @tableType = 1
    BEGIN
        SET @Num_Leaf_Cols = @Num_Leaf_Cols + (select count(*) from #columnsTable where isKeyColumn IS NULL) + CASE WHEN @isTableIndexUnique = 0 THEN 1 ELSE 0 END
        SET @Fixed_Leaf_Size = @Fixed_Leaf_Size + (select count(*) from #columnsTable where isKeyColumn IS NULL AND isVariableLength = 0)
        SET @Num_Variable_Leaf_Cols = @Num_Variable_Leaf_Cols + (select count(*) from #columnsTable where isKeyColumn IS NULL AND isVariableLength = 1) + CASE WHEN @isTableIndexUnique = 0 THEN 1 ELSE 0 END
        SET @Max_Var_Leaf_Size = @Max_Var_Leaf_Size + (select isnull(sum(maxLength),0) from #columnsTable where isKeyColumn IS NULL AND isVariableLength = 1) + CASE WHEN @isTableIndexUnique = 0 THEN 4 ELSE 0 END
    END
END

--Step 2.3 Calculate null bitmap
SET @Leaf_Null_Bitmap = 2 + ((@Num_Leaf_Cols + 7)/8)

--Step 2.4 Calculate variable length data size
IF (select count(*) from #columnsTable where isVariableLength = 1) > 0
BEGIN
    SET @Variable_Leaf_Size = 2 + (@Num_Variable_Leaf_Cols * 2) + (@Max_Var_Leaf_Size * (select sum(avgLength) * 1.0 / sum([maxLength]) from #columnsTable where isVariableLength = 1)) -- * percentage full for better estimate otherwise this assumes all var sizes are 100% full
END

--Step 2.5 Calculate index row size
SET @Leaf_Row_Size = @Fixed_Leaf_Size + @Variable_Leaf_Size + @Leaf_Null_Bitmap + 1

--Step 2.6 Calculate Index Rows per Page
SET @Leaf_Rows_Per_Page = 8096 / (@Leaf_Row_Size + 2)

--Step 2.7 Calculate number of reserved free rows per page based on fill factor
SET @Free_Rows_Per_Page = 8096 * ((100 - @fillFactor)/100)/(@Leaf_Row_Size + 2)

--Step 2.8 Calculate number of pages required
SET @Num_Leaf_Pages = @Num_Rows / (@Leaf_Rows_Per_Page - @Free_Rows_Per_Page) + 1 --rounded up to nearest whole page using + 1 as int will just truncate decimal

--Step 2.9 Calculate the size of the index leaf pages
SET @Leaf_Space_Used = 8192 * @Num_Leaf_Pages

---------
--Step 3 Calculate the Space Used to Store Index Information in the Non-leaf Levels
---------
--Step 3.1 Calculate number of non-leaf levels
SET @Non_Leaf_Levels = 1 + LOG(@Num_Leaf_Pages * 1.0 /@Index_Rows_Per_Page, @Index_Rows_Per_Page) + 1 --rounded up to nearest whole page using + 1 as int will just truncate decimal

--Step 3.2 Calculate the number of non-leaf pages in the index
DECLARE @a int = 1
WHILE @a <= @Non_Leaf_Levels
BEGIN
    SET @Num_Index_Pages = @Num_Index_Pages + ((@Num_Leaf_Pages/POWER(@Index_Rows_Per_Page,@a)) + 1) --rounded up to nearest whole page using + 1 as int will just truncate decimal
    SET @a = @a + 1
END

--Step 3.3 Calculate size of index
SET @Index_Space_Used = 8192 * @Num_Index_Pages

---------
--Step 4 Total Calculated Values
---------
SET @TotalSizeBytes = @Leaf_Space_Used + @Index_Space_Used

select @TotalSizeBytes as EstTotalBytes, CAST(@TotalSizeBytes*1.0/1024 as decimal(18,2)) as EstTotalKBs, CAST(@TotalSizeBytes*1.0/1024/1024 as decimal(18,2)) as EstTotalMBs

SET @InfoMessage = 'Done!'
RAISERROR('%s',10,1,@InfoMessage) WITH NOWAIT


select * from #columnsTable

IF @Debug = 1
BEGIN
    select @fillFactor as [fillFactor], @Num_Rows as Num_Rows, @Num_Key_Cols as Num_Key_Cols, @Fixed_Key_Size as Fixed_Key_Size, @Num_Variable_Key_Cols as Num_Variable_Key_Cols, @Max_Var_Key_Size as Max_Var_Key_Size, @Index_Null_Bitmap as Index_Null_Bitmap, @Variable_Key_Size as Variable_Key_Size, @Index_Row_Size as Index_Row_Size, @Index_Rows_Per_Page as Index_Rows_Per_Page
    select @Num_Leaf_Cols as Num_Leaf_Cols, @Fixed_Leaf_Size as Fixed_Leaf_Size, @Num_Variable_Leaf_Cols as Num_Variable_Leaf_Cols, @Max_Var_Leaf_Size as Max_Var_Leaf_Size, @Leaf_Null_Bitmap as Leaf_Null_Bitmap, @Variable_Leaf_Size as Variable_Leaf_Size, @Leaf_Row_Size as Leaf_Row_Size, @Leaf_Rows_Per_Page as Leaf_Rows_Per_Page, @Free_Rows_Per_Page as Free_Rows_Per_Page, @Num_Leaf_Pages as Num_Leaf_Pages, @Leaf_Space_Used as Leaf_Space_Used
    select @Non_Leaf_Levels as Non_Leaf_Levels, @Num_Index_Pages as Num_Index_Pages, @Index_Space_Used as Index_Space_Used
END