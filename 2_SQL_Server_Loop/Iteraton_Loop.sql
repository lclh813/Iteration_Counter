DROP TABLE IF EXISTS #results
CREATE TABLE #results (
    [Alias_9] VARCHAR(20),
	[Alias_10] INT,
	[Alias_11] INT,
	[Alias_12] INT,
	[Alias_13] INT
)

DROP TABLE IF EXISTS #tables
SELECT TABLE_NAME
INTO #tables
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE='BASE TABLE'
AND TABLE_NAME LIKE '%TopSales%'

DECLARE @table_id INT = 0
DECLARE @curr_table VARCHAR(20)
WHILE @table_id < (SELECT COUNT(*) FROM #tables)
BEGIN 	
    SELECT @curr_table = [TABLE_NAME] 
	FROM #tables 
	ORDER BY [TABLE_NAME]
	OFFSET @table_id ROWS
	FETCH FIRST 1 ROWS ONLY	
	SET @table_id += 1

	DECLARE @iter INT = 1
	WHILE @iter < 5
	BEGIN
		DECLARE @SSQL VARCHAR(2000) = '
		INSERT INTO #results
		SELECT ''' +  @curr_table + ''' AS [Alias_9],'
		         + CAST(@iter AS VARCHAR(2)) + ' AS [Alias_10],
			   COUNT([Col_3]) AS [Alias_11],
			   SUM([B/A:Alias_8]) AS [Alias_12],
			   SUM([Col_18]) AS [Alias_13]	    
		FROM 
			(
			SELECT *
			FROM 
			(
				SELECT  [Col_3],
						[Col_4],
						[Col_18],
						[Col_41],
						[Col_42],
						[Col_43],
						[Alias_2],
						[Alias_4],
						[Alias_6],
						[Alias_7] AS [A:Alias_7],
						[Col_47] AS [B:Col_47],		   
						CEILING([Col_47] / [Alias_7]) AS [B/A:Alias_8]
				FROM
					(
					SELECT [Col_3],
						   [Col_4],
						   [Col_18],
						   [Col_47],
						   [Col_41,
						   [Col_42],
						   [Col_43],
						   [Alias_2],		   
						   [Alias_4],		   
						   [Alias_6],	
						   [Alias_2] * [Alias_4] * [Alias_6] AS [Alias_7]
					FROM 
						(
						SELECT [Col_3],
							   [Col_4],	
							   [Col_18],
							   [Col_47],
							   [Col_41],
							   1 / [Col_41] AS [Alias_1],
							   FLOOR(60 / [Col_41]) AS [Alias_2],
							   [Col_42],
							   2 / [Col_42] AS [Alias_3],
							   FLOOR(40 / [Col_42]) AS [Alias_4],
							   [Col_43],
							   3 / [Col_43] AS [Alias_5],
							   FLOOR(30 / [Col_43]) AS [Alias_6]							   
						FROM 
							(
							SELECT *
							FROM ' + @curr_table + '
							WHERE [Col_22] >= ' + CAST(@iter AS VARCHAR(2)) + '
							AND [Col_41] <= 1
							AND [Col_42] <= 2
							AND [Col_43] <= 3
							AND [Col_44] <= 4
							) AS filtered
						) AS count_capacity
					) AS count_item
				) AS count_box
			WHERE [A:Alias_7] > 1
			) AS filter_capacity
			'
		PRINT (@SSQL)	
		EXEC (@SSQL)
		SET @iter += 1
	END
END

SELECT *
FROM #results