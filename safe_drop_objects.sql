
/* =========================================================
   Safe Drop Script - SQL Server
   Drops objects only if they exist
   ========================================================= */

-- Drop View
DROP VIEW IF EXISTS dbo.MyView;
GO

-- Drop Stored Procedure
DROP PROCEDURE IF EXISTS dbo.MyProcedure;
GO

-- Drop Table
DROP TABLE IF EXISTS dbo.MyTable;
GO

-- Drop Schema (must be empty)
IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'MySchema')
BEGIN
    DROP SCHEMA MySchema;
END
GO
