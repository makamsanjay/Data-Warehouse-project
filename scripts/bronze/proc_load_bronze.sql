/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


DELIMITER $$

CREATE PROCEDURE bronze_load_bronze()
BEGIN
  DECLARE start_time DATETIME;
  DECLARE end_time DATETIME;
  DECLARE batch_start_time DATETIME;
  DECLARE batch_end_time DATETIME;

  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
      SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER' AS error;
    END;

  SET batch_start_time = NOW();

  -- CRM Tables
  SET start_time = NOW();
  TRUNCATE TABLE bronze.crm_cust_info;
  LOAD DATA LOCAL INFILE '/path/to/cust_info.csv'
  INTO TABLE bronze.crm_cust_info
  FIELDS TERMINATED BY ',' 
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS;
  SET end_time = NOW();

  SET start_time = NOW();
  TRUNCATE TABLE bronze.crm_prd_info;
  LOAD DATA LOCAL INFILE '/path/to/prd_info.csv'
  INTO TABLE bronze.crm_prd_info
  FIELDS TERMINATED BY ',' 
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS;
  SET end_time = NOW();

  SET start_time = NOW();
  TRUNCATE TABLE bronze.crm_sales_details;
  LOAD DATA LOCAL INFILE '/path/to/sales_details.csv'
  INTO TABLE bronze.crm_sales_details
  FIELDS TERMINATED BY ',' 
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS;
  SET end_time = NOW();

  -- ERP Tables
  SET start_time = NOW();
  TRUNCATE TABLE bronze.erp_loc_a101;
  LOAD DATA LOCAL INFILE '/path/to/loc_a101.csv'
  INTO TABLE bronze.erp_loc_a101
  FIELDS TERMINATED BY ',' 
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS;
  SET end_time = NOW();

  SET start_time = NOW();
  TRUNCATE TABLE bronze.erp_cust_az12;
  LOAD DATA LOCAL INFILE '/path/to/cust_az12.csv'
  INTO TABLE bronze.erp_cust_az12
  FIELDS TERMINATED BY ',' 
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS;
  SET end_time = NOW();

  SET start_time = NOW();
  TRUNCATE TABLE bronze.erp_px_cat_g1v2;
  LOAD DATA LOCAL INFILE '/path/to/px_cat_g1v2.csv'
  INTO TABLE bronze.erp_px_cat_g1v2
  FIELDS TERMINATED BY ',' 
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS;
  SET end_time = NOW();

  SET batch_end_time = NOW();

  SELECT CONCAT('Bronze load completed in ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS result;
END $$

DELIMITER ;

/*

LOAD DATA LOCAL INFILE '/tmp/PX_CAT_G1V2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(id, cat, subcat, maintenance);


LOAD DATA LOCAL INFILE '/Users/sanjaychariteshmakam/Downloads/data-warehouse-project/datasets/source_crm/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date);

LOAD DATA LOCAL INFILE '/Users/sanjaychariteshmakam/Downloads/data-warehouse-project/datasets/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;crm_prd_info


SELECT COUNT(*) FROM bronze.crm_cust_info;
SELECT COUNT(*) FROM bronze.crm_prd_info;
SELECT COUNT(*) FROM bronze.crm_sales_details;
SELECT COUNT(*) FROM bronze.erp_cust_az12;
SELECT COUNT(*) FROM bronze.erp_loc_a101;
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
*/
