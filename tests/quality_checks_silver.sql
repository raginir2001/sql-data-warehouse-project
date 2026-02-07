
/*
==========================================================================
Quality Checks
==========================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
   - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.

*/

--====================================================================
--Checking 'silver.crm_cust_info'
--====================================================================
--Check for NULLS Duplicates in Primary Key
--Expectation: No Results
SELECT
   cst_id,
   COUNT(*)
FROM silver.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) > 1 OR cust_id IS NULL;

--Check for Unwanted Spaces
--Expectation: No Results
SELECT
    cst key
FROM silver.crm_cust_info
WHERE cust key != TRIM(cust_key);

--Data Standardization & Consistency
SELECT DISTINCT
    cust_marital_status
FROM silver.crm_cust_info;

--=================================================================
--Checking 'silver.crm_prd_info"
--=================================================================
--Check for NULLS or  Duplicates in Primary Key
--Expectation: No Results
SELECT
    prd_id,
    COUNT(*)
FROM silver.crm_prd_info;
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--Check for Unwanted Spaces
--Expectation: No Results
SELECT
    prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--Check for NULLS or Negative Values in Cost
--Results Expectation: No Results
SELECT

67 prd cost

FROM silver.crm_prd_info

WHERE pro cost of prd cost IS MALL;

Data Standardization & Consistency 71

in main

Cancel changes Commit changes...

72 SELECT DISTINCT

73

prd line

74 FROM silver.cre_prd_info;

75

76 Check for Invalid Date Orders (Start Date End Date)

77 Expectation: No Results

78 SELECT

Use Contral Shift to toggle the tab






Data Standardization & Consistency

datasets

72 SELECT DESTINCT

23 prd line

> docs

74 FROM silver.crm_prd_info;

scripts

75

tests

Check for Invalid Date Orders (Start Date End Date)

79

73 Expectation: No Results

quality checks_silver.sql

78 SELECT

gitignore

FROM silver.crm_prd_info

LICENSE

82

31 WHERE prd end_dt prd start_dt;

83

Checking 'silver.crm_sales details

README.md

Check for Invalid Dates

requirements.txt

Expectation: No Invalid Dates

SELECT

NULLIF(sis_due dt, 6) AS sis_due_dt

FROM bronze.crm_sales_details

91 WHERE sis due dt = 0

192 OR LEN(Sls due dt) 1

93 Of sis due dt> 20500101

94 OR sis due dt 19000101;

95

06 Check for Invalid Date Orders (Order Date Shipping/Due Dates)

97 Expectation: No Results

98 SELECT

90

100 FROM silver.crm_sales_details

Use Control Shift to toggle the tab key moving focus. Alternatively,






FROM bronze.crm sales details

datasets

91 WHERE sis due dt

92 OR LEN(SIS due_dt) =

> docs

93 OR sis due dt 20500101

94 Of sis due dt < 19000101;

scripts

95

tests

06 Check for Invalid Date Orders (Order Date Shipping/Due Dates)

quality checks_silver.sql

SELECT

197 Expectation: No Results

gitignore

90

1400 FROM silver.crm_sales_details

LICENSE

101 WHERE sls order dt sis ship dt

README.md

102 om sls order_dt > sis_due_dt;

103

requirements.txt

104 Check Data Consistency: Sales Quantity Price

105 Expectation: No Results

106 SELECT DISTINCT

107 sis_sales,

108

sls quantity,

100 sis price

118 FROM silver.crm_sales_details

111 WHERE sls sales I sis quantity sis price

112 OR sis sales IS NULL

113 08 sls quantity IS NULL

114 On sis price IS NULL

115 O sls sales ca

116 of sis quantity

117 Of sls price c

118 ORDER BY sls sales, sIs_quantity, sls_price;




Preview

Q Go to file

107 108 119 122 130 135 136

sls sales,

109 sls price

sis quantity,

datasets

110 FROM silver.crm_sales_details

> docs

111 HERE sls sales I sis quantity sis price

scripts

112 Of sls sales 25 NULL

113 Of sis quantity IS NULL

tests

114 OR sis price IS NULL

quality_checks_silver.sql

115 On sis sales a

116 O sls quantity<

gitignore

117 OF sis price 0

LICENSE

118 ORDER BY sis sales, sis quantity, sis price;

120

README.md

121 Checking silver.erp cust az12'

requirements.txt

123 Identify Out-of-Range Dates

124 Expectation: Birthdates between 1924-01-01 and Today

125 SELECT DISTINCT

126 bdate

127 FROM silver.erp_cust_az12

128 WHERE bdate 1924-01-01

129 OR bdate GETDATE();

131 Data Standardization & Consistency

132 SELECT DISTINCT

133 gen

134 FROM silver.erp_cust_az12;

137 Checking 'silver.erp_loc_a101"

Use Contral Shift to toggle the tab key moving focus.



Checking 'silver.erp_cust_az12

122

datasets

123 Identify Out-of-Range Dates

124 Expectation: Birthdates between 1924-01-01 and Today

> docs

125 SELECT DISTINCT

scripts

126 bdate

127 FROM silver.erp_cust_az12

128 WHERE bdate 1924-01-01

quality_checks_silver.sql

130

129 on bdate GETDATE();

gitignore

131 Data Standardization & Consistency

LICENSE

133

gen

132 SELECT DISTINCT

README.md

134 FROM silver.erp_cust_az12;

135

requirements.txt

--==========================================================
--Checking 'silver.erp_loc_a101'
--==========================================================
--Data Standardization & Consistency
SELECT DISTINCT
   cntry
FROM silver.erp_loc_0101
ORDER BY entry;

--============================================================
--Checking 'silver.erp_pa_cat giv2'
--============================================================
--Check for Unwanted Spaces.
--Expectation: No Results
SELECT
     *
FROM silver_erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintanence)

-- Data Standardization & Consistency
SELECT DISTINCT
    maintanence 
FROM silver.erp_px_cat_g1v2;
