# Pyspark Delta Medallion - AB InBev business case

Data pipeline for beverage sales analysis developed as a solution for AB InBev's data engineering business case using medallion architecture with PySpark and Delta Lake.

## Project context

This project was developed as a response to AB InBev's data engineering business case, implementing a complete data pipeline to process and analyze beverage sales information. The goal is to transform raw sales data into actionable insights for business decision-making.

## Technology stack

- **Platform**: Databricks on Azure stack
- **Processing**: PySpark for distributed transformations
- **Storage**: Delta Lake for ACID-compliant tables
- **Metastore**: Hive Metastore (Unity Catalog not available in trial version)
- **File system**: HDFS/DBFS for data storage

## Medallion architecture

The project implements the three-layer medallion architecture, promoting progressive data refinement from ingestion to business insights:

### Bronze layer - raw data

First ingestion layer where data is loaded from original sources with minimal transformations.

**Function**: preserve raw data for audit and future reprocessing

**Ingested datasets**:
- **beverage_sales**: transactional sales data including date, brand, region, channel, package and volume
- **beverage_channel_group**: hierarchical mapping of sales channels (channel description, trade group and trade type)

**Implementation**:

`bronze/beverage_sales.ipynb`:
- Reads CSV file with UTF-16 encoding and tab separator
- Applies column name cleaning function to snake_case
- Removes special characters and extra spaces
- Writes as Delta table: `abinbev_case_bronze.beverage_sales`

`bronze/beverage_channel_group.ipynb`:
- Reads CSV file with UTF-8 encoding and comma separator
- Maintains original data structure
- Writes as Delta table: `abinbev_case_bronze.beverage_channel_group`

**Benefits**:
- Original data preserved without information loss
- Enables reprocessing in case of errors in upper layers
- Complete data audit and traceability

### Silver layer - refined data

Second layer where data is cleaned, standardized and modeled into a dimensional schema optimized for analytics.

**Function**: create reliable and consistent dimensional model for analytical consumption

**Dimensional modeling**:

Implementation of star schema with two dimension tables and one fact table:

**Dimensions**:

`silver/dim_brand.ipynb` - **dim_brand**:
- Extracts unique brands from sales table
- Applies normalization: trim and uppercase
- Generates surrogate key using CRC32 function to create brand_id
- Removes duplicates
- Table: `abinbev_case_silver.dim_brand`
- Fields: brand_id, brand_nm

`silver/dim_channel.ipynb` - **dim_channel**:
- Extracts channel hierarchy from mapping table
- Normalizes descriptions: trim and uppercase at all levels
- Maintains three hierarchical levels: channel, trade group and type
- Removes duplicates
- Table: `abinbev_case_silver.dim_channel`
- Fields: trade_chnl_desc, trade_group_desc, trade_type_desc

**Fact**:

`silver/fact_sales.ipynb` - **fact_sales**:
- Enriches sales with left join on channel information
- Converts date field to standard date type (M/d/yyyy format)
- Normalizes all textual dimensions (trim and uppercase)
- Adds temporal partition columns: year_partition, month_partition, day_partition
- Writes partitioned by year/month/day for query optimization
- Table: `abinbev_case_silver.fact_sales`
- Metrics: sales volume
- Dimensions: date, brand, region, channel, package, trade group, trade type

**Applied transformations**:
- Data type conversion and standardization
- String normalization (space removal, uppercase)
- Enrichment via joins
- Surrogate key generation
- Addition of technical columns for partitioning
- Duplicate removal

**Benefits**:
- Dimensional model facilitates analytical queries
- Clean and consistent data
- Partitions dramatically improve performance
- Schema optimized for BI tools

### Gold layer - business insights

Third layer containing pre-calculated aggregations and business metrics ready for consumption.

**Function**: provide specific insights answering business questions

**Implemented analyses**:

**1. Top 3 trade groups by region** (`gold/4.1_top3_tradegroup_region.ipynb`):

*Business question*: what are the three main trade groups in each region based on sales volume?

*Implementation*:
- Aggregates total volume by region (btlr_org_lvl_c_desc) and trade group (trade_group_desc)
- Applies window function with partitioning by region
- Ranks groups using dense_rank ordered by descending volume
- Filters only top 3 from each region
- Table: `abinbev_case_gold.v_top3_tradegroup_region`

*Business value*: identifies priority channels for regionalized distribution and marketing strategies

**2. Sales by brand and month** (`gold/4.2_sales_by_brand_month.ipynb`):

*Business question*: what is the temporal evolution of each brand's sales throughout the months?

*Implementation*:
- Aggregates total volume by year, month and brand
- Orders chronologically to facilitate trend analysis
- Table: `abinbev_case_gold.v_sales_by_brand_month`

*Business value*: enables seasonality analysis, trend identification and production planning

**3. Lowest volume brand by region** (`gold/4.3_lowest_brand_by_region.ipynb`):

*Business question*: which brand has the lowest sales volume in each region?

*Implementation*:
- Aggregates total volume by region and brand
- Applies window function partitioned by region
- Ranks brands using dense_rank ordered by ascending volume
- Filters only the lowest volume brand (rank = 1) in each region
- Table: `abinbev_case_gold.v_lowest_brand_by_region`

*Business value*: identifies brands with low regional penetration for turnaround actions or discontinuation

**Benefits**:
- Pre-calculated queries reduce response time
- Standardized metrics ensure consistency across users
- Ready for integration with dashboards and BI tools
- Directly answers specific business questions

## Configuration and setup

### Initialization scripts

The `scripts.ipynb` file contains all necessary commands to prepare the environment:

**1. Schema configuration in Hive Metastore**:
```sql
USE CATALOG hive_metastore;
CREATE SCHEMA IF NOT EXISTS abinbev_case_bronze;
CREATE SCHEMA IF NOT EXISTS abinbev_case_silver;
CREATE SCHEMA IF NOT EXISTS abinbev_case_gold;
USE SCHEMA abinbev_case_bronze;
```

**2. Validation commands**:
- DESCRIBE TABLE to validate bronze table structure

### Data loading

**CSV files upload to DBFS**:

The source CSV files were uploaded to DBFS through Databricks UI using the "Add Data" feature:
- Navigate to Data tab in Databricks workspace
- Click "Add Data" or "Create Table" 
- Upload files directly through the UI
- Files are automatically stored in `dbfs:/FileStore/tables/`

This approach was used instead of programmatic upload for simplicity in the development environment.

**Note on data storage**:
- Source CSV files: `dbfs:/FileStore/tables/`
- Delta tables: stored in default Hive warehouse location (`/user/hive/warehouse/[schema_name].db/[table_name]/`)
- Since tables are created using `.saveAsTable()` without explicit path specification, they use the Hive Metastore default location

### Data requirements

**Source files** (located in `dbfs:/FileStore/tables/`):
- `abi_bus_case1_beverage_sales_20210726.csv`
  - Encoding: UTF-16
  - Separator: tab (\t)
  - Header: yes
  - InferSchema: yes

- `abi_bus_case1_beverage_channel_group_20210726.csv`
  - Encoding: UTF-8
  - Separator: comma (,)
  - Header: yes
  - InferSchema: yes

## Execution guide

Execute the following steps in order to build the complete pipeline:

**Data preparation**:
0. Upload CSV files to DBFS via Databricks UI "Add Data" feature (files will be stored in `dbfs:/FileStore/tables/`)

**Initial setup**:
1. `scripts.ipynb` - configures schemas and directories

**Bronze layer** (ingestion):
2. `bronze/beverage_sales.ipynb` - loads sales data
3. `bronze/beverage_channel_group.ipynb` - loads channel data

**Silver layer** (transformation):
4. `silver/dim_brand.ipynb` - creates brand dimension
5. `silver/dim_channel.ipynb` - creates channel dimension
6. `silver/fact_sales.ipynb` - creates enriched fact table

**Gold layer** (aggregation):
7. `gold/4.1_top3_tradegroup_region.ipynb` - top 3 groups by region
8. `gold/4.2_sales_by_brand_month.ipynb` - sales by brand and month
9. `gold/4.3_lowest_brand_by_region.ipynb` - lowest brand by region

## Technical decisions

### Delta Lake format

Chosen for providing:
- ACID transactions in data lake
- Time travel for audit and recovery
- Schema evolution to add future columns
- Better read performance compared to pure Parquet
- Automatic small file optimization

### Partitioning strategy

Fact table partitioned by year, month and day because:
- Drastically reduces amount of data read (partition pruning)
- Facilitates maintenance and historical data retention
- Common to perform monthly or daily aggregated analyses

### Data normalization

Consistent application of trim and uppercase on strings because:
- Eliminates inconsistencies caused by extra spaces
- Standardizes comparisons and joins
- Avoids duplicates caused by case differences
- Facilitates search and filtering by end users

### Overwrite mode

All writes use overwrite mode to:
- Guarantee idempotency (reruns produce same result)
- Facilitate complete reprocessing when necessary
- Simplify logic (no need for merge/upsert in this case)
- Adequate for this batch processing full refresh scenario

### Window functions

Used for ranking calculations because:
- Allow ranking within partitions (by region)
- More efficient than correlated subqueries
- dense_rank chosen to handle ties appropriately
- More readable and declarative code

## Table structure

### Bronze

**abinbev_case_bronze.beverage_sales**:
- date, brand_nm, ce_brand_flvr, btlr_org_lvl_c_desc, chnl_group, trade_chnl_desc, pkg_cat, pkg_cat_desc, tsr_pckg_nm, _volume, period

**abinbev_case_bronze.beverage_channel_group**:
- TRADE_CHNL_DESC, TRADE_GROUP_DESC, TRADE_TYPE_DESC

### Silver

**abinbev_case_silver.dim_brand**:
- brand_id (surrogate key)
- brand_nm

**abinbev_case_silver.dim_channel**:
- trade_chnl_desc
- trade_group_desc
- trade_type_desc

**abinbev_case_silver.fact_sales** (partitioned):
- sale_date, brand_nm, brand_flvr_id, btlr_org_lvl_c_desc, chnl_group, trade_chnl_desc, pkg_cat, pkg_cat_desc, tsr_pckg_nm, volume, period, trade_group_desc, trade_type_desc
- year_partition, month_partition, day_partition

### Gold

**abinbev_case_gold.v_top3_tradegroup_region**:
- btlr_org_lvl_c_desc, trade_group_desc, total_volume, rank

**abinbev_case_gold.v_sales_by_brand_month**:
- year, month, brand_nm, total_volume

**abinbev_case_gold.v_lowest_brand_by_region**:
- btlr_org_lvl_c_desc, brand_nm, total_volume


