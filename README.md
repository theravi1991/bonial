
# Robot Delivery Optimization with Citi Bike Data  
_A Spark + Prophet powered pipeline for logistics simulation & demand forecasting_

## Overview

This notebook builds a modular data pipeline to simulate and optimize a robot delivery fleet using Citi Bike trip data (Feb 2025). It leverages Apache Spark (PySpark) for distributed processing, integrates holiday metadata, performs time-series forecasting using Facebook Prophet, and simulates robot eligibility based on energy and location criteria.

All intermediate and final outputs are persisted in CSV and Parquet formats for downstream use.

## Pipeline Modules — Technical Breakdown

### 1. Environment Setup & Package Installation
- Installs exact dependency versions for reproducibility.
- Ensures compatibility between PySpark 3.5.0, Prophet 1.1.5, and pandas 2.2.2.

### 2. Spark Session Initialization
- Uses `local[*]` master with optimized configurations:
  - `spark.sql.shuffle.partitions = 200`
  - `spark.driver.memory = 4g`

### 3. Data Ingestion
- Loads raw Citi Bike trip data (Feb 2025) and US Holidays (2023) from Google Drive.
- Applies schema manually for consistency:
  [ride_id, rideable_type, started_at, ended_at, start_station_name, ...]

### 4. Data Quality Validations
Runs structured DQ checks with logging for traceability:
- Null checks on `started_at` and `ended_at`
- Timestamp conversion validation to ensure proper formatting
- Trip duration validation to flag unrealistic values outside 1s – 86400s
- Completeness checks on key fields: `ride_id`, `start_station_name`, `end_station_name`

All results are logged to a data quality report CSV.

Final clean dataset is persisted to `/outputFiles/validated_data/`

### 5. Holiday Mapping (Feature Enrichment)
- Converts holiday dates to proper `DateType`
- Joins on trip date to generate an `is_holiday` boolean flag
- Outputs stored in both CSV and Parquet formats

### 6. Demand Forecasting with Prophet
- Aggregates hourly trip volume using Spark’s `date_trunc('hour', starttime)`
- Converts Spark DataFrame to Pandas for Prophet compatibility
- Trains a Prophet model and forecasts the next 24 hours
- Outputs forecast in both CSV and Parquet

### 7. Robot Simulation
- Creates synthetic robot data with:
  - `robot_id`, `charge_level` (10–100%), `location` (area_1 to area_5)
- Ensures at least one robot in `area_1` is eligible
- Converts to Spark DataFrame for downstream use

### 8. Delivery Receipts Generator
- Extracts key delivery fields:
  - `delivery_id`, `delivery_time`, `from_location`, `to_location`, `receipt`
- Outputs are stored in `/outputFiles/receipts/`

### 9. Inter-City Trip Detection
- Filters rides where `start_station_id != end_station_id`
- Useful for identifying long-range deliveries

### 10. Robot Availability Filter
- Filters robots with `charge_level > 50%`
- Saves eligible fleet to `/outputFiles/available_robots/`

## Output Summary

| Module | Output Location | Format |
|--------|------------------|--------|
| Validated Trip Data | `/outputFiles/validated_data/` | CSV, Parquet |
| Holiday Trips | `/outputFiles/holiday_trip_data/` | CSV, Parquet |
| Forecasted Demand | `/outputFiles/forecast_data/` | CSV, Parquet |
| Robot Data | `/outputFiles/robot_data/` | CSV, Parquet |
| Receipts | `/outputFiles/receipts/` | CSV, Parquet |
| Inter-City Trips | `/outputFiles/inter_city_trips/` | CSV, Parquet |
| Available Robots | `/outputFiles/available_robots/` | CSV, Parquet |
| DQ Report | `/outputFiles/data_quality_report.csv` | CSV |

## Technology Stack

- Apache Spark (PySpark) – Scalable batch processing  
- Prophet – Time-series forecasting  
- Pandas, NumPy – Lightweight transformation and simulation  
- Google Colab + Drive – Cloud-hosted notebook environment

## Execution Instructions

1. Upload the notebook to Google Colab  
2. Mount Google Drive  
3. Adjust file paths as needed  
4. Run all cells in order  
5. Outputs will be saved to designated folders under `/outputFiles/`

## Final Remarks

This case study demonstrates a realistic application of Spark and machine learning to simulate and optimize logistics workflows. The modular pipeline enables:

- Integration with real-time APIs and streaming data sources
- Extension to live robot fleet data and delivery systems
- Scalability across larger geographies and timeframes
