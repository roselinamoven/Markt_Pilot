# MARKT-PILOT ETL Pipeline

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline for **MARKT-PILOT**, a company providing market intelligence solutions for pricing transparency in the manufacturing industry. The ETL pipeline extracts data from MongoDB collections, transforms it into a relational format, and loads it into a PostgreSQL database to facilitate analytics and answer key business questions.

### Key Objectives
- **Extract** data from MongoDB collections.
- **Transform** the data into a relational structure while maintaining referential integrity.
- **Load** the transformed data into PostgreSQL optimized for analytical queries.
- Provide insights such as:
  - Number of results per part, shop, and country.
  - How part prices evolve over time.

---

## Features

- **Data Extraction**: Connects to MongoDB to extract data from the following collections:
  - `clients` 
  - `suppliers`
  - `sonar_runs`
  - `sonar_results`

- **Data Transformation**: Converts extracted JSON data into pandas DataFrames, normalizing nested structures and cleaning data.

- **Data Loading**: Loads transformed data into a PostgreSQL database with appropriate schema definitions, including:
  - `clients_table`
  - `suppliers_table`
  - `sonar_runs_table`
  - `sonar_results_table`
  - `sonar_run_suppliers_table`

- **Error Handling**: Implements error handling during data extraction and database operations.


## Data Description

The project involves four MongoDB collections:

1. **Clients**
   - Fields: `client_id`, `client_name`, `location`
   - Describes the clients of the platform.
   
2. **Suppliers**
   - Fields: `supplier_id`, `supplier_name`, `country`
   - Lists the web shops where pricing data is scraped.
   
3. **Sonar Runs**
   - Fields: `sonar_run_id`, `client_id`, `supplier_ids`, `date`, `status`
   - Represents price research runs for specific clients.
   - Relationships:
     - `client_id` references the `clients` collection.
     - `supplier_ids` references the `suppliers` collection.
   
4. **Sonar Results**
   - Fields: `sonar_result_id`, `sonar_run_id`, `part_number`, `supplier_id`, `price`, `lead_time`, `date`
   - Contains price information for parts found during a sonar run.
   - Relationships:
     - `sonar_run_id` references the `sonar_runs` collection.
     - `supplier_id` references the `suppliers` collection.

---

## Relational Database Schema

The data is transformed and loaded into a PostgreSQL database, using the following schema:

1. **Clients Table**
   - `client_id` (Primary Key)
   - `client_name`
   - `country`

2. **Suppliers Table**
   - `supplier_id` (Primary Key)
   - `supplier_name`
   - `country`

3. **Sonar Runs Table**
   - `sonar_run_id` (Primary Key)
   - `client_id` (Foreign Key referencing `clients.client_id`)
   - `date`
   - `status`
   - **Associative Table**: `sonar_run_suppliers`
     - `sonar_run_id` (Foreign Key referencing `sonar_runs.sonar_run_id`)
     - `supplier_id` (Foreign Key referencing `suppliers.supplier_id`)

4. **Sonar Results Table**
   - `sonar_result_id` (Primary Key)
   - `sonar_run_id` (Foreign Key referencing `sonar_runs.sonar_run_id`)
   - `supplier_id` (Foreign Key referencing `suppliers.supplier_id`)
   - `part_number`
   - `price`

---

## ETL Pipeline Design

### 1. Extract
- Data is extracted from MongoDB collections using the PyMongo library.
- The data is then loaded into Pandas DataFrames for processing.

### 2. Transform
- Flatten the embedded fields (e.g., `supplier_ids` array in `sonar_runs`).
- Handle missing values and ensure data type consistency.
- Ensure referential integrity between tables.

### 3. Load
- Load the transformed data into PostgreSQL using `psycopg2`.
- Tables are populated in a specific order to ensure data dependencies are respected:
  1. Clients
  2. Suppliers
  3. Sonar Runs
  4. Sonar Run Suppliers (Associative Table)
  5. Sonar Results

---

## Setup and Requirements

### Prerequisites
- Python 3.x
- MongoDB (for initial extraction)
- PostgreSQL (for loading transformed data)

### Python Libraries
- PyMongo
- Pandas
- Psycopg2
- JSON

### Steps to Run the Pipeline
1. **Replace Username,Password and Database Name**:
-replace the username and password in the config.json file to the actual username and pasword
-replace database name with your database name 
2. **Replace Username,Password and Database Name in Mongodb connection string**:
-replace the username and password in the mongodb connection string to the actual username and pasword
-replace database name with your database name 
3. **Clone the repository**:
   ```bash
   git clone <repository_url>
   cd <project_directory>
