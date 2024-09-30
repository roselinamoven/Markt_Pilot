import os
import json
import pandas as pd
import psycopg2
from bson import ObjectId
from psycopg2 import sql 
from pymongo import MongoClient
from pymongo.errors import PyMongoError

def main():

    #
    username = "roselinam"
    password = "1234"   
    database_name = "Markt-Pilot"
    data = extract_data_from_mongodb(username, password, database_name)
    # if data:
    # Print extracted data for each collection
    #  for collection_name, collection_data in data.items():
    #     print(f"\nData from collection '{collection_name}':")
    #     print(json.dumps(collection_data, indent=4, default=str))  # Pretty print the JSON data

#  if data:
#     # Print extracted data for each collection
#     for collection_name, collection_data in data.items():
#         print(f"\nData from collection '{collection_name}':")
#         print(json.dumps(collection_data, indent=4, default=str))  # Pretty print the JSON data


#     # Extract data from each collection
#     for collection_name in collections:
#         collection = db[collection_name]
#         data[collection_name] = list(collection.find({}))

#     # Optionally, convert ObjectId to string if needed
#     for collection_name in data:
#         for record in data[collection_name]:
#             if '_id' in record:
#                 record['_id'] = str(record['_id'])

#     # Save the extracted data to JSON files
#     for collection_name, records in data.items():
#         with open(f"{collection_name}.json", "w") as json_file:
#             json.dump(records, json_file, indent=4)

#     print("Data extracted and saved to JSON files.")  

    # Extract
    clients_data, suppliers_data, sonar_runs_data, sonar_results_data = extract_data()

    # Transform
    clients_df, suppliers_df, sonar_runs_df, sonar_results_df = transform_data(clients_data, suppliers_data, sonar_runs_data, sonar_results_data)

    # Clean the data
    clients_df, suppliers_df, sonar_runs_df, sonar_results_df = clean_data(clients_df, suppliers_df, sonar_runs_df, sonar_results_df)
   
    create_tables()
    # Load the data into PostgreSQL
    if not sonar_results_df.empty:
        load_to_postgresql(clients_df, suppliers_df, sonar_runs_df, sonar_results_df)
    else:
        print("No sonar results to load into PostgreSQL.")
 


import json
import os

def extract_data_from_mongodb(username, password, database_name):
    """
    Extract data from specified MongoDB collections and return them as dictionaries.

    :param username: MongoDB username
    :param password: MongoDB password
    :param database_name: Name of the MongoDB database
    :return: Dictionary containing collections and their data
    """
    mongo_uri = f"mongodb://{username}:{password}@localhost:27017/"
    collections = ['clients', 'suppliers', 'sonar_runs', 'sonar_results']
    extracted_data = {}

    try:
        # Connect to MongoDB
        client = MongoClient(mongo_uri)
        client.admin.command('ping')  # Check connectivity
        print("Connected to MongoDB successfully.")

        db = client[database_name]

        # Extract data from each specified collection
        for collection_name in collections:
            collection = db[collection_name]
            extracted_data[collection_name] = list(collection.find({}))  # Convert cursor to list

            # Save extracted data to JSON files
            with open(os.path.join('collections', f'{collection_name}.json'), 'w') as json_file:
                json.dump(extracted_data[collection_name], json_file, default=str)  # Use default=str to handle ObjectId

        print("Data extracted and saved to JSON files successfully.")
        return extracted_data

    except PyMongoError as e:
        print(f"An error occurred: {e}")
        return None


# Function to read and parse the JSON files from the collections folder
def load_json_data(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)
    
def load_config(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

# Extract: Read MongoDB collections (JSON files) and convert to DataFrames
def extract_data():
    collections_path = 'collections'
    
    clients_data = load_json_data(os.path.join(collections_path, 'clients.json'))
    suppliers_data = load_json_data(os.path.join(collections_path, 'suppliers.json'))
    sonar_runs_data = load_json_data(os.path.join(collections_path, 'sonar_runs.json'))
    sonar_results_data = load_json_data(os.path.join(collections_path, 'sonar_results.json'))
    return clients_data, suppliers_data, sonar_runs_data, sonar_results_data

# Transform: Convert JSON data to DataFrames
def transform_data(clients, suppliers, sonar_runs, sonar_results):
    clients_df = pd.json_normalize(clients)
    suppliers_df = pd.json_normalize(suppliers)
    sonar_runs_df = pd.json_normalize(sonar_runs)

    # Display the DataFrames and their columns
    print("Clients DataFrame:")
    print(clients_df.head())
    print(clients_df.columns)

    print("Suppliers DataFrame:")
    print(suppliers_df.head())
    print(suppliers_df.columns)

    print("Sonar Runs DataFrame:")
    print(sonar_runs_df.head())
    print(sonar_runs_df.columns)

    # Normalize sonar_results
    sonar_results_df = pd.json_normalize(
        sonar_results,
        sep='.',
        record_path=None,
        meta=['price_norm'],
        errors='ignore'
    )

    print("Sonar Results DataFrame:")
    print(sonar_results_df.head())
    print(sonar_results_df.columns)

    # Ensure that the nested $oid values are flattened into separate columns
    if '_id' in sonar_results_df.columns:
        sonar_results_df['_id'] = sonar_results_df['_id']
    if 'part_id' in sonar_results_df.columns:
        sonar_results_df['part_id'] = sonar_results_df['part_id']
    if 'supplier_id' in sonar_results_df.columns:
        sonar_results_df['supplier_id'] = sonar_results_df['supplier_id']
    if 'sonar_run_id' in sonar_results_df.columns:
        sonar_results_df['sonar_run_id'] = sonar_results_df['sonar_run_id']
    
    # Debugging: Check if sonar_results_df is empty 
    if sonar_results_df.empty:
        print("sonar_results_df is empty after transformation. Check the structure of sonar_results.")
    
    # Debugging: Check the final structure of the DataFrame
    print("Transformed sonar_results DataFrame:")
    print(sonar_results_df.info()) 
    return clients_df, suppliers_df, sonar_runs_df, sonar_results_df


# Clean the data
def clean_data(clients_df, suppliers_df, sonar_runs_df, sonar_results_df):
    # Drop duplicates based on IDs
    clients_df.drop_duplicates(subset=['_id'], inplace=True)
    suppliers_df.drop_duplicates(subset=['_id'], inplace=True)

    # Convert date columns to datetime format
    sonar_runs_df['date'] = pd.to_datetime(sonar_runs_df['date'], errors='coerce')
    clients_df['contract_start'] = pd.to_datetime(clients_df['contract_start'], errors='coerce')

    # Ensure foreign keys match between sonar runs and suppliers
    valid_suppliers = suppliers_df['_id'].unique()
    print(f"Valid suppliers: {valid_suppliers}")

    # Extract supplier IDs from dictionaries in supplier_ids
    sonar_runs_df['supplier_ids'] = sonar_runs_df['supplier_ids'].apply(
        lambda x: [supplier for supplier in x if isinstance(supplier, str)]
    )
    
    # Debugging: supplier_ids format after extraction
    print(f"Sample extracted supplier_ids from sonar_runs_df: {sonar_runs_df['supplier_ids'].head(5)}")
    print(f"Before supplier filtering, sonar_runs_df has {len(sonar_runs_df)} rows")
    
    # Filter sonar_runs_df for valid suppliers
    sonar_runs_df = sonar_runs_df[sonar_runs_df['supplier_ids'].apply(
        lambda x: any(supplier in valid_suppliers for supplier in x)
    )]

    print(f"After supplier filtering, sonar_runs_df has {len(sonar_runs_df)} rows")

    # Clean sonar results data
    valid_sonar_run_ids = sonar_runs_df['_id'].unique()
    print(f"Valid sonar run IDs: {valid_sonar_run_ids}")
    
    # Ensure sonar_run_id is of string type
    sonar_results_df['sonar_run_id'] = sonar_results_df['sonar_run_id'].astype(str)
    
    # Check if there are any null or problematic sonar_run_id entries before filtering
    print(f"Before filtering, sonar_results_df has {len(sonar_results_df)} rows")
    
    # Filter sonar_results_df based on valid sonar_run_ids
    sonar_results_df = sonar_results_df[sonar_results_df['sonar_run_id'].isin(valid_sonar_run_ids)]

    # Print the result after filtering
    print(f"After filtering, sonar_results_df has {len(sonar_results_df)} rows")
    
    # Handle duplicates
    sonar_results_df = sonar_results_df.drop_duplicates(subset=['sonar_run_id', 'supplier_id'], keep='first')

    # Final debug output
    print("After cleaning the data")
    print(sonar_results_df.info())
    
    return clients_df, suppliers_df, sonar_runs_df, sonar_results_df


def create_tables():
    # Database connection settings (replace with your actual configuration)
    collections_path = 'collections' 
    config = load_config(os.path.join(collections_path, 'config.json'))
    
    # Establish the connection
    try:
        conn = psycopg2.connect(
            host=config["DB_HOST"],
            database=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASSWORD"]
        )
        conn.autocommit = True
        cursor = conn.cursor()
        print("Connection to PostgreSQL successful.")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return
    
    # SQL queries to create tables with error handling
    try:
        # Creating clients_table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.clients_table (
            client_id VARCHAR PRIMARY KEY, 
            client_name VARCHAR, 
            contract_start TIMESTAMP
        ); 
        """)
        print("clients_table created successfully.")
    except Exception as e:
        print(f"Error creating clients_table: {e}")
    
    try:
        # Creating suppliers_table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.suppliers_table (
            supplier_id VARCHAR PRIMARY KEY, 
            supplier_name VARCHAR, 
            country VARCHAR  -- Adding country column
        ); 
        """)
        print("suppliers_table created successfully.")
    except Exception as e:
        print(f"Error creating suppliers_table: {e}")

    try:
        # Creating sonar_runs_table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.sonar_runs(
            sonar_run_id VARCHAR PRIMARY KEY, 
            status VARCHAR, 
            date TIMESTAMP, 
            client_id VARCHAR REFERENCES clients_table(client_id)
        ); 
        """)
        print("sonar_runs_table created successfully.")
    except Exception as e:
        print(f"Error creating sonar_runs_table: {e}")
    
    try:
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.sonar_run_suppliers(
              sonar_run_id VARCHAR,
              supplier_id VARCHAR,
                CONSTRAINT unique_sonar_run_suppliers UNIQUE (sonar_run_id, supplier_id),
                CONSTRAINT sonar_run_ids FOREIGN KEY (sonar_run_id)
                    REFERENCES public.sonar_runs (sonar_run_id) MATCH SIMPLE
                    ON UPDATE NO ACTION
                    ON DELETE NO ACTION
                    NOT VALID
        );
        """)
        print("sonar_run_suppliers table created successfully.")
    except Exception as e:
        print(f"Error creating sonar_run_suppliers table: {e}")

    try:
        # Creating sonar_results_table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.sonar_results(               
            sonar_result_id VARCHAR,
            sonar_run_id VARCHAR,
            supplier_id VARCHAR,
            price_norm numeric,
            part_id VARCHAR,
            CONSTRAINT sonar_results_pk PRIMARY KEY (sonar_result_id),
            CONSTRAINT sonar_i_fk FOREIGN KEY (sonar_run_id)
            REFERENCES public.sonar_runs (sonar_run_id) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION
             NOT VALID
        ); 
        """)
        print("sonar_results_table created successfully.")
    except Exception as e:
        print(f"Error creating sonar_results_table: {e}")

    # Check if tables exist after creation
    tables = ['clients_table', 'suppliers_table', 'sonar_runs', 'sonar_results','sonar_run_suppliers']
    for table in tables:
        try:
            cursor.execute(sql.SQL("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);"), [table])
            table_exists = cursor.fetchone()[0]
            if table_exists:
                print(f"{table} exists.")
            else:
                print(f"{table} does not exist.")
        except Exception as e:
            print(f"Error checking table existence for {table}: {e}")
    
    # Close connection
    cursor.close()
    conn.close()

# Load data into PostgreSQL
def load_to_postgresql(clients_df, suppliers_df, sonar_runs_df, sonar_results_df):
    collections_path = 'collections' 
    config = load_config(os.path.join(collections_path, 'config.json'))
    
    conn = psycopg2.connect(
        host=config["DB_HOST"],
        database=config["DB_NAME"],
        user=config["DB_USER"],
        password=config["DB_PASSWORD"]
    )
    cursor = conn.cursor()

    # Load clients
    for index, row in clients_df.iterrows():
        cursor.execute(
            """
            INSERT INTO public.clients_table (client_id, client_name, contract_start) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (client_id) DO NOTHING
            """,
            (row['_id'], row['name'], row['contract_start'])
        )

    # Load suppliers
    for index, row in suppliers_df.iterrows():
        cursor.execute(
            """
            INSERT INTO public.suppliers_table (supplier_id, supplier_name, country) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (supplier_id) DO NOTHING
            """,
            (row['_id'], row['name'], row['country'])
        )

    # Load sonar runs and handle many-to-many relationship with suppliers
    print("Contents of supplier_ids column:")
    print(sonar_runs_df['supplier_ids'].head())
    # Loop through each sonar run
    for index, row in sonar_runs_df.iterrows():
     
    # Extract supplier IDs from the row
     supplier_ids = row['supplier_ids']  # Assuming supplier_ids is a list in the DataFrame
     sonar_id=row['_id']
     print(sonar_id)
    
     if isinstance(supplier_ids, list):
        supplier_id_entry = supplier_ids  # Create a list of supplier IDs
     else:
        supplier_id_entry = []  

    # Insert sonar run into the sonar_runs table
     try:
            cursor.execute(
                """
                INSERT INTO public.sonar_runs (sonar_run_id, status, date, client_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (sonar_run_id) DO NOTHING
                """,
                (row['_id'], row['status'], row['date'], row['client_id'])
            )
            print(f"Inserted sonar run with ID {row['_id']}")
     except Exception as e:
            print(f"Error inserting sonar run for index {index}: {e}")
    
    # Insert the relationship between sonar run and each supplier
     try:
        for supplier_id in supplier_id_entry:
            cursor.execute(
                """
                INSERT INTO public.sonar_run_suppliers (sonar_run_id, supplier_id)
                VALUES (%s, %s)
                ON CONFLICT (sonar_run_id, supplier_id) DO NOTHING
                """,
                (row['_id'], supplier_id)
            )
        #print(f"Inserted relationship for sonar run ID {row['_id']} and supplier ID {supplier_id}")  
     except Exception as e:
        print(f"Error inserting supplier relationships for sonar run ID {row['_id']}: {e}")

 # Load sonar results
    for index, row in sonar_results_df.iterrows():
        # Ensure the sonar_run_id and supplier_id exist before insertion
        cursor.execute("SELECT COUNT(*) FROM public.sonar_runs WHERE sonar_run_id = %s", (row['sonar_run_id'],))
        sonar_run_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM public.suppliers_table WHERE supplier_id = %s", (row['supplier_id'],))
        supplier_count = cursor.fetchone()[0]
        
        if sonar_run_count > 0 and supplier_count > 0:  # Check if both IDs exist
            cursor.execute(
                """
                INSERT INTO public.sonar_results (sonar_result_id, sonar_run_id, supplier_id, price_norm, part_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sonar_result_id) DO NOTHING
                """,
                (
                    row['_id'],          
                    row['sonar_run_id'],    
                    row['supplier_id'],    
                    row['price_norm'],     
                    row['part_id']      
                )
            )

    #Commit changes and close the cursor and connection
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
