# src/data_reconciliation/reconcile_data_sql.py

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import logging
from datetime import date, timedelta
from pandasql import sqldf  # For executing SQL on DataFrames
from io import StringIO
import numpy as np  # Import numpy
import pytz

load_dotenv()

# --- Configuration ---
TARGET_DATE_STR = (date.today() - timedelta(days=1)).strftime("%Y%m%d")  # Yesterday's date
CLEANED_DATA_ROOT = "cleaned_data"
CLEANED_TABLE_PREFIX = "cleaned_"
RECONCILED_TABLE_PREFIX = "reconciled_"
POWER_BI_EXPORT_ROOT = "power_bi_data"
#TIMEZONE_SINGAPORE = pytz.timezone('Asia/Singapore')  # Removed as per user request

# --- Logging Configuration ---
LOG_FILE = f"reconciliation_{TARGET_DATE_STR}.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Configuration ---
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# --- Helper Function to Execute SQL Query ---
def execute_sql_query(conn, query, params=None):
    """Executes an SQL query."""
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        logging.info("SQL query executed successfully.")
        return True  # Return True on success
    except psycopg2.Error as e:
        conn.rollback()
        error_message = f"Error executing SQL query: {e}. SQL Query: {query}"  # Include the query
        logging.error(error_message)
        print(error_message)  # Print the error
        return False  # Return False on error
    finally:
        cursor.close()

# --- Function to Connect to PostgreSQL ---
def connect_to_postgres():
    """Connects to the PostgreSQL database."""
    print("Connecting to Postgres...")
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        logging.info("Database connection established.")
        print("Postgres connection successful!")
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        logging.error(f"Error connecting to the database: {e}")
        return None

# --- Helper Function to Load CSV to DataFrame ---
def load_csv_to_dataframe(file_path):
    """Loads a CSV file into a Pandas DataFrame."""
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Error: CSV file not found at {file_path}")
        return None
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

# --- Function to Load DataFrame to PostgreSQL Table ---
def load_dataframe_to_postgres(conn, df, table_name):
    """Loads a Pandas DataFrame into an existing PostgreSQL table."""
    cursor = conn.cursor()
    try:
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False, na_rep='')  # Important: header=False
        output.getvalue()
        output.seek(0)
        cursor.copy_from(output, table_name, sep='\t', null='')
        conn.commit()
        logging.info(f"DataFrame loaded to existing table: {table_name}.")
        return True
    except psycopg2.Error as e:
        conn.rollback()
        error_message = f"Error loading DataFrame to {table_name}: {e}"
        logging.error(error_message)
        print(error_message)
        return False
    finally:
        cursor.close()

# --- Function to Save DataFrame to CSV ---
def save_dataframe_to_csv(df, filename_base, directory):
    """Saves a DataFrame to a CSV file."""
    print(f"Saving DataFrame to CSV: {filename_base} in {directory}")
    os.makedirs(directory, exist_ok=True)
    filename = f"{filename_base}.csv"
    file_path = os.path.join(directory, filename)
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logging.info(f"Data saved to CSV: {file_path} with {len(df)} records.")
        print("CSV file saved successfully!")
    except Exception as e:
        logging.error(f"Error saving DataFrame to CSV {file_path}: {e}")
        print(f"Error saving CSV file: {e}")

def convert_dataframe_column_types(df):
    """
    Converts DataFrame columns to their appropriate data types.
    Handles data type conversions for reconciliation.
    """
    for col in df.columns:
        if 'amount' in col:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif 'status' in col:
            df[col] = df[col].astype(str)
        elif 'currency_code' in col:
            df[col] = df[col].astype(str)
        elif 'id' in col and col != 'grab_account_id':
            df[col] = df[col].astype(str)  # Keep 'id' as string
        elif 'account_id' in col:
            df[col] = df[col].astype(str)
        elif 'name' in col or 'type' in col or 'method' in col:
            df[col] = df[col].astype(str)
        elif 'datetime' in col or 'created_at' in col or 'updated_at' in col:
            if not pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True) # Removed timezone conversion
            elif df[col].dt.tz is not None:
                df[col] = df[col].dt.tz_convert(df[col].dt.tz) # Keep original timezone if any
            
    return df

# --- Function to Create Reconciled Table if it Doesn't Exist ---
def create_reconciled_table_if_not_exists(conn, table_name, df):
    """Creates a reconciled table in PostgreSQL if it doesn't exist."""
    cursor = conn.cursor()
    try:
        column_definitions = []
        for col, dtype in df.dtypes.items():
            sql_type = "TEXT"  # Default to TEXT
            if col in ('amount', 'grab_amount', 'partner_amount'):
                sql_type = "NUMERIC"
            elif 'datetime' in col or 'created_at' in col or 'updated_at' in col:
                sql_type = "TIMESTAMP WITH TIME ZONE"
            elif 'id' in col and col != 'grab_account_id':  # Explicitly handle 'id'
                 sql_type = "TEXT"
            elif pd.api.types.is_integer_dtype(dtype):
                sql_type = "BIGINT"
            elif pd.api.types.is_float_dtype(dtype):
                sql_type = "NUMERIC"

            column_definitions.append(f'"{col}" {sql_type}')

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(column_definitions)}
            )
        """
        cursor.execute(create_table_query)
        conn.commit()
        logging.info(f"Table '{table_name}' created (if it didn't exist).")
    except psycopg2.Error as e:
        conn.rollback()
        error_message = f"Error creating table '{table_name}': {e}"
        logging.error(error_message)
        print(error_message)
    finally:
        cursor.close()

# --- Main Function for Data Reconciliation and CSV Export for a specific date ---
def reconcile_and_export_data_for_date(conn, target_date_str):
    """
    Performs data reconciliation using SQL queries on DataFrames loaded from CSV files
    and exports results to both PostgreSQL and CSV.
    """
    print(f"reconcile_and_export_data_for_date function called for {target_date_str}.")
    logging.info(f"reconcile_and_export_data_for_date function called for {target_date_str}.")

    cleaned_data_target_path = os.path.join(CLEANED_DATA_ROOT, target_date_str)
    export_directory = os.path.join(POWER_BI_EXPORT_ROOT, target_date_str)
    os.makedirs(export_directory, exist_ok=True)

    # --- Construct CSV file paths for the target date's cleaned data ---
    cleaned_grab_transactions_csv = os.path.join(
        cleaned_data_target_path, f"cleaned_grab_transactions_{target_date_str}.csv")
    cleaned_partner_transactions_csv = os.path.join(
        cleaned_data_target_path, f"cleaned_partner_transactions_{target_date_str}.csv")

    # --- Load Cleaned DataFrames from CSVs ---
    logging.info(
        f"--- Loading Cleaned Data from CSVs in {cleaned_data_target_path} ---")
    print(f"Loading cleaned data from: {cleaned_data_target_path}")
    cleaned_grab_df = load_csv_to_dataframe(cleaned_grab_transactions_csv)
    cleaned_partner_df = load_csv_to_dataframe(cleaned_partner_transactions_csv)

    if cleaned_grab_df is None or cleaned_partner_df is None:
        logging.error(
            "Error loading cleaned data from CSVs. Reconciliation and export aborted.")
        return

    # --- Perform Data Reconciliation using SQL on DataFrames ---
    logging.info("--- Performing Data Reconciliation using SQL on DataFrames ---")
    print("Performing data reconciliation using SQL on DataFrames...")

    # Make the DataFrames available for SQL queries
    globals()['grab_transactions'] = cleaned_grab_df
    globals()['partner_transactions'] = cleaned_partner_df

    # --- SQL Queries for Reconciliation ---
    match_query = """
        SELECT
            gt.transaction_id AS grab_transaction_id,
            gt.grab_account_id AS grab_account_id,
            gt.amount AS grab_amount,
            gt.currency_code AS grab_currency_code,
            gt.status AS grab_status,
            gt.created_at AS grab_created_at,
            gt.updated_at AS grab_updated_at,
            gt.transaction_datetime AS grab_transaction_datetime,
            pt.grab_transaction_id AS partner_grab_transaction_id,
            pt.grab_account_id AS partner_grab_account_id,
            pt.amount AS partner_amount,
            pt.currency_code AS partner_currency_code,
            pt.status AS partner_status,
            pt.created_at AS partner_created_at,
            pt.updated_at AS partner_updated_at,
            pt.transaction_datetime AS partner_transaction_datetime
        FROM
            grab_transactions gt
        INNER JOIN
            partner_transactions pt
            ON gt.transaction_id = pt.grab_transaction_id
            AND gt.grab_account_id = pt.grab_account_id;
    """

    grab_only_query = """
        SELECT gt.*
        FROM grab_transactions gt
        LEFT JOIN partner_transactions pt
            ON gt.transaction_id = pt.grab_transaction_id
            AND gt.grab_account_id = pt.grab_account_id
        WHERE pt.grab_transaction_id IS NULL;
    """

    partner_only_query = """
        SELECT pt.*
        FROM partner_transactions pt
        LEFT JOIN grab_transactions gt
            ON pt.grab_transaction_id = gt.transaction_id
            AND pt.grab_account_id = gt.grab_account_id
        WHERE gt.transaction_id IS NULL;
    """

    discrepancy_query = """
        SELECT
            gt.transaction_id,
            gt.grab_account_id,
            gt.amount AS grab_amount,
            pt.amount AS partner_amount,
            gt.currency_code AS grab_currency_code,
            pt.currency_code AS partner_currency_code,
            gt.status AS grab_status,
            pt.status AS partner_status,
            gt.created_at AS grab_created_at,
            pt.created_at AS partner_created_at,
            gt.updated_at AS grab_updated_at,
            pt.updated_at AS partner_updated_at,
            gt.transaction_datetime AS grab_transaction_datetime,
            pt.transaction_datetime AS partner_transaction_datetime
        FROM
            grab_transactions gt
        INNER JOIN
            partner_transactions pt
            ON gt.transaction_id = pt.grab_transaction_id
            AND gt.grab_account_id = pt.grab_account_id
        WHERE
            gt.amount <> pt.amount OR
            gt.status <> pt.status OR
            gt.currency_code <> pt.currency_code;
    """

    reconciliation_results = {
        "reconciled_transactions": sqldf(match_query),
        "grab_only_transactions": sqldf(grab_only_query),
        "partner_only_transactions": sqldf(partner_only_query),
        "discrepant_transactions": sqldf(discrepancy_query),
    }

    logging.info("--- Data Reconciliation using SQL on DataFrames Completed ---")
    print("Data reconciliation using SQL on DataFrames completed.")

    # --- Export Reconciliation Results to CSV and PostgreSQL ---
    logging.info(
        f"--- Exporting Reconciliation Results to CSV in {export_directory} and PostgreSQL ---")
    print(f"Exporting results to: {export_directory} and PostgreSQL")

    for table_name_base, df in reconciliation_results.items():
        # Convert DataFrame columns to appropriate types
        df_converted = convert_dataframe_column_types(df.copy()) # Use a copy to avoid modifying the original

        # --- Construct the table name with yesterday's date ---
        if table_name_base == "reconciled_transactions":
            table_name = f"{RECONCILED_TABLE_PREFIX}transactions_{TARGET_DATE_STR}"
            csv_filepath = os.path.join(export_directory, f"{table_name}.csv")
        else:
            table_name = f"{RECONCILED_TABLE_PREFIX}{table_name_base.split('_')[0]}_{TARGET_DATE_STR}"
            csv_filepath = os.path.join(export_directory, f"{table_name}.csv")

        # --- Create the table if it doesn't exist ---
        create_reconciled_table_if_not_exists(conn, table_name, df_converted)

        save_dataframe_to_csv(df_converted, table_name, export_directory)

        # Insert the converted dataframe into postgres
        if conn is not None and not df_converted.empty:
            load_dataframe_to_postgres(conn, df_converted, table_name)

        logging.info(
            f"Data from {table_name_base} exported to CSV and PostgreSQL.")
        print(
            f"Data from {table_name_base} exported to CSV and PostgreSQL.")

    logging.info("Data reconciliation and export process completed.")

if __name__ == "__main__":
    print("Python script is running...")
    logging.info("Script started.")
    conn = connect_to_postgres()
    if conn:
        logging.info("Database connection established.")
        reconcile_and_export_data_for_date(conn, TARGET_DATE_STR)
        conn.close()
        logging.info("Database connection closed.")
    else:
        logging.error("Failed to establish database connection. Script aborted.")
    logging.info("Script finished.")
    print("Script finished.")

