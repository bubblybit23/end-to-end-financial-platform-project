# src/data_cleaning/clean_data.py

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta

load_dotenv()

# --- Configuration ---
GENERATED_DATA_ROOT = "generated_data"
CLEANED_DATA_ROOT = "cleaned_data"
CLEANED_TABLE_PREFIX = "cleaned_"
# TARGET_DATE_STR will be set dynamically
DATE_FORMAT_FOLDER = "%Y%m%d"

# --- Database Configuration ---
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

def create_sqlalchemy_engine():
    """Creates a SQLAlchemy engine."""
    return create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def execute_sql_query(engine, sql):
    """Executes an SQL query using SQLAlchemy."""
    try:
        with engine.connect() as connection:
            connection.execute(text(sql))
            connection.commit()
        print("[INFO] SQL executed successfully.")
    except SQLAlchemyError as e:
        print(f"[ERROR] Error executing SQL query: {e}")
        raise

def read_sql_query(engine, sql):
    """Executes an SQL query and returns a Pandas DataFrame."""
    try:
        with engine.connect() as connection:
            result = connection.execute(text(sql))
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        print("[INFO] SQL query executed and DataFrame created.")
        return df
    except SQLAlchemyError as e:
        print(f"[ERROR] Error executing SQL query: {e}")
        raise

def save_dataframe_to_csv(df, filename_base, output_path, target_date_str):
    """Saves a Pandas DataFrame to a CSV file with the target date in the filename."""
    # Use target_date_str to create the subfolder
    dated_output_path = os.path.join(output_path, target_date_str)
    os.makedirs(dated_output_path, exist_ok=True)
    filename = f"{filename_base}_{target_date_str}.csv"
    filepath = os.path.join(dated_output_path, filename)  # Save in the subfolder
    df.to_csv(filepath, index=False)
    print(f"[INFO] Cleaned data saved to CSV: {filepath}")

def load_dataframe_to_staging(engine, df, table_name):
    """Loads a Pandas DataFrame into a PostgreSQL staging table using SQLAlchemy."""
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"[INFO] Data loaded to table: {table_name}")
    except SQLAlchemyError as e:
        print(f"[ERROR] Error loading data to {table_name}: {e}")
        raise

def parse_datetime(text_value):
    """Attempts to parse a datetime string with various formats."""
    if pd.isna(text_value):
        return None
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%m/%d/%Y %I:%M:%S %p",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%dT%H%M%SZ",
        "%Y-%m-%d"
    ]
    for fmt in formats:
        try:
            return pd.to_datetime(text_value, format=fmt, utc=True)
        except ValueError:
            continue
    return None

def clean_data_for_date(engine, target_date_str):
    """Cleans data for a specific date by fetching and processing dates in Python."""
    print(f"[INFO] Processing data for: {target_date_str}")

    # --- 1. Construct the path to the target data directory ---
    target_data_path = os.path.join(GENERATED_DATA_ROOT, target_date_str)

    # --- 2. Load CSVs to DataFrames for the target date ---
    accounts_file = os.path.join(target_data_path, f"accounts_{target_date_str}.csv")
    grab_transactions_file = os.path.join(target_data_path, f"grab_transactions_{target_date_str}.csv")
    partner_transactions_file = os.path.join(target_data_path, f"partner_transactions_{target_date_str}.csv")

    try:
        accounts_df = pd.read_csv(accounts_file)
        print(f"[INFO] DataFrame loaded from CSV: {accounts_file}")
        grab_transactions_df = pd.read_csv(grab_transactions_file)
        print(f"[INFO] DataFrame loaded from CSV: {grab_transactions_file}")
        partner_transactions_df = pd.read_csv(partner_transactions_file)
        print(f"[INFO] DataFrame loaded from CSV: {partner_transactions_file}")
    except FileNotFoundError as e:
        print(f"[ERROR] Fatal error: {e}")
        raise

    # --- 3. Create Staging Tables and Load Data Directly from DataFrames ---
    staging_accounts_table = "staging_accounts"
    staging_grab_transactions_table = "staging_grab_transactions"
    staging_partner_transactions_table = "staging_partner_transactions"

    execute_sql_query(engine, f"DROP TABLE IF EXISTS {staging_accounts_table}")
    accounts_df.to_sql(staging_accounts_table, engine, if_exists='replace', index=False)
    print(f"[INFO] Data loaded to table: {staging_accounts_table}")

    execute_sql_query(engine, f"DROP TABLE IF EXISTS {staging_grab_transactions_table}")
    grab_transactions_df.to_sql(staging_grab_transactions_table, engine, if_exists='replace', index=False)
    print(f"[INFO] Data loaded to table: {staging_grab_transactions_table}")

    execute_sql_query(engine, f"DROP TABLE IF EXISTS {staging_partner_transactions_table}")
    partner_transactions_df.to_sql(staging_partner_transactions_table, engine, if_exists='replace', index=False)
    print(f"[INFO] Data loaded to table: {staging_partner_transactions_table}")

    # --- Clean Accounts Data and Save to New Table and CSV ---
    cleaned_accounts_table = f"{CLEANED_TABLE_PREFIX}accounts_{target_date_str}"
    accounts_data = read_sql_query(engine, f"SELECT grab_account_id, user_id, account_type, created_at, updated_at FROM {staging_accounts_table}")
    accounts_data['created_at'] = accounts_data['created_at'].apply(parse_datetime)
    accounts_data['updated_at'] = accounts_data['updated_at'].apply(parse_datetime)
    accounts_data['user_id'] = accounts_data['user_id'].str.strip()
    accounts_data['account_type'] = accounts_data['account_type'].str.lower()
    accounts_data.to_sql(cleaned_accounts_table, engine, if_exists='replace', index=False)
    save_dataframe_to_csv(accounts_data, f"cleaned_accounts", CLEANED_DATA_ROOT, target_date_str)

    # --- Clean Grab Transactions Data and Save to New Table and CSV ---
    cleaned_grab_transactions_table = f"{CLEANED_TABLE_PREFIX}grab_transactions_{target_date_str}"
    grab_transactions_data = read_sql_query(engine, f"SELECT transaction_id, grab_account_id, transaction_datetime, transaction_type, amount, currency_code, status, partner_name, payment_method, created_at, updated_at FROM {staging_grab_transactions_table}")
    grab_transactions_data['transaction_datetime'] = grab_transactions_data['transaction_datetime'].apply(parse_datetime)
    grab_transactions_data['created_at'] = grab_transactions_data['created_at'].apply(parse_datetime)
    grab_transactions_data['updated_at'] = grab_transactions_data['updated_at'].apply(parse_datetime)
    grab_transactions_data['transaction_type'] = grab_transactions_data['transaction_type'].str.strip().str.lower()
    grab_transactions_data['amount'] = pd.to_numeric(grab_transactions_data['amount'], errors='coerce')
    grab_transactions_data['currency_code'] = grab_transactions_data['currency_code'].str.strip().str.upper().str.replace(r'[^a-zA-Z]', '', regex=True)
    grab_transactions_data['status'] = grab_transactions_data['status'].str.strip().str.lower()
    grab_transactions_data['partner_name'] = grab_transactions_data['partner_name'].str.strip()
    grab_transactions_data['payment_method'] = grab_transactions_data['payment_method'].str.strip().str.lower()
    grab_transactions_data.to_sql(cleaned_grab_transactions_table, engine, if_exists='replace', index=False)
    save_dataframe_to_csv(grab_transactions_data, f"cleaned_grab_transactions", CLEANED_DATA_ROOT, target_date_str)

    # --- Clean Partner Transactions Data and Save to New Table and CSV ---
    cleaned_partner_transactions_table = f"{CLEANED_TABLE_PREFIX}partner_transactions_{target_date_str}"
    partner_transactions_data = read_sql_query(engine, f"SELECT partner_transaction_id, grab_transaction_id, grab_account_id, transaction_datetime, transaction_type, amount, currency_code, status, payment_method, created_at, updated_at, partner_name FROM {staging_partner_transactions_table}")
    partner_transactions_data['transaction_datetime'] = partner_transactions_data['transaction_datetime'].apply(parse_datetime)
    partner_transactions_data['created_at'] = partner_transactions_data['created_at'].apply(parse_datetime)
    partner_transactions_data['updated_at'] = partner_transactions_data['updated_at'].apply(parse_datetime)
    partner_transactions_data['transaction_type'] = partner_transactions_data['transaction_type'].str.strip().str.lower()
    partner_transactions_data['amount'] = pd.to_numeric(partner_transactions_data['amount'], errors='coerce')
    partner_transactions_data['currency_code'] = partner_transactions_data['currency_code'].str.strip().str.upper().str.replace(r'[^a-zA-Z]', '', regex=True)
    partner_transactions_data['status'] = partner_transactions_data['status'].str.strip().str.lower()
    partner_transactions_data['payment_method'] = partner_transactions_data['payment_method'].str.strip().str.lower()
    partner_transactions_data['partner_name'] = partner_transactions_data['partner_name'].str.strip()
    partner_transactions_data.to_sql(cleaned_partner_transactions_table, engine, if_exists='replace', index=False)
    save_dataframe_to_csv(partner_transactions_data, f"cleaned_partner_transactions", CLEANED_DATA_ROOT, target_date_str)

    # --- 5. Optionally Drop Staging Tables ---
    execute_sql_query(engine, f"DROP TABLE IF EXISTS {staging_accounts_table}")
    execute_sql_query(engine, f"DROP TABLE IF EXISTS {staging_grab_transactions_table}")
    execute_sql_query(engine, f"DROP TABLE IF EXISTS {staging_partner_transactions_table}")

    print(f"[INFO] Phase 2: Data Cleaning and Saving (CSV & PostgreSQL) Complete for {target_date_str}")

if __name__ == "__main__":
    print("[INFO] Script started...")
    engine = None
    try:
        engine = create_sqlalchemy_engine()
        print("[INFO] Database engine created using SQLAlchemy.")

        # --- Dynamically set TARGET_DATE_STR to yesterday ---
        yesterday = datetime.now() - timedelta(days=1)
        TARGET_DATE_STR = yesterday.strftime("%Y%m%d")
        print(f"[INFO] Target date set to: {TARGET_DATE_STR}")

        clean_data_for_date(engine, TARGET_DATE_STR)

    except SQLAlchemyError as e:
        print(f"[ERROR] Fatal error: {e}")
    finally:
        print("[INFO] SQLAlchemy engine disposed (connection closed).")