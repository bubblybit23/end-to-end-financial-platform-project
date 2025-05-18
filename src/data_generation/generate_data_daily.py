# src/data_generation/generate_data_daily.py

import os
import random
from datetime import datetime, timezone, timedelta
from uuid import uuid4
from faker import Faker
import pandas as pd
import psycopg2
from enum import Enum
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
DATA_DIR = "generated_data"
NUM_ACCOUNTS_YESTERDAY = 700  # Increased number of accounts from yesterday
NUM_ACCOUNTS_HISTORICAL = 300 # Increased number of historical accounts
NUM_GRAB_TRANSACTIONS = 100000  # Increased number of Grab transactions
DISCREPANCY_RULES = {
    "missing_partner_transaction_rate": 0.05,  # Slightly reduced missing rate
    "extra_partner_transaction_rate": 0.01,    # Significantly reduced extra rate
    "amount_mismatch_rate": 0.03,
    "status_mismatch_rate": 0.02,
}

# --- Database Configuration ---
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

# --- Enums for Data Consistency ---
class AccountType(Enum):
    REGULAR = "regular"
    PREMIUM = "premium"
    BUSINESS = "business"

class TransactionType(Enum):
    CREDIT = "credit"
    DEBIT = "debit"

class TransactionStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"
    REFUNDED = "refunded"

# --- Helper Functions ---
def generate_random_datetime(start_year=2023, end_year=2025):
    """Generates a random datetime within a given year range, with UTC+8 timezone."""
    pht_timezone = timezone(timedelta(hours=8))
    start_date = datetime(start_year, 1, 1, tzinfo=pht_timezone)
    end_date = datetime(end_year, 5, 17, tzinfo=pht_timezone) # Up to today
    return fake.date_time_between(start_date=start_date, end_date=end_date, tzinfo=pht_timezone)

def generate_random_datetime_yesterday():
    """Generates a random datetime within the 24 hours of yesterday, with UTC+8 timezone."""
    pht_timezone = timezone(timedelta(hours=8))
    now_utc = datetime.now(timezone.utc)
    now_pht = now_utc.astimezone(pht_timezone)
    start_of_yesterday_pht = now_pht.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    end_of_yesterday_pht = start_of_yesterday_pht.replace(hour=23, minute=59, second=59, microsecond=999999)
    return fake.date_time_between(start_date=start_of_yesterday_pht, end_date=end_of_yesterday_pht, tzinfo=pht_timezone)

def generate_accounts(num_yesterday, num_historical):
    """Generates synthetic account data, some from yesterday, some historical."""
    accounts = []
    # Accounts created/updated yesterday
    for _ in range(num_yesterday):
        created_at = generate_random_datetime_yesterday()
        updated_at = created_at + timedelta(minutes=random.randint(0, 1440))
        accounts.append({
            "grab_account_id": str(uuid4()),
            "user_id": fake.uuid4(),
            "account_type": random.choice([t.value for t in AccountType]),
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        })
    # Historical accounts
    for _ in range(num_historical):
        created_at = generate_random_datetime(start_year=2023, end_year=2024)
        updated_at = created_at + timedelta(days=random.randint(30, 365))
        accounts.append({
            "grab_account_id": str(uuid4()),
            "user_id": fake.uuid4(),
            "account_type": random.choice([t.value for t in AccountType]),
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        })
    return accounts

def generate_grab_transactions(num_transactions, accounts):
    """Generates synthetic Grab transaction data with timestamps from yesterday."""
    transactions = []
    account_ids = [acc["grab_account_id"] for acc in accounts]
    for _ in range(num_transactions):
        transaction_datetime = generate_random_datetime_yesterday()
        created_at = transaction_datetime - timedelta(minutes=random.randint(0, 60))
        updated_at = transaction_datetime + timedelta(minutes=random.randint(0, 60))
        transactions.append({
            "transaction_id": str(uuid4()),
            "grab_account_id": random.choice(account_ids),
            "transaction_datetime": transaction_datetime.isoformat(),
            "transaction_type": random.choice([t.value for t in TransactionType]),
            "amount": round(random.uniform(5, 500), 2),
            "currency_code": "PHP",
            "status": random.choice([s.value for s in TransactionStatus]),
            "partner_name": fake.company(),
            "payment_method": random.choice(["E-Wallet", "Credit Card", "Cash"]),
            "created_at": created_at.isoformat(),
            "updated_at": updated_at.isoformat(),
        })
    return transactions

def generate_partner_transactions(grab_transactions, discrepancy_rules):
    """Generates synthetic partner transaction data with timestamps from yesterday and discrepancies."""
    partner_transactions = []
    num_grab = len(grab_transactions)
    missing_rate = discrepancy_rules.get("missing_partner_transaction_rate", 0)
    extra_rate = discrepancy_rules.get("extra_partner_transaction_rate", 0)
    amount_mismatch_rate = discrepancy_rules.get("amount_mismatch_rate", 0)
    status_mismatch_rate = discrepancy_rules.get("status_mismatch_rate", 0)

    for txn in grab_transactions:
        if random.random() >= missing_rate:
            partner_txn = {
                "partner_transaction_id": str(uuid4()),
                "grab_transaction_id": txn["transaction_id"],
                "grab_account_id": txn["grab_account_id"],
                "transaction_datetime": generate_random_datetime_yesterday(),
                "transaction_type": txn["transaction_type"],
                "amount": txn["amount"],
                "currency_code": txn["currency_code"],
                "status": txn["status"],
                "payment_method": txn["payment_method"],
                "created_at": txn["created_at"],
                "updated_at": txn["updated_at"],
                "partner_name": fake.company(), # You might want to generate a new partner name
            }
            if random.random() < amount_mismatch_rate:
                partner_txn["amount"] = round(partner_txn["amount"] * random.uniform(0.9, 1.1), 2)
            if random.random() < status_mismatch_rate:
                partner_txn["status"] = random.choice([s.value for s in TransactionStatus if s.value != partner_txn["status"]])

            partner_txn["created_at"] = (partner_txn["transaction_datetime"] - timedelta(minutes=random.randint(0, 60))).isoformat()
            partner_txn["updated_at"] = (partner_txn["transaction_datetime"] + timedelta(minutes=random.randint(0, 60))).isoformat()
            partner_txn["transaction_datetime"] = partner_txn["transaction_datetime"].isoformat()
            partner_transactions.append(partner_txn)

    # Significantly reduce the generation of extra partner transactions
    num_extra = int(num_grab * extra_rate)
    for _ in range(num_extra):
        # Generate extra partner transactions, ensuring they are linked to existing Grab accounts
        linked_grab_txn = random.choice(grab_transactions)
        extra_partner_txn = {
            "partner_transaction_id": str(uuid4()),
            "grab_transaction_id": linked_grab_txn["transaction_id"],
            "grab_account_id": linked_grab_txn["grab_account_id"],
            "transaction_datetime": generate_random_datetime_yesterday().isoformat(),
            "transaction_type": random.choice([t.value for t in TransactionType]),
            "amount": round(random.uniform(1, 1000), 2),
            "currency_code": "PHP",
            "status": random.choice([s.value for s in TransactionStatus]),
            "payment_method": random.choice(["E-Wallet", "Credit Card", "Debit Card", "Cash"]),
            "created_at": (generate_random_datetime_yesterday() - timedelta(minutes=random.randint(0, 60))).isoformat(),
            "updated_at": (generate_random_datetime_yesterday() + timedelta(minutes=random.randint(0, 60))).isoformat(),
            "partner_name": fake.company(),
        }
        partner_transactions.append(extra_partner_txn)

    random.shuffle(partner_transactions)

    return partner_transactions

def create_tables(conn, yesterday_date_str):
    """Creates the necessary tables in PostgreSQL with date-specific names."""
    cursor = conn.cursor()
    accounts_table_name = f"accounts_{yesterday_date_str}"
    grab_transactions_table_name = f"grab_transactions_{yesterday_date_str}"
    partner_transactions_table_name = f"partner_transactions_{yesterday_date_str}"

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {accounts_table_name} (
            grab_account_id VARCHAR(50) PRIMARY KEY,
            user_id VARCHAR(50),
            account_type VARCHAR(50),
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {grab_transactions_table_name} (
            transaction_id VARCHAR(255) PRIMARY KEY,
            grab_account_id VARCHAR(50) NOT NULL REFERENCES accounts_{yesterday_date_str}(grab_account_id),
            transaction_datetime TIMESTAMP WITH TIME ZONE,
            transaction_type VARCHAR(50),
            amount NUMERIC,
            currency_code VARCHAR(3),
            status VARCHAR(50),
            partner_name VARCHAR(255),
            payment_method VARCHAR(50),
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        )
    """)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {partner_transactions_table_name} (
            partner_transaction_id VARCHAR(255) PRIMARY KEY,
            grab_transaction_id VARCHAR(255) NOT NULL,
            grab_account_id VARCHAR(50),
            transaction_datetime TIMESTAMP WITH TIME ZONE,
            transaction_type VARCHAR(50),
            amount NUMERIC,
            currency_code VARCHAR(3),
            status VARCHAR(50),
            payment_method VARCHAR(50),
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE,
            partner_name VARCHAR(255)
        )
    """)
    conn.commit()
    cursor.close()

def load_dataframes_to_db(conn, accounts_df, grab_transactions_df, partner_transactions_df, yesterday_date_str):
    """Loads Pandas DataFrames into PostgreSQL tables with date-specific names."""
    cursor = conn.cursor()
    accounts_table_name = f"accounts_{yesterday_date_str}"
    grab_transactions_table_name = f"grab_transactions_{yesterday_date_str}"
    partner_transactions_table_name = f"partner_transactions_{yesterday_date_str}"

    def load_df(df, table_name):
        if df is not None and not df.empty:
            cols = ','.join(df.columns)
            vals = ','.join(['%s'] * len(df.columns))
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
            data = [tuple(x) for x in df.to_numpy()]
            try:
                cursor.executemany(sql, data)
                conn.commit()
                print(f"Data loaded into {table_name}")
            except psycopg2.Error as e:
                conn.rollback()
                print(f"Error loading data into {table_name}: {e}. Rolling back.")
                raise
        else:
            print(f"DataFrame for {table_name} is empty or None, skipping load.")

    load_df(accounts_df, accounts_table_name)
    load_df(grab_transactions_df, grab_transactions_table_name)
    load_df(partner_transactions_df, partner_transactions_table_name)

    cursor.close()

def save_dataframes_to_csv(dataframes, output_root, yesterday_date_str):
    """Saves DataFrames to CSV files in a yesterday-date-specific subdirectory."""
    output_dir = os.path.join(output_root, yesterday_date_str)
    os.makedirs(output_dir, exist_ok=True)
    for df, filename_base in dataframes:
        filename = f"{filename_base}_{yesterday_date_str}.csv"
        df.to_csv(os.path.join(output_dir, filename), index=False)
    print(f"Data saved to directory: {output_dir}")

# --- Main Execution ---
if __name__ == "__main__":
    fake = Faker("fil_PH")
    yesterday_date = datetime.now(timezone(timedelta(hours=8))).date() - timedelta(days=1)
    yesterday_date_str = yesterday_date.strftime("%Y%m%d")
    output_dir = os.path.join(DATA_DIR, yesterday_date_str)

    # Generate accounts with a mix of yesterday's and historical data
    accounts_df = pd.DataFrame(generate_accounts(NUM_ACCOUNTS_YESTERDAY, NUM_ACCOUNTS_HISTORICAL))
    grab_transactions_df = pd.DataFrame(
        generate_grab_transactions(NUM_GRAB_TRANSACTIONS, accounts_df.to_dict("records"))
    )
    partner_transactions_df = pd.DataFrame(
        generate_partner_transactions(
            grab_transactions_df.to_dict("records"), DISCREPANCY_RULES
        )
    )

    # Save to CSV in the yesterday-date-specific directory under generated_data
    save_dataframes_to_csv(
        [
            (accounts_df, "accounts"),
            (grab_transactions_df, "grab_transactions"),
            (partner_transactions_df, "partner_transactions"),
        ],
        DATA_DIR,
        yesterday_date_str
    )

    # Connect to PostgreSQL and load data for yesterday
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
        )
        print("Connected to PostgreSQL")
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        exit()

    # Create tables (if they don't exist)
    create_tables(conn, yesterday_date_str)  # Pass the yesterday_date_str

     # Load data into PostgreSQL (all generated data)
    try:
        print("Columns in partner_transactions_df:", partner_transactions_df.columns.tolist())
        load_dataframes_to_db(conn, accounts_df, grab_transactions_df, partner_transactions_df, yesterday_date_str)
        print(f"Data loaded into PostgreSQL tables for {yesterday_date_str}")
    except Exception as e:
        print(f"An error occurred during data loading: {e}")

    conn.close()
    print(f"Phase 1: Messy Data Generation and Loading for {yesterday_date_str} Complete")