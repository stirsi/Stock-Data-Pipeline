from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from google.cloud import storage, bigquery
from google.oauth2 import service_account  
import pandas as pd
import requests
import os
from typing import List

# Set Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path to credentials.json"

# Prefect tasks
@task(retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch_symbol_data(symbol: str, api_key: str) -> pd.DataFrame:
    logger = get_run_logger()
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    time_series = data.get('Time Series (Daily)', {})

    if not time_series:
        logger.error(f"No data found for {symbol}. Continuing with the next symbol.")
        return pd.DataFrame()  # Return empty DataFrame

    df = pd.DataFrame.from_dict(time_series, orient='index', dtype=float)
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df.index = pd.to_datetime(df.index)
    df['Symbol'] = symbol
     # Cast Volume to int to avoid schema issues with BigQuery
    df['Volume'] = df['Volume'].astype(int)
    return df

@task(retries=2)
def fetch_and_combine_stock_data(api_key: str, symbols: List[str], output_csv: str = None) -> pd.DataFrame:
    logger = get_run_logger()
    dataframes = []
    failed_symbols = []
    
    for symbol in symbols:
        df = fetch_symbol_data(symbol, api_key)
        if not df.empty:
            dataframes.append(df)
        else:
            failed_symbols.append(symbol)

    if failed_symbols:
        logger.warning(f"Failed to fetch data for: {', '.join(failed_symbols)}")

    combined_df = pd.concat(dataframes).sort_index().reset_index().rename(columns={'index': 'Date'})
    
    if output_csv:
        upload_to_gcs(bucket_name='your_bucket_name', object_name=output_csv, dataframe=combined_df)
        logger.info(f"Data saved to gs://your_bucket_name/{output_csv}")
    
    return combined_df

@task
def upload_to_gcs(bucket_name: str, object_name: str, dataframe: pd.DataFrame):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(dataframe.to_csv(index=False), content_type='text/csv')

@task
def load_data_to_bigquery(csv_file_path, project_id, dataset_id, table_id, credentials_path):
    credentials = service_account.Credentials.from_service_account_file(credentials_path)  
    client = bigquery.Client(credentials=credentials, project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Step 1: Truncate the table
    logger = get_run_logger()  # Get the logger
    try:
        truncate_query = f"TRUNCATE TABLE `{table_ref}`"
        client.query(truncate_query).result()  # Execute the truncate query
        logger.info(f"Successfully truncated table: {table_ref}")
    except Exception as e:
        logger.error(f"Error truncating table {table_ref}: {e}")
        return 0  # Return 0 rows loaded in case of truncation error

    # Step 2: Load new data into the table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
    )
    
    try:
        job = client.load_table_from_uri(
            f"gs://your_bucket_name/{csv_file_path}",
            table_ref,
            job_config=job_config,
        )
        job.result() # waut for the job to complete

        table = client.get_table(table_ref) # get table details
        logger.info(f"Loaded {table.num_rows} rows to {table_ref}.")
        return table.num_rows # return number of rows loaded
    except Exception as e:
        logger.error(f"Error loading data into table {table_ref}: {e}")
        return 0  # Return 0 rows loaded in case of loading error


@task
def execute_saved_queries(credentials_path: str, project_id: str, queries_folder: str):
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)
    
    logger = get_run_logger()  # Get the logger

    query_files = [
        "Daily_Merge_Deduplicate.sql",
        "Stock_Aggregation.sql",
        "Scaling_Normalizing_ML.sql",
        "Test_Train_Tables.sql"
    ]

    for query_file in query_files:
        file_path = os.path.join(queries_folder, query_file)
        try:
            # Read the SQL query from the file
            with open(file_path, 'r') as file:
                query = file.read()

            # Execute the query
            query_job = client.query(query)  # Make an API request
            query_job.result()  # Wait for the job to complete
            
            logger.info(f"Successfully executed saved query from file: {query_file}")
        except Exception as e:
            logger.error(f"Failed to execute query from file {query_file}: {e}")

# Prefect flow
@flow(name="Stock Data Flow", retries=2)
def stock_data_flow():
    api_key = 'your_api_key'
    symbols = ['WMT', 'AMZN', 'UPS', 'FDX', 'HD', 'UNH', 'CNXC', 'TGT', 'KR', 'MAR']
    csv_file = 'combined_daily_stock_data.csv'

    fetch_and_combine_stock_data(api_key, symbols, output_csv=csv_file)
    load_data_to_bigquery(
        csv_file_path=csv_file, 
        project_id='your_project_id', 
        dataset_id='your_dataset', 
        table_id='your_table', 
        credentials_path = 'path to credentials.json'
    )
    execute_saved_queries(
        credentials_path = 'path to credentials.json', 
        project_id='your_project_id', 
        queries_folder=r'folder that holds the queries' 
    )

# To run the flow
#if __name__ == "__main__":
#    stock_data_flow()

if __name__ == "__main__":
    stock_data_flow.serve(name="daily_stock_data_update",
                    interval=86400)
