import pandas as pd
import re
import json
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError


def load_csv(file_path):
    """Loads a CSV file and returns a DataFrame."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        raise RuntimeError(f"Error loading CSV file {file_path}: {e}")


def extract_placement_id(url):
    """Extracts placement ID from URL."""
    match = re.search("(?<=;)(\d+)(?=;)", url)
    return match.group(1) if match else None


def create_bigquery_client(json_path):
    """Creates and returns a BigQuery client."""
    try:
        return bigquery.Client.from_service_account_json(json_path)
    except GoogleCloudError as e:
        raise RuntimeError(f"Error creating BigQuery client: {e}")


def load_data_to_bigquery(client, dataframe, dataset_id, table_id):
    """Load data from a DataFrame into BigQuery."""
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Dataset creation if needed
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)

    # Configuration of loading options
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True

    try:
        # Loading data into BigQuery
        load_job = client.load_table_from_dataframe(dataframe, full_table_id, job_config=job_config)
        load_job.result()  # Wait for the job to finish
        print(f"The data have been successfully loaded into {full_table_id}")
    except GoogleCloudError as e:
        raise RuntimeError(f"Error loading data in BigQuery: {e}")


def load_config(config_path):
    """Load configuration from a JSON file."""
    with open(config_path, 'r') as file:
        return json.load(file)


config_path = 'config.json'
config = load_config(config_path)

# Use config values
project_id = config['project_id']
dataset_id = config['dataset_id']
table_id = config['table_id']
json_path = config['json_path']
csv_files_path = config['csv_paths']

final_columns = [
    'placement_id', 'date', 'funnel', 'format', 'size', 'campaign_name',
    'impressions', 'clicks', 'spend', 'sessions', 'bounces'
]

if __name__ == "__main__":
    try:
        # Loading CSV files
        placements_df = load_csv(csv_files_path['placements'])
        raw_glassbook_df = load_csv(csv_files_path['raw_glassbook'])
        raw_googleanalytics_df = load_csv(csv_files_path['raw_googleanalytics'])

        # Data transformation
        raw_glassbook_df['placement_id'] = raw_glassbook_df['web_tracking'].apply(extract_placement_id).astype('int64')
        placements_df[['funnel', 'format', 'size', 'campaign_name']] = placements_df['placement_name'].str.split(
            '_', expand=True
        )

        # Merging data
        merged_df = raw_glassbook_df.merge(placements_df, on='placement_id', how='left')
        final_df = merged_df.merge(raw_googleanalytics_df, on='placement_id', how='left')[final_columns]

        # Creating the BigQuery client and loading data
        bigquery_client = create_bigquery_client(json_path)
        load_data_to_bigquery(bigquery_client, final_df, dataset_id, table_id)

    except RuntimeError as e:
        print(e)
