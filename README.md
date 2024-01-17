# Glassroom

## Description

The script performs the following operations:
1. Loads data from specified CSV files.
2. Transforms data, including extracting placement IDs from URLs.
3. Merges data from multiple sources.
4. Loads merged data into a table specified in Google BigQuery.

## Configuration

The script uses a JSON configuration file (config.json) to manage CSV file paths, BigQuery project and table identifiers, and the path to the Google Cloud authentication JSON key file.

Format of config.json file:

```
{
    "project_id": "your_project_id",
    "dataset_id": "your_dataset_id",
    "table_id": "your_table_id",
    "json_path": "path/to/your-key-file.json",
    "csv_paths": {
        "placements": "path/to/placements.csv",
        "raw_glassbook": "path/to/raw_glassbook.csv",
        "raw_googleanalytics": "path/to/raw_googleanalytics.csv"
    }
}
```

## Installation

1. Make sure you have Python installed on your system.
2. Install the necessary dependencies:
```
pip install -r requirements.txt
```
3. Configure your Google Cloud environment and obtain a JSON key file for authentication.

## Use

1. Place your CSV files and the config.json file in the same directory as the script.
2. Run the :
```
python main.py
```
3. Check that the data has been loaded into your BigQuery table.
