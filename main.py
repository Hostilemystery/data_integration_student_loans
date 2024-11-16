from config import hdfs_config
from hdfs import InsecureClient
import os
import logging
from io import BytesIO
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import re

# HDFS Configuration
HDFS_HOST = hdfs_config.HDFSConfig.HOST
HDFS_PORT = hdfs_config.HDFSConfig.PORT
HDFS_RAW_DEST_PATH = hdfs_config.HDFSConfig.RAW_DEST_PATH
HDFS_CLEAN_DEST_PATH = hdfs_config.HDFSConfig.CLEAN_DEST_PATH

# Initialize Spark session
def initialize_spark():
    return SparkSession.builder.appName("Data Cleaning").getOrCreate()

# Extract quarter dates from the Quarterly Activity sheet
def extract_quarter_dates(file_data):
    metadata_df = pd.read_excel(file_data, sheet_name='Quarterly Activity', nrows=5)
    metadata_text = " ".join(metadata_df.astype(str).values.flatten())
    quarter_dates_match = re.search(r"\((\d{2}/\d{2}/\d{4})-(\d{2}/\d{2}/\d{4})\)", metadata_text)
    return quarter_dates_match.group(1) if quarter_dates_match else None, quarter_dates_match.group(2) if quarter_dates_match else None

# Load and convert sheets into Spark DataFrames
def load_ffel_data(file_data, spark):
    quarterly_activity_pd = pd.read_excel(file_data, sheet_name='Quarterly Activity', skiprows=5, dtype={'OPE ID': str})
    award_year_summary_pd = pd.read_excel(file_data, sheet_name='Award Year Summary', skiprows=5, dtype={'OPE ID': str})
    quarterly_activity_spark = spark.createDataFrame(quarterly_activity_pd)
    award_year_summary_spark = spark.createDataFrame(award_year_summary_pd)
    return quarterly_activity_spark, award_year_summary_spark

# Add quarter dates to the FFEL DataFrames
def add_quarter_dates(quarterly_df, award_year_df, quarter_start, quarter_end):
    quarterly_df = quarterly_df.withColumn("Quarter_Start", lit(quarter_start)).withColumn("Quarter_End", lit(quarter_end))
    award_year_df = award_year_df.withColumn("Quarter_Start", lit(None)).withColumn("Quarter_End", lit(None))
    return quarterly_df, award_year_df

def combine_dataframes(*dfs):
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.union(df)
    return combined_df.dropDuplicates()

def rename_columns(df, column_mapping):
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df

def cast_columns_to_double(df, columns_to_cast):
    for col_name in columns_to_cast:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast("double"))
    return df

# Combine, clean, and process the data, then save to a local Excel file
def process_ffel_data(file_data, output_path, spark):
    quarter_start, quarter_end = extract_quarter_dates(file_data)
    quarterly_activity_spark, award_year_summary_spark = load_ffel_data(file_data, spark)
    quarterly_activity_spark, award_year_summary_spark = add_quarter_dates(quarterly_activity_spark, award_year_summary_spark, quarter_start, quarter_end)
    
    # Combine the two sheets into one DataFrame
    combined_df = combine_dataframes(quarterly_activity_spark, award_year_summary_spark)
    
    # Define column mappings for renaming
    column_mapping = {
        "# of Loans Originated": "ffel_subsidized_number_of_loans_originated",
        "$ of Loans Originated": "ffel_subsidized_amount_of_loans_originated",
        # Add other mappings here...
    }
    combined_df = rename_columns(combined_df, column_mapping)
    
    # Cast columns to double where necessary
    columns_to_cast = [
        "ffel_subsidized_amount_of_loans_originated", "ffel_unsubsidized_amount_of_loans_originated"
        # Add other columns as needed
    ]
    combined_df = cast_columns_to_double(combined_df, columns_to_cast)
    
    # Save cleaned DataFrame to local Excel
    save_to_excel(combined_df, output_path)

# Save the DataFrame to Excel for later upload
def save_to_excel(df, output_path):
    df_pd = df.toPandas()
    df_pd.to_excel(output_path, index=False, float_format="%.2f")

# Main processing function for each file type
def read_and_clean_files(hdfs_directory_path, hdfs_host, hdfs_port, local_cleaned_path):
    client = InsecureClient(f'http://{hdfs_host}:{hdfs_port}')
    spark = initialize_spark()
    try:
        files = client.list(hdfs_directory_path)
        
        for file_name in files:
            file_path = f"{hdfs_directory_path}/{file_name}"
            print(f"Processing file: {file_name}")
            
            with client.read(file_path) as f:
                file_content = f.read()
            file_data = BytesIO(file_content)
            
            if "FL_Dashboard" in file_name:
                output_path = os.path.join(local_cleaned_path, f"cleaned_{file_name}")
                process_ffel_data(file_data, output_path, spark)
                
    finally:
        spark.stop()

# Upload cleaned files to HDFS
def upload_files_to_hdfs(local_path, hdfs_path, hdfs_client):
    try:
        if not hdfs_client.exists(hdfs_path):
            hdfs_client.mkdirs(hdfs_path)
        
        for file_name in os.listdir(local_path):
            local_file_path = os.path.join(local_path, file_name)
            if os.path.isfile(local_file_path):
                hdfs_file_path = f"{hdfs_path}/{file_name}"
                with open(local_file_path, 'rb') as file_data:
                    hdfs_client.create(hdfs_file_path, file_data)
                logging.info(f"Uploaded {file_name} to HDFS at {hdfs_file_path}")
    
    except Exception as e:
        logging.error(f"Unexpected error during file upload: {e}")

# Main execution
local_cleaned_path = "/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/cleaned"
read_and_clean_files(HDFS_RAW_DEST_PATH, HDFS_HOST, HDFS_PORT, local_cleaned_path)
hdfs_client = InsecureClient(f'http://{HDFS_HOST}:{HDFS_PORT}')
upload_files_to_hdfs(local_cleaned_path, HDFS_CLEAN_DEST_PATH, hdfs_client)


