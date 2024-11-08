# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit, format_string, col
# import pandas as pd
# import re

# # Initialize Spark session
# spark = SparkSession.builder.appName("FFEL Data Cleaning").getOrCreate()

# # Function to dynamically extract quarter start and quarter ending from Quarterly Activity sheet
# def extract_quarter_dates(file_path):
#     metadata_df = pd.read_excel(file_path, sheet_name='Quarterly Activity', nrows=5)
#     metadata_text = " ".join(metadata_df.astype(str).values.flatten())
    
#     # Extract quarter start and quarter end dates using regex
#     quarter_dates_match = re.search(r"\((\d{2}/\d{2}/\d{4})-(\d{2}/\d{2}/\d{4})\)", metadata_text)
#     quarter_start = quarter_dates_match.group(1) if quarter_dates_match else None
#     quarter_end = quarter_dates_match.group(2) if quarter_dates_match else None
    
#     return quarter_start, quarter_end

# # Define the file path
# file_path = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/raw/FL_Dashboard_AY2009_2010_Q4.xls'

# # Extract quarter start and end dates from the Quarterly Activity sheet
# quarter_start, quarter_end = extract_quarter_dates(file_path)

# # Load the Quarterly Activity sheet with Pandas and specify column types
# quarterly_activity_pd = pd.read_excel(file_path, sheet_name='Quarterly Activity', skiprows=5, dtype={'OPE ID': str})
# award_year_summary_pd = pd.read_excel(file_path, sheet_name='Award Year Summary', skiprows=5, dtype={'OPE ID': str})

# # Convert to Spark DataFrames
# quarterly_activity_spark = spark.createDataFrame(quarterly_activity_pd)
# award_year_summary_spark = spark.createDataFrame(award_year_summary_pd)

# # Add quarter start and end dates as new columns to the Quarterly Activity DataFrame only
# quarterly_activity_spark = quarterly_activity_spark.withColumn("Quarter_Start", lit(quarter_start)).withColumn("Quarter_End", lit(quarter_end))

# # Add placeholder columns to Award Year Summary DataFrame for consistency
# award_year_summary_spark = award_year_summary_spark.withColumn("Quarter_Start", lit(None)).withColumn("Quarter_End", lit(None))

# # Combine the two DataFrames and remove duplicates
# combined_df = quarterly_activity_spark.union(award_year_summary_spark).dropDuplicates()

# # Print column names for debugging
# print("Column names in combined_df:", combined_df.columns)

# # Update column names to avoid any reference issues
# combined_df = combined_df.withColumnRenamed("$ of Loans Originated", "ffel_subsidized_amount")\
#                          .withColumnRenamed("$ of Loans Originated.1", "ffel_unsubsidized_amount")

# # Convert columns to Double (if they contain numeric data) before formatting
# combined_df = combined_df.withColumn("ffel_subsidized_amount", col("ffel_subsidized_amount").cast("double"))
# combined_df = combined_df.withColumn("ffel_unsubsidized_amount", col("ffel_unsubsidized_amount").cast("double"))

# # Now apply currency formatting
# combined_df = combined_df.withColumn("ffel_subsidized_amount", format_string("$%,.2f", col("ffel_subsidized_amount")))
# combined_df = combined_df.withColumn("ffel_unsubsidized_amount", format_string("$%,.2f", col("ffel_unsubsidized_amount")))


# # Standardize and clean column names
# for column in combined_df.columns:
#     combined_df = combined_df.withColumnRenamed(column, column.strip().lower().replace(" ", "_"))

# # Save the cleaned DataFrame to Excel
# output_path = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/cleaned_FFEL_data.xlsx'

# combined_df_pd = combined_df.toPandas()
# combined_df_pd.to_excel(output_path, index=False, float_format="%.2f")

# # Stop Spark session
# spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import pandas as pd
import re

# Initialize Spark session
def initialize_spark():
    return SparkSession.builder.appName("Data Cleaning").getOrCreate()

# Extract quarter start and end dates from Quarterly Activity sheet (specific to FFEL Dashboard files)
def extract_quarter_dates(file_path):
    metadata_df = pd.read_excel(file_path, sheet_name='Quarterly Activity', nrows=5)
    metadata_text = " ".join(metadata_df.astype(str).values.flatten())
    
    quarter_dates_match = re.search(r"\((\d{2}/\d{2}/\d{4})-(\d{2}/\d{2}/\d{4})\)", metadata_text)
    quarter_start = quarter_dates_match.group(1) if quarter_dates_match else None
    quarter_end = quarter_dates_match.group(2) if quarter_dates_match else None
    
    return quarter_start, quarter_end

# Load FFEL Dashboard data from Excel sheets and convert to Spark DataFrames
def load_ffel_data(file_path, spark):
    quarterly_activity_pd = pd.read_excel(file_path, sheet_name='Quarterly Activity', skiprows=5, dtype={'OPE ID': str})
    award_year_summary_pd = pd.read_excel(file_path, sheet_name='Award Year Summary', skiprows=5, dtype={'OPE ID': str})
    
    quarterly_activity_spark = spark.createDataFrame(quarterly_activity_pd)
    award_year_summary_spark = spark.createDataFrame(award_year_summary_pd)
    
    return quarterly_activity_spark, award_year_summary_spark

# Load Fed School Code data and convert to Spark DataFrame
def load_fed_school_code_data(file_path, spark):
    # Customize based on the structure of the Fed School Code file
    fed_school_code_pd = pd.read_excel(file_path)  # Adjust sheet name if different
    fed_school_code_spark = spark.createDataFrame(fed_school_code_pd)
    
    return fed_school_code_spark

# Add quarter dates for FFEL data
def add_quarter_dates(quarterly_df, award_year_df, quarter_start, quarter_end):
    quarterly_df = quarterly_df.withColumn("Quarter_Start", lit(quarter_start)).withColumn("Quarter_End", lit(quarter_end))
    award_year_df = award_year_df.withColumn("Quarter_Start", lit(None)).withColumn("Quarter_End", lit(None))
    return quarterly_df, award_year_df

# Combine DataFrames and remove duplicates
def combine_dataframes(*dfs):
    combined_df = dfs[0]
    for df in dfs[1:]:
        combined_df = combined_df.union(df)
    return combined_df.dropDuplicates()

# Rename columns to a standardized format
def rename_columns(df, column_mapping):
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    return df

# Convert specified columns to double type
def cast_columns_to_double(df, columns_to_cast):
    for col_name in columns_to_cast:
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast("double"))
    return df

# Save the cleaned DataFrame to Excel
def save_to_excel(df, output_path):
    df_pd = df.toPandas()
    df_pd.to_excel(output_path, index=False, float_format="%.2f")

# Main function to process FFEL data files
def process_ffel_data(file_path, output_path):
    spark = initialize_spark()
    try:
        # Extract quarter dates
        quarter_start, quarter_end = extract_quarter_dates(file_path)
        
        # Load FFEL data
        quarterly_activity_spark, award_year_summary_spark = load_ffel_data(file_path, spark)
        
        # Add quarter dates
        quarterly_activity_spark, award_year_summary_spark = add_quarter_dates(
            quarterly_activity_spark, award_year_summary_spark, quarter_start, quarter_end
        )
        
        # Combine data
        combined_df = combine_dataframes(quarterly_activity_spark, award_year_summary_spark)
        
        # Column renaming mappings
        column_mapping = {
            "# of Loans Originated": "ffel_subsidized_number_of_loans_originated",
            "$ of Loans Originated": "ffel_subsidized_amount_of_loans_originated",
            "Recipients": "ffel_subsidized_recipients",
            "# of Loans Originated.1": "ffel_unsubsidized_number_of_loans_originated",
            "$ of Loans Originated.1": "ffel_unsubsidized_amount_of_loans_originated",
            "Recipients.1": "ffel_unsubsidized_recipients",
            "# of Loans Originated.2": "ffel_stafford_number_of_loans_originated",
            "$ of Loans Originated.2": "ffel_stafford_amount_of_loans_originated",
            "Recipients.2": "ffel_stafford_recipients",
            "# of Loans Originated.3": "ffel_plus_number_of_loans_originated",
            "$ of Loans Originated.3": "ffel_plus_amount_of_loans_originated",
            "Recipients.3": "ffel_plus_recipients",
            "# of Disbursements": "ffel_subsidized_number_of_disbursements",
            "$ of Disbursements": "ffel_subsidized_amount_of_disbursements",
            "# of Disbursements.1": "ffel_unsubsidized_number_of_disbursements",
            "$ of Disbursements.1": "ffel_unsubsidized_amount_of_disbursements",
            "# of Disbursements.2": "ffel_stafford_number_of_disbursements",
            "$ of Disbursements.2": "ffel_stafford_amount_of_disbursements",
            "# of Disbursements.3": "ffel_plus_number_of_disbursements",
            "$ of Disbursements.3": "ffel_plus_amount_of_disbursements"
        }
        
        # Rename columns
        combined_df = rename_columns(combined_df, column_mapping)
        
        # Columns to cast to double
        columns_to_cast = [
            "ffel_subsidized_amount_of_loans_originated", "ffel_unsubsidized_amount_of_loans_originated",
            "ffel_stafford_amount_of_loans_originated", "ffel_plus_amount_of_loans_originated",
            "ffel_subsidized_amount_of_disbursements", "ffel_unsubsidized_amount_of_disbursements",
            "ffel_stafford_amount_of_disbursements", "ffel_plus_amount_of_disbursements"
        ]
        
        # Cast columns to double
        combined_df = cast_columns_to_double(combined_df, columns_to_cast)
        
        # Save to Excel
        save_to_excel(combined_df, output_path)
    
    finally:
        # Stop Spark session
        spark.stop()

# Main function to process Fed School Code data files
def process_fed_school_code_data(file_path, output_path):
    spark = initialize_spark()
    try:
        # Load Fed School Code data
        fed_school_code_spark = load_fed_school_code_data(file_path, spark)
        
        # Save to Excel
        save_to_excel(fed_school_code_spark, output_path)
    
    finally:
        # Stop Spark session
        spark.stop()


file_path = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/raw/FL_Dashboard_AY2009_2010_Q4.xls'
output_path ='/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/cleaned_FFEL_data.xlsx'
fedschool_path ='/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/1617fedschoolcodelist.xls'
fedschooloutput_path ='/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/datacleaned_Fed_School_Code_data.xlsx'
# process_ffel_data(file_path,output_path)

process_fed_school_code_data(fedschool_path,fedschooloutput_path)