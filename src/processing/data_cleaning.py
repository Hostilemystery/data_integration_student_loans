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
from pyspark.sql.functions import lit, format_string, col
import pandas as pd
import re

# Initialize Spark session
spark = SparkSession.builder.appName("FFEL Data Cleaning").getOrCreate()

# Function to dynamically extract quarter start and quarter ending from Quarterly Activity sheet
def extract_quarter_dates(file_path):
    metadata_df = pd.read_excel(file_path, sheet_name='Quarterly Activity', nrows=5)
    metadata_text = " ".join(metadata_df.astype(str).values.flatten())
    
    # Extract quarter start and quarter end dates using regex
    quarter_dates_match = re.search(r"\((\d{2}/\d{2}/\d{4})-(\d{2}/\d{2}/\d{4})\)", metadata_text)
    quarter_start = quarter_dates_match.group(1) if quarter_dates_match else None
    quarter_end = quarter_dates_match.group(2) if quarter_dates_match else None
    
    return quarter_start, quarter_end

# Define the file path
file_path = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/raw/FL_Dashboard_AY2009_2010_Q4.xls'

# Extract quarter start and end dates from the Quarterly Activity sheet
quarter_start, quarter_end = extract_quarter_dates(file_path)

# Load the Quarterly Activity sheet with Pandas and specify column types
quarterly_activity_pd = pd.read_excel(file_path, sheet_name='Quarterly Activity', skiprows=5, dtype={'OPE ID': str})
award_year_summary_pd = pd.read_excel(file_path, sheet_name='Award Year Summary', skiprows=5, dtype={'OPE ID': str})

# Convert to Spark DataFrames
quarterly_activity_spark = spark.createDataFrame(quarterly_activity_pd)
award_year_summary_spark = spark.createDataFrame(award_year_summary_pd)

# Add quarter start and end dates as new columns to the Quarterly Activity DataFrame only
quarterly_activity_spark = quarterly_activity_spark.withColumn("Quarter_Start", lit(quarter_start)).withColumn("Quarter_End", lit(quarter_end))

# Add placeholder columns to Award Year Summary DataFrame for consistency
award_year_summary_spark = award_year_summary_spark.withColumn("Quarter_Start", lit(None)).withColumn("Quarter_End", lit(None))

# Combine the two DataFrames and remove duplicates
combined_df = quarterly_activity_spark.union(award_year_summary_spark).dropDuplicates()

# Rename columns to follow the specified format
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

# Apply column renaming
for old_name, new_name in column_mapping.items():
    if old_name in combined_df.columns:
        combined_df = combined_df.withColumnRenamed(old_name, new_name)

columns_to_format = [
    "ffel_subsidized_amount_of_loans_originated", "ffel_unsubsidized_amount_of_loans_originated",
    "ffel_stafford_amount_of_loans_originated", "ffel_plus_amount_of_loans_originated",
    "ffel_subsidized_amount_of_disbursements", "ffel_unsubsidized_amount_of_disbursements",
    "ffel_stafford_amount_of_disbursements", "ffel_plus_amount_of_disbursements"
]

for col_name in columns_to_format:
    if col_name in combined_df.columns:
        combined_df = combined_df.withColumn(col_name, col(col_name).cast("double"))

# Save the cleaned DataFrame to Excel
output_path = '/home/freddy/Documents/Cours_efrei/Data_integration/Projets_data_integration/data_integration_student_loans/data/cleaned_FFEL_data.xlsx'

combined_df_pd = combined_df.toPandas()
combined_df_pd.to_excel(output_path, index=False, float_format="%.2f")

# Stop Spark session
spark.stop()
