
import requests
import pandas as pd
import io
import psycopg2
from dateutil.parser import parse
from sqlalchemy import create_engine

# EXTRACT
def retrieve_cdc_data(api_url, limit=1000):

  """
  Extracts data from the CDC API. Use this function if you want to extract filtered data.

  Args:
      api_url (str): The URL of the CDC API.
      limit (int): The number of records returned.

  Returns:
      pandas.DataFrame: The extracted data as a DataFrame.
  """

  api_url = f"{api_url}%20LIMIT%20{limit}"
  response = requests.get(api_url)
  response_text = response.text
  csv_file = io.StringIO(response_text)
  df = pd.read_csv(csv_file)
  return df

# Retrieve data from 2024
weekly_hospitalizations_url = ('https://data.cdc.gov/resource/aemt-mg7g.csv?$query=SELECT%20week_end_date%2C%20'
    'jurisdiction%2C%20weekly_actual_days_reporting_any_data%2C%20weekly_percent_days_reporting_any_data%2C%20'
    'num_hospitals_previous_day_admission_adult_covid_confirmed%2C%20num_hospitals_previous_day_admission_pediatric_covid_confirmed%2C%20'
    'num_hospitals_previous_day_admission_influenza_confirmed%2C%20num_hospitals_total_patients_hospitalized_confirmed_influenza%2C%20'
    'num_hospitals_icu_patients_confirmed_influenza%2C%20num_hospitals_inpatient_beds%2C%20num_hospitals_total_icu_beds%2C%20'
    'num_hospitals_inpatient_beds_used%2C%20num_hospitals_icu_beds_used%2C%20num_hospitals_percent_inpatient_beds_occupied%2C%20'
    'num_hospitals_percent_staff_icu_beds_occupied%2C%20num_hospitals_percent_inpatient_beds_covid%2C%20num_hospitals_percent_inpatient_beds_influenza%2C%20'
    'num_hospitals_percent_staff_icu_beds_covid%2C%20num_hospitals_percent_icu_beds_influenza%2C%20num_hospitals_admissions_all_covid_confirmed%2C%20'
    'num_hospitals_total_patients_hospitalized_covid_confirmed%2C%20num_hospitals_staff_icu_patients_covid_confirmed%2C%20'
    'avg_admissions_adult_covid_confirmed%2C%20total_admissions_adult_covid_confirmed%2C%20avg_admissions_pediatric_covid_confirmed%2C%20'
    'total_admissions_pediatric_covid_confirmed%2C%20avg_admissions_all_covid_confirmed%2C%20total_admissions_all_covid_confirmed%2C%20'
    'avg_admissions_all_influenza_confirmed%2C%20total_admissions_all_influenza_confirmed%2C%20avg_total_patients_hospitalized_covid_confirmed%2C%20'
    'avg_total_patients_hospitalized_influenza_confirmed%2C%20avg_staff_icu_patients_covid_confirmed%2C%20avg_icu_patients_influenza_confirmed%2C%20'
    'avg_inpatient_beds%2C%20avg_total_icu_beds%2C%20avg_inpatient_beds_used%2C%20avg_icu_beds_used%2C%20avg_percent_inpatient_beds_occupied%2C%20'
    'avg_percent_staff_icu_beds_occupied%2C%20avg_percent_inpatient_beds_covid%2C%20avg_percent_inpatient_beds_influenza%2C%20'
    'avg_percent_staff_icu_beds_covid%2C%20avg_percent_icu_beds_influenza%2C%20percent_adult_covid_admissions%2C%20'
    'percent_pediatric_covid_admissions%2C%20percent_hospitals_previous_day_admission_adult_covid_confirmed%2C%20'
    'percent_hospitals_previous_day_admission_pediatric_covid_confirmed%2C%20percent_hospitals_previous_day_admission_influenza_confirmed%2C%20'
    'percent_hospitals_total_patients_hospitalized_confirmed_influenza%2C%20percent_hospitals_icu_patients_confirmed_influenza%2C%20'
    'percent_hospitals_inpatient_beds%2C%20percent_hospitals_total_icu_beds%2C%20percent_hospitals_inpatient_beds_used%2C%20'
    'percent_hospitals_icu_beds_used%2C%20percent_hospitals_percent_inpatient_beds_occupied%2C%20percent_hospitals_percent_staff_icu_beds_occupied%2C%20'
    'percent_hospitals_percent_inpatient_beds_covid%2C%20percent_hospitals_percent_inpatient_beds_influenza%2C%20'
    'percent_hospitals_percent_staff_icu_beds_covid%2C%20percent_hospitals_percent_icu_beds_influenza%2C%20'
    'percent_hospitals_admissions_all_covid_confirmed%2C%20percent_hospitals_total_patients_hospitalized_covid_confirmed%2C%20'
    'percent_hospitals_staff_icu_patients_covid_confirmed%20WHERE%20((%60week_end_date%60%20%3E%20%272023-12-31%27)%20'
    'AND%20%60week_end_date%60%20IS%20NOT%20NULL)%20ORDER%20BY%20week_end_date%20DESC')
weekly_hospitalizations = retrieve_cdc_data(weekly_hospitalizations_url, limit=2000)

# TRANSFORM
# Convert week_end_date column to YYYY-MM-DD format
weekly_hospitalizations['week_end_date'] = weekly_hospitalizations['week_end_date'].apply(lambda x: parse(x).strftime("%Y-%m-%d"))
weekly_hospitalizations.head()

# Get covid-related columns to create new dataframe
def get_covid_columns(df):
    covid_columns = [col for col in df.columns if 'covid' in col.lower()]
    return covid_columns

# Create weekly covid hospitalizations dataframe
covid_columns = get_covid_columns(weekly_hospitalizations)
weekly_covid_hospitalizations = weekly_hospitalizations[['week_end_date', 'jurisdiction', 'weekly_actual_days_reporting_any_data'] + covid_columns]

# LOAD
def load_data_to_postgresql(df, table_name, conn_string):
    """
    Load data from a DataFrame into a PostgreSQL table.

    Parameters:
        df (DataFrame): The DataFrame containing the data to be loaded.
        table_name (str): The name of the PostgreSQL table to load the data into.
        conn_string (str): The connection string for connecting to the PostgreSQL database.

    Returns:
        None
    """
    try:
        engine = create_engine(conn_string)

        df.to_sql(table_name, con=engine, if_exists='replace', index=False)

        print("Data loaded successfully.")

    except Exception as e:
        print("Error:", e)

# Refer to create_table_queries.sql to create tables before loading data.
your_connection_string = "postgresql://postgres:978258@localhost:5432/cdc_covid"
load_data_to_postgresql(weekly_covid_hospitalizations.head(), 'covid_hospitalizations', your_connection_string)
