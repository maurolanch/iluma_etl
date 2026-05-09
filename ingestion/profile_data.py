import pandas as pd
import ast

# CSV Path
CSV_PATH = "data/data_jobs.csv"

# load data

print("\nLoading CSV...\n")

df = pd.read_csv(CSV_PATH)

print("CSV loaded successfully.")

# basic info

print("\nDataframe shape")
print(df.shape)

print("\nColumn names")
print(df.columns.tolist())

print("\nData types")
print(df.dtypes)

print("\nDataframe info")
print(df.info())

# null analysis

print("\nNull values")

nulls = df.isnull().sum().sort_values(ascending=False)

print(nulls)

# duplicates

print("\nDuplicate rows")

duplicates = df.duplicated().sum()

print(f"Duplicate rows: {duplicates}")

# unique counts

print("\nUnique value counts")

columns_to_check = [
    "job_title_short",
    "company_name",
    "job_country",
    "job_schedule_type",
]

for col in columns_to_check:
    print(f"\n{col}: {df[col].nunique()} unique values")

# date validation

print("\nDate validation")

parsed_dates = pd.to_datetime(
    df["job_posted_date"],
    errors="coerce"
)

invalid_dates = parsed_dates.isna().sum()

print(f"Invalid dates: {invalid_dates}")

# skills parsing validation

print("\nJob skills validation")

invalid_job_skills = 0

for idx, value in df["job_skills"].dropna().items():

    try:
        parsed = ast.literal_eval(value)

        if not isinstance(parsed, list):
            invalid_job_skills += 1

    except Exception:
        invalid_job_skills += 1

print(f"Invalid job_skills rows: {invalid_job_skills}")

# job type skills validation

print("\nJob type skills validation")

invalid_job_type_skills = 0

for idx, value in df["job_type_skills"].dropna().items():

    try:
        parsed = ast.literal_eval(value)

        if not isinstance(parsed, dict):
            invalid_job_type_skills += 1

    except Exception:
        invalid_job_type_skills += 1

print(f"Invalid job_type_skills rows: {invalid_job_type_skills}")

# sample data

print("\nSample rows")

print(df.head())

# top countries

print("\nTop countries")

print(df["job_country"].value_counts().head(10))


# remote jobs
print("\nRemote jobs")

remote_jobs = df["job_work_from_home"].value_counts()

print(remote_jobs)

# salary analysis

print("\nSalary null analysis")

salary_cols = [
    "salary_year_avg",
    "salary_hour_avg",
    "salary_rate"
]

print(df[salary_cols].isnull().sum())

print("\nProfiling completed successfully.\n")