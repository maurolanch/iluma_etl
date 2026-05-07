import pandas as pd
import ast

# CSV Path
CSV_PATH = "data/data_jobs.csv"

# =========================
# LOAD DATA
# =========================

print("\nLoading CSV...\n")

df = pd.read_csv(CSV_PATH)

print("CSV loaded successfully.")

# =========================
# BASIC INFO
# =========================

print("\n==============================")
print("DATAFRAME SHAPE")
print("==============================")
print(df.shape)

print("\n==============================")
print("COLUMN NAMES")
print("==============================")
print(df.columns.tolist())

print("\n==============================")
print("DATA TYPES")
print("==============================")
print(df.dtypes)

print("\n==============================")
print("DATAFRAME INFO")
print("==============================")
print(df.info())

# =========================
# NULL ANALYSIS
# =========================

print("\n==============================")
print("NULL VALUES")
print("==============================")

nulls = df.isnull().sum().sort_values(ascending=False)

print(nulls)

# =========================
# DUPLICATES
# =========================

print("\n==============================")
print("DUPLICATE ROWS")
print("==============================")

duplicates = df.duplicated().sum()

print(f"Duplicate rows: {duplicates}")

# =========================
# UNIQUE COUNTS
# =========================

print("\n==============================")
print("UNIQUE VALUE COUNTS")
print("==============================")

columns_to_check = [
    "job_title_short",
    "company_name",
    "job_country",
    "job_schedule_type",
]

for col in columns_to_check:
    print(f"\n{col}: {df[col].nunique()} unique values")

# =========================
# DATE VALIDATION
# =========================

print("\n==============================")
print("DATE VALIDATION")
print("==============================")

parsed_dates = pd.to_datetime(
    df["job_posted_date"],
    errors="coerce"
)

invalid_dates = parsed_dates.isna().sum()

print(f"Invalid dates: {invalid_dates}")

# =========================
# SKILLS PARSING VALIDATION
# =========================

print("\n==============================")
print("JOB_SKILLS VALIDATION")
print("==============================")

invalid_job_skills = 0

for idx, value in df["job_skills"].dropna().items():

    try:
        parsed = ast.literal_eval(value)

        if not isinstance(parsed, list):
            invalid_job_skills += 1

    except Exception:
        invalid_job_skills += 1

print(f"Invalid job_skills rows: {invalid_job_skills}")

# =========================
# JOB TYPE SKILLS VALIDATION
# =========================

print("\n==============================")
print("JOB_TYPE_SKILLS VALIDATION")
print("==============================")

invalid_job_type_skills = 0

for idx, value in df["job_type_skills"].dropna().items():

    try:
        parsed = ast.literal_eval(value)

        if not isinstance(parsed, dict):
            invalid_job_type_skills += 1

    except Exception:
        invalid_job_type_skills += 1

print(f"Invalid job_type_skills rows: {invalid_job_type_skills}")

# =========================
# SAMPLE DATA
# =========================

print("\n==============================")
print("SAMPLE ROWS")
print("==============================")

print(df.head())

# =========================
# TOP COUNTRIES
# =========================

print("\n==============================")
print("TOP COUNTRIES")
print("==============================")

print(df["job_country"].value_counts().head(10))


# REMOTE JOBS


print("\n==============================")
print("REMOTE JOBS")
print("==============================")

remote_jobs = df["job_work_from_home"].value_counts()

print(remote_jobs)

# =========================
# SALARY ANALYSIS
# =========================

print("\n==============================")
print("SALARY NULL ANALYSIS")
print("==============================")

salary_cols = [
    "salary_year_avg",
    "salary_hour_avg",
    "salary_rate"
]

print(df[salary_cols].isnull().sum())

print("\nProfiling completed successfully.\n")