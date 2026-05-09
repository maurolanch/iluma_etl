# ETL Pipeline — Job Market Intelligence

A production-grade data pipeline that ingests 785k job postings from a CSV source, loads them into PostgreSQL, and transforms the raw data into a normalized 3NF relational model using dbt.

---

## Architecture Overview

```
CSV (250MB)
    ↓
raw.jobs_raw          ← Python + COPY (faithful source replica)
    ↓
staging.stg_jobs      ← dbt view (type casting only)
    ↓
intermediate.*        ← dbt views (deduplication, normalization)
    ↓
marts.*               ← dbt tables (3NF final model)
```

---

## Quick Start

### Requirements
 
- Python 3.10+
- Docker + Docker Compose
- pandas
- psycopg2-binary
- sqlalchemy
- python-dotenv
- dbt-postgres
- pytest

### Setup

```bash
# Clone the repo
git clone <repo-url>
cd etl_iluma

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Environment variables

```bash
# .env
DB_URL=postgresql://etl_user:password@localhost:5432/iluma_db

# Edit .env with your database credentials
```

### Start the database

```bash
docker-compose up -d
```

### Run the full pipeline

```bash
PYTHONPATH=. python pipeline/main.py
```

This runs in sequence:
1. Creates raw schema and tables
2. Loads CSV into `raw.jobs_raw`
3. Runs all dbt transformations
4. Runs all dbt tests

### Run unit tests

```bash
# Unit tests (pytest)
pytest tests/ -v

# dbt data quality tests only
cd dbt
dbt test
```

---

## Design Decisions

### 1. Data profiling before building

Before writing any transformation, the raw data was profiled directly in SQL to understand what needed to be cleaned. This revealed three critical issues that shaped the entire pipeline design: `job_country` was unreliable (US cities mapped to Sudan), company names had up to 4 spelling variations for the same entity, and `job_skills` / `job_type_skills` were stored as Python-formatted strings with single quotes — not valid JSON.

### 2. Raw ingestion: PostgreSQL COPY over pandas `to_sql`

The initial implementation used `pandas.to_sql(method="multi")`, which crashed consistently around row 450,000 due to a known SQLAlchemy compiler bug with large multi-row INSERT statements.

The solution was PostgreSQL's native `COPY` command via `psycopg2.copy_expert()`. COPY bypasses SQL parsing entirely and streams data directly into the table. Combined with `pd.read_csv(chunksize=50_000)` to avoid loading 250MB into memory at once, the full 785,741 rows load across 16 chunks. Note: ingestion time is higher than a plain TEXT load because `job_skills` and `job_type_skills` require Python-to-JSON conversion via `ast.literal_eval()` + `json.dumps()` on every row before the COPY buffer is written.

### 3. Raw layer: everything as TEXT except JSONB

The raw layer stores all columns as `TEXT` to preserve the source data faithfully — no type coercion, no cleaning, no dropped rows. This follows the medallion architecture principle: the raw layer is an immutable replica of the source, enabling full reprocessing if transformation logic changes.

The only exceptions are `job_skills` and `job_type_skills`, which the assessment explicitly requires to be loaded as appropriate data types. These are stored as `JSONB` because:
- The source format is Python literals (`['python', 'sql']`) not valid JSON
- `ast.literal_eval()` + `json.dumps()` converts them reliably during ingestion
- JSONB enables native Postgres operators like `@>` for skill filtering queries

### 4. Centralized logger

A single `get_logger(name)` function in `utils/logger.py` is imported by every module. It writes `DEBUG` and above to `logs/etl.log` and `INFO` and above to stdout. Using `__name__` as the logger name means every log line identifies exactly which module produced it — critical when debugging a multi-step pipeline.

### 5. dbt as the transformation layer

dbt was chosen over raw SQL scripts or Python pandas transformations for three reasons: transformations are version-controlled SQL with lineage tracking via `ref()`, tests are declarative and run automatically, and the layered model structure makes each step independently auditable.

### 6. Custom `generate_schema_name` macro

By default, dbt prefixes every schema with the target schema defined in `profiles.yml`. Since the profile uses `schema: staging`, dbt was generating `staging_marts` and `staging_intermediate` instead of `marts` and `intermediate`. A custom `generate_schema_name` macro overrides this behavior, telling dbt to use the schema name exactly as declared in `dbt_project.yml` with no prefixes.

### 7. Three-layer transformation model

Each layer has a single responsibility:

**Staging** — type casting only, 1:1 with raw. No business logic. `DISTINCT ON` removes 845 exact duplicates found during profiling. `NULLIF` handles empty strings before numeric casts.

**Intermediate** — normalization and deduplication:
- `int_companies_deduped`: `trim(lower())` collapses variations like `GOOGLE`, `Google`, `google` into a single canonical name. All "confidential" variants are unified into a single `confidential` entry.
- `int_locations_deduped`: `job_country` from the source was derived by a scraper bot and is unreliable — US cities in Texas, Georgia, and other states were systematically assigned `Sudan`. A Postgres pattern match (`~*`) against US state abbreviations re-derives the correct country from `job_location`. `DISTINCT ON ... ORDER BY job_country NULLS LAST` ensures one row per location, prioritizing a non-null country.
- `int_job_type_skills`: explodes the nested `job_type_skills` JSONB object into one row per job+skill+category using double `LATERAL` — first `jsonb_each` to unpack the category keys, then `jsonb_array_elements_text` to unpack each skill array.

A validation query confirmed that every skill in `job_skills` also appears in `job_type_skills` (0 exceptions across 785k rows), so `job_skills` was dropped as redundant. `job_type_skills` is a strict superset.

**Marts** — the final 3NF model with surrogate keys and foreign key relationships validated by dbt tests.

### 8. 3NF model design

The model separates the main entities identified in the source:

| Table | Granularity | Natural key |
|---|---|---|
| `dim_companies` | one row per unique company | `company_name_clean` |
| `dim_locations` | one row per unique location | `job_location` |
| `dim_skills` | one row per skill+category pair | `skill + skill_category` |
| `dim_job_types` | one row per flag combination | `schedule_type + 3 booleans` |
| `fact_jobs` | one row per job posting | `raw_id` |
| `bridge_job_skills` | one row per job+skill combination | `job_id + skill_id` |

`bridge_job_skills` resolves the many-to-many relationship between jobs and skills. A single job posting requires multiple skills, and each skill appears across thousands of job postings. The bridge holds one row per job+skill pair: if a job requires Python, SQL, and AWS, it produces three rows in the bridge, each linking the same `job_id` to a different `skill_id`. This allows querying "all jobs that require Python" or "all skills required by a given job" without data duplication.

### 9. Surrogate keys

`row_number()` was the initial approach but is unstable — the same record can receive a different ID on each `dbt run` if new rows are added. `dbt_utils.generate_surrogate_key()` generates a deterministic MD5 hash from the natural key fields, guaranteeing the same ID for the same record across every run.

The hash inputs were chosen deliberately:

- `dim_companies`: `company_name_clean` — the normalized name is the identity
- `dim_locations`: `job_location` — after `DISTINCT ON`, one location maps to exactly one country, so the location string alone is sufficient
- `dim_skills`: `skill + skill_category` — the same skill name can appear in different categories (e.g. `mongodb` appears in both `databases` and `programming`)
- `dim_job_types`: all four columns — the dimension captures unique combinations of schedule type and boolean flags
- `fact_jobs`: `raw_id` — the source primary key is already unique after deduplication in staging

### 10. Test strategy

Tests were defined at every layer to catch issues as early as possible:

**Source tests** (`sources.yml`): `unique` and `not_null` on `raw.jobs_raw.id` — validates the source data before any transformation runs.

**Staging tests**: `unique` and `not_null` on `raw_id`, `accepted_values` on `job_title_short` — validates that deduplication worked and that only the 10 expected job categories are present.

**Intermediate tests**: `unique` on `company_name_clean` and `job_location` — validates that deduplication produced exactly one row per entity before dimensions are built.

**Marts tests**: `unique` and `not_null` on all PKs, `relationships` tests on all FKs — validates full referential integrity across the 3NF model. These tests caught two real bugs during development: 65 duplicate `location_id` values caused by the same `job_location` mapping to two different countries, and 12,602 duplicate `job_id` values in `fact_jobs` caused by a cartesian product from the locations join. Both were fixed before the final run.

**Unit tests** (pytest): `sanitize_json_columns` is tested with 8 cases covering valid lists, valid nested dicts, empty strings, `"nan"` strings, corrupted values, already-valid JSON, column independence, and the semantic distinction between empty (`[]`/`{}`) and absent (`None`).

---

## Repository Structure

```
etl_iluma/
├── data/                   # source CSV (gitignored)
├── db/
│   └── create_tables.py    # DDL execution
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
├── ingestion/
│   ├── ingest_jobs.py
│   └── profile_data.py     # data profiling scripts
├── logs/                   # gitignored
├── pipeline/
│   └── main.py             # pipeline orchestrator
├── sql/
│   └── create_raw_tables.sql
├── tests/
│   └── test_ingest.py
├── utils/
│   └── logger.py           # centralized logging module
├── .env                    # gitignored
├── .gitignore
├── docker-compose.yml
└── requirements.txt
```

---

## Bonus: Conceptual OLAP Star Schema

The 3NF model built here is optimized for **data integrity** — every fact lives in one place, updates propagate automatically, and there is no redundancy. This is ideal for writing and maintaining data correctly.

A star schema is a different model optimized for **analytical queries** — BI tools like Tableau, Power BI, and Looker read it faster because it requires fewer JOINs. The tradeoff is intentional redundancy: dimension data is denormalized into flat, wide tables around a central fact.

```
             dim_date
                |
dim_company — fact_job_postings — dim_location
                |
          bridge_fact_skills
                |
            dim_skill
```

**Fact table** — `fact_job_postings`: one row per job posting. Measures: `salary_year_avg`, `salary_hour_avg`, `job_count` (always 1, used for COUNT aggregations in dashboards).

**Dimensions**:
- `dim_date`: derived from `job_posted_date` — year, quarter, month, week
- `dim_company`: company name
- `dim_location`: city, country, remote flag
- `dim_job_type`: schedule type and boolean flags

**Handling `job_skills`**: skills cannot be flattened into the fact table without creating one row per skill, which inflates the fact and breaks salary averages. The standard solution is a bridge table — `bridge_fact_skills` connects `fact_job_postings` to `dim_skill`, preserving the many-to-many relationship in the OLAP layer exactly as in the 3NF model.

**Handling boolean flags**: `work_from_home`, `no_degree_mention`, and `health_insurance` are low-cardinality flags with no natural dimension of their own. They would be consolidated into a single `dim_job_attributes` junk dimension — a standard dimensional modeling technique that reduces the number of foreign keys in the fact table while keeping the flags queryable.