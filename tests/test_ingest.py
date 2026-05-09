import json
import pytest
import pandas as pd
from ingestion.ingest_jobs import sanitize_json_columns


# --- job_skills tests ---

def test_sanitize_valid_list():
    """Valid Python list string converts to JSON array."""
    df = pd.DataFrame({
        'job_skills': ["['python', 'sql']"],
        'job_type_skills': ["{'programming': ['python']}"]
    })
    result = sanitize_json_columns(df)
    assert result['job_skills'][0] == '["python", "sql"]'


def test_sanitize_empty_string():
    """Empty string returns None."""
    df = pd.DataFrame({
        'job_skills': [""],
        'job_type_skills': [""]
    })
    result = sanitize_json_columns(df)
    assert result['job_skills'][0] is None


def test_sanitize_nan_string():
    """String 'nan' returns None."""
    df = pd.DataFrame({
        'job_skills': ["nan"],
        'job_type_skills': ["nan"]
    })
    result = sanitize_json_columns(df)
    assert result['job_skills'][0] is None


def test_sanitize_corrupted_value():
    """Unparseable value returns None without raising."""
    df = pd.DataFrame({
        'job_skills': ["[invalid}"],
        'job_type_skills': ["[invalid}"]
    })
    result = sanitize_json_columns(df)
    assert result['job_skills'][0] is None


def test_sanitize_already_valid_json():
    """Already valid JSON string passes through correctly."""
    df = pd.DataFrame({
        'job_skills': ['["python", "sql"]'],
        'job_type_skills': ['{"programming": ["python"]}']
    })
    result = sanitize_json_columns(df)
    assert result['job_skills'][0] == '["python", "sql"]'


# --- job_type_skills tests ---

def test_sanitize_nested_dict():
    """Nested dict with lists converts correctly to JSON object."""
    df = pd.DataFrame({
        'job_skills': ["['python', 'sql']"],
        'job_type_skills': ["{'programming': ['python', 'sql'], 'cloud': ['aws']}"]
    })
    result = sanitize_json_columns(df)
    parsed = json.loads(result['job_type_skills'][0])
    assert parsed['programming'] == ['python', 'sql']
    assert parsed['cloud'] == ['aws']


def test_sanitize_empty_dict():
    """Empty dict is valid JSON — distinct from NULL."""
    df = pd.DataFrame({
        'job_skills': ["[]"],
        'job_type_skills': ["{}"]
    })
    result = sanitize_json_columns(df)
    assert result['job_skills'][0] == '[]'
    assert result['job_type_skills'][0] == '{}'


def test_sanitize_columns_independent():
    """Corrupted job_skills does not affect job_type_skills."""
    df = pd.DataFrame({
        'job_skills': ["[invalid}"],
        'job_type_skills': ["{'programming': ['python']}"]
    })
    result = sanitize_json_columns(df)
    assert result['job_skills'][0] is None
    assert result['job_type_skills'][0] is not None