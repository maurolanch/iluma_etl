from sqlalchemy import create_engine, text

def create_tables(engine, sql_path):

    with open(sql_path, "r") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))