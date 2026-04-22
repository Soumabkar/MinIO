from sqlalchemy import create_engine, text
import pandas as pd
from config import TRINO_HOST, TRINO_PORT, TRINO_USER


def get_engine(catalog: str, schema: str):
    return create_engine(
        f"trino://{TRINO_USER}@{TRINO_HOST}:{TRINO_PORT}/{catalog}/{schema}",
        connect_args={"http_scheme": "http"}
    )


def read_table(catalog: str, schema: str, table: str, query: str = None) -> pd.DataFrame:
    engine = get_engine(catalog, schema)
    sql = query or f"SELECT * FROM {table}"
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn)


def insert_dataframe(df: pd.DataFrame, catalog: str, schema: str, table: str) -> int:
    """
    Insère le DataFrame via des VALUES batch dans Trino.
    Retourne le nombre de lignes insérées.
    """
    if df.empty:
        print("DataFrame vide, rien à insérer.")
        return 0

    engine = get_engine(catalog, schema)
    cols = ", ".join(df.columns)

    def format_val(v):
        if v is None:
            return "NULL"
        if isinstance(v, float):
            return str(v)
        return f"'{v}'"

    rows_sql = ",\n".join(
        f"({', '.join(format_val(v) for v in row)})"
        for row in df.itertuples(index=False)
    )

    sql = f"INSERT INTO {catalog}.{schema}.{table} ({cols})\nVALUES\n{rows_sql}"

    with engine.connect() as conn:
        conn.execute(text(sql))

    print(f"{len(df)} lignes insérées dans {catalog}.{schema}.{table}")
    return len(df)
