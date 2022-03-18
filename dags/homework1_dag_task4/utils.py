import pandas as pd


def extract_currency(date: str, table_name: str, conn, **context) -> None:
    """Load currency for given date to sqlite table"""
    url = f"https://api.exchangerate.host/timeseries?start_date={date}&end_date={date}&base=EUR&symbols=USD&format=csv"
    currency = pd.read_csv(url)
    currency.to_sql(table_name, conn, if_exists="replace", index=False)


def extract_data(date: str, table_name: str, conn, **context) -> None:
    """Load data for given date to sqlite table"""
    url = f"https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/{date}.csv"
    data = pd.read_csv(url)
    data.to_sql(table_name, conn, if_exists="replace", index=False)


def sql_query(sql, conn):
    """Give a result of sql query in pd dataframe type"""
    command = sql.split()[0]
    cursor = conn.cursor()
    cursor.execute(sql)
    if command.lower() == "select":
        return pd.read_sql_query(sql, conn)


def join_tables(join_table, table1: str, table2: str, conn, **context):
    """Join tables and inset the result to another table"""
    join_df = sql_query(
        "select D.date, C.code, C.base, C.rate, D.value  "
        "from {} as C inner join {} as D on (C.start_date=D.date)".format(
            table1, table2
        ),
        conn,
    )
    join_df.to_sql(join_table, conn, if_exists="append", index=False)
