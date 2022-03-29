import pandas as pd



def extract_currency(date: str, **context) -> None:
    """Load currency for given date to sqlite table"""
    url = f"https://api.exchangerate.host/timeseries?start_date={date}&end_date={date}&base=EUR&symbols=USD&format=csv"
    currency = pd.read_csv(url)["rate"].iloc[0]
    context['ti'].xcom_push(key="currency", value=currency)


def extract_and_fill_data(date: str, table_name: str, conn, **context) -> None:
    """Load data for given date to sqlite table"""
    url = f"https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/{date}.csv"
    data = pd.read_csv(url)
    currency = context['ti'].xcom_pull(key="currency", task_ids="extract_currency")
    data = data.assign(currency=currency)
    data.to_sql(table_name, conn, if_exists="replace", index=False)
