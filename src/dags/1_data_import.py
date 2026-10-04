from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import psycopg2
import vertica_python
import csv
import tempfile
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook


DEFAULT_CONF = {
  "date_from": "2022-10-01",
  "date_to": "2022-10-31" #пока оставлю так, далее можно поставить 31-12-2999 или другую и реализовать загрузку только за день
}

def get_vertica_conn_info():
    """Вспомогательная функция для получения параметров из Connection 'Vertica'"""
    v_info = BaseHook.get_connection('Vertica_CONNECTION')
    return {
        'host': v_info.host,
        'port': v_info.port,
        'user': v_info.login,
        'password': v_info.password,
        'database': v_info.schema,
        'connection_timeout': 600
    }


def load_transactions(**context):
    conf = context["dag_run"].conf or DEFAULT_CONF


    date_from = conf.get("date_from")
    date_to = conf.get("date_to")

    if not date_from or not date_to:
        raise ValueError("date_from and date_to must be provided in dag_run.conf")

    date_from = datetime.fromisoformat(date_from).date()
    date_to = datetime.fromisoformat(date_to).date()

    pg_hook = PostgresHook(postgres_conn_id="postgre")
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    vt_auth = get_vertica_conn_info()
    vt_conn = vertica_python.connect(**vt_auth)
    vt_cur = vt_conn.cursor()

    current_date = date_from

    while current_date <= date_to:
        pg_cur.execute(
            """
            SELECT
                operation_id,
                account_number_from,
                account_number_to,
                currency_code,
                country,
                status,
                transaction_type,
                amount,
                transaction_dt
            FROM public.transactions
            WHERE transaction_dt::date = %s
            """,
            (current_date,)
        )

        rows = pg_cur.fetchall()

        if rows:
            vt_cur.execute(
                """
                DELETE
                FROM VT260525088095__STAGING.transactions
                WHERE transaction_dt::date = %s
                """,
                (current_date,)
            )

            with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
                writer = csv.writer(f)
                writer.writerows(rows)
                temp_path = f.name

            copy_sql = """
                COPY VT260525088095__STAGING.transactions (
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                )
                FROM STDIN
                DELIMITER ','
                NULL ''
            """

            with open(temp_path, "r") as file:
                vt_cur.copy(copy_sql, file)

            vt_conn.commit()

        current_date += timedelta(days=1)

    pg_cur.close()
    pg_conn.close()
    vt_cur.close()
    vt_conn.close()


def load_currencies(**context):
    conf = context["dag_run"].conf or DEFAULT_CONF

    date_from = conf.get("date_from")
    date_to = conf.get("date_to")

    if not date_from or not date_to:
        raise ValueError("date_from and date_to must be provided in dag_run.conf")

    date_from = datetime.fromisoformat(date_from).date()
    date_to = datetime.fromisoformat(date_to).date()

    pg_hook = PostgresHook(postgres_conn_id="postgre")
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    vt_auth = get_vertica_conn_info()
    vt_conn = vertica_python.connect(**vt_auth)
    vt_cur = vt_conn.cursor()

    current_date = date_from

    while current_date <= date_to:
        pg_cur.execute(
            """
            SELECT
                date_update,
                currency_code,
                currency_code_with,
                currency_with_div
            FROM public.currencies
            WHERE date_update::date = %s
            """,
            (current_date,)
        )

        rows = pg_cur.fetchall()

        if rows:
            vt_cur.execute(
                """
                DELETE
                FROM VT260525088095__STAGING.currencies
                WHERE date_update::date = %s
                """,
                (current_date,)
            )

            # 2. Пишем данные во временный CSV
            with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
                writer = csv.writer(f)
                writer.writerows(rows)
                temp_path = f.name

            # 3. COPY в Vertica
            copy_sql = """
                COPY VT260525088095__STAGING.currencies (
                    date_update,
                    currency_code,
                    currency_code_with,
                    currency_with_div
                )
                FROM STDIN
                DELIMITER ','
                NULL ''
            """

            with open(temp_path, "r") as file:
                vt_cur.copy(copy_sql, file)

            vt_conn.commit()

        current_date += timedelta(days=1)

    # закрываем коннекты после цикла
    pg_cur.close()
    pg_conn.close()
    vt_cur.close()
    vt_conn.close()


with DAG(
    dag_id="pg_to_vertica_stg",
    start_date=pendulum.datetime(2022, 10, 1),
    schedule_interval=None,
    catchup=False,
    tags=["staging", "vertica"],
) as dag:

    start = EmptyOperator(task_id="start")

    load_transactions_task = PythonOperator(
        task_id="load_transactions",
        python_callable=load_transactions,
        provide_context=True,
    )

    load_currencies_task = PythonOperator(
        task_id="load_currencies",
        python_callable=load_currencies,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> [load_transactions_task, load_currencies_task] >> end
