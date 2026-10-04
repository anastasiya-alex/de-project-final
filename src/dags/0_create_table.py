from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


with DAG(
    dag_id="create_table",
    start_date=datetime(2022, 10, 1),   
    catchup=False,
) as dag:

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id="Vertica_CONNECTION",          
        sql="sql/create_table_stg.sql",    
        autocommit=True,                   
    )

    create_dwh_table = SQLExecuteQueryOperator(
        task_id="create_dwh_table",
        conn_id="Vertica_CONNECTION",         
        sql="sql/create_table_dwh.sql",    
        autocommit=True,                   
    )

    [create_stg_table, create_dwh_table]
