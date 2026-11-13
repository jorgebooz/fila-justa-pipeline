"""DAG de teste do passo 1: confirma que o Airflow enxerga o warehouse (Postgres).

Sai de cena quando a DAG real (fila_justa_pipeline.py) existir.
"""
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="teste_conexao_warehouse",
    schedule=None,  # só disparo manual
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["teste"],
)
def teste_conexao_warehouse():
    @task
    def selecionar_um():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="warehouse")
        resultado = hook.get_first("SELECT 1, current_database()")
        print(f"Warehouse respondeu: {resultado}")
        if resultado[0] != 1:
            raise ValueError("Resposta inesperada do warehouse")

    selecionar_um()


teste_conexao_warehouse()
