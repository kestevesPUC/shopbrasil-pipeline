"""
Pipeline de preços por categoria - ShopBrasil

Ingestão:      buscar_produtos                              (linear)
Análise:       agrupar_por_categoria -> calcular_metricas   (fan-out)
               consolidar_metricas                          (fan-in)
Persistência:  salvar_metricas
"""

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

from callbacks import alerta_falha, alerta_retry, alerta_sucesso

log = logging.getLogger(__name__)

API_URL = "https://fakestoreapi.com/products"
POSTGRES_CONN_ID = "postgres_analytics"
POOL = "ecommerce_pool"
FUSO = "America/Sao_Paulo"

DEFAULT_ARGS = {
    "owner": "time-de-dados",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": alerta_falha,
}


@dag(
    dag_id="precos_por_categoria",
    description="Métricas diárias de preço por categoria",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=FUSO),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["shopbrasil", "precos"],
    doc_md=__doc__,
)
def precos_por_categoria():

    @task(
        retries=4,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        on_success_callback=alerta_sucesso,
        on_retry_callback=alerta_retry,
        on_failure_callback=alerta_falha,
    )
    def buscar_produtos():
        try:
            resposta = requests.get(API_URL, timeout=30)
            resposta.raise_for_status()
            produtos = resposta.json()
        except (requests.RequestException, ValueError) as erro:
            log.error("Erro ao buscar produtos: %s", erro)
            raise

        log.info("%d produtos recebidos da API", len(produtos))

        # Apenas os campos usados seguem pelo XCom
        return [
            {
                "id": produto.get("id"),
                "categoria": produto.get("category"),
                "preco": produto.get("price"),
            }
            for produto in produtos
        ]

    @task
    def agrupar_por_categoria(produtos):
        precos = {}
        for produto in produtos:
            precos.setdefault(produto["categoria"], []).append(produto["preco"])

        return [
            {"categoria": categoria, "precos": lista}
            for categoria, lista in sorted(precos.items())
        ]

    @task(pool=POOL)
    def calcular_metricas(grupo):
        precos = grupo["precos"]
        metricas = {
            "categoria": grupo["categoria"],
            "preco_medio": round(sum(precos) / len(precos), 2),
            "preco_minimo": min(precos),
            "preco_maximo": max(precos),
            "quantidade_produtos": len(precos),
        }
        log.info("Métricas calculadas: %s", metricas)
        return metricas

    @task
    def consolidar_metricas(metricas):
        consolidado = list(metricas)
        total = sum(item["quantidade_produtos"] for item in consolidado)
        log.info("%d categorias e %d produtos consolidados", len(consolidado), total)
        return consolidado

    @task
    def salvar_metricas(metricas, data_interval_end=None, run_id=None):
        # Dia em que a run agendada executa, no horário de Brasília.
        # O valor é fixo para a run, então reprocessar grava na mesma data.
        data_referencia = data_interval_end.in_timezone(FUSO).date()

        linhas = [
            (
                data_referencia,
                item["categoria"],
                item["preco_medio"],
                item["preco_minimo"],
                item["preco_maximo"],
                item["quantidade_produtos"],
                run_id,
            )
            for item in metricas
        ]
        colunas = """
            (data_referencia, categoria, preco_medio, preco_minimo,
             preco_maximo, quantidade_produtos, dag_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            # Gravação idempotente: substitui as linhas da data na mesma transação
            cursor.execute(
                "DELETE FROM metricas_categoria WHERE data_referencia = %s",
                (data_referencia,),
            )
            cursor.executemany(f"INSERT INTO metricas_categoria {colunas}", linhas)
            conn.commit()
        except Exception:
            conn.rollback()
            log.exception("Erro ao salvar as métricas")
            raise
        finally:
            cursor.close()
            conn.close()

        log.info("%d categorias salvas para %s", len(linhas), data_referencia)

    with TaskGroup("ingestao"):
        produtos = buscar_produtos()

    with TaskGroup("analise"):
        grupos = agrupar_por_categoria(produtos)
        metricas = calcular_metricas.expand(grupo=grupos)
        consolidado = consolidar_metricas(metricas)

    with TaskGroup("persistencia"):
        salvar_metricas(consolidado)


precos_por_categoria()
