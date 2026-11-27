"""Callbacks de ciclo de vida das tasks."""

import logging

log = logging.getLogger(__name__)


def alerta_sucesso(context):
    task_id = context["task_instance"].task_id
    log.info("Task %s concluída com sucesso.", task_id)


def alerta_retry(context):
    ti = context["task_instance"]
    log.warning(
        "Task %s falhou na tentativa %s e será executada novamente. Erro: %s",
        ti.task_id,
        # No callback de retry o contador já pode apontar para a próxima tentativa
        ti.try_number - 1,
        context.get("exception"),
    )


def alerta_falha(context):
    ti = context["task_instance"]
    log.error(
        "Falha na DAG: %s | Task: %s | Run: %s | Erro: %s",
        ti.dag_id,
        ti.task_id,
        context["run_id"],
        context.get("exception"),
    )
