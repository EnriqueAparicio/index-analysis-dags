"""
dag_factory.py
==============
Lee dags_config.yml y genera todos los DAGs de Airflow dinámicamente.
Para añadir o modificar un DAG, edita SOLO dags_config.yml — no toques este archivo.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
CONFIG_PATH = Path(__file__).parent / "dags_config.yml"


def _timeout(task_cfg: dict) -> timedelta:
    if "timeout_hours" in task_cfg:
        return timedelta(hours=task_cfg["timeout_hours"])
    return timedelta(minutes=task_cfg.get("timeout_minutes", 30))


def _build_dag(dag_id: str, cfg: dict, defaults: dict) -> DAG:
    retries = cfg.get("retries", defaults.get("retries", 1))
    retry_minutes = cfg.get("retry_delay_minutes", defaults.get("retry_delay_minutes", 5))

    default_args = {
        "owner": defaults.get("owner", "airflow"),
        "depends_on_past": defaults.get("depends_on_past", False),
        "email_on_failure": defaults.get("email_on_failure", False),
        "retries": retries,
        "retry_delay": timedelta(minutes=retry_minutes),
    }

    dag = DAG(
        dag_id=dag_id,
        description=cfg.get("description", ""),
        schedule=cfg.get("schedule"),
        start_date=datetime.fromisoformat(defaults.get("start_date", "2026-01-01")),
        catchup=defaults.get("catchup", False),
        default_args=default_args,
        params=cfg.get("params", {}),
        tags=cfg.get("tags", []),
    )

    task_objects: dict[str, BashOperator] = {}

    with dag:
        for task_cfg in cfg.get("tasks", []):
            task_id = task_cfg["id"]
            command = task_cfg["command"].replace("{PROJECT}", PROJECT)

            operator = BashOperator(
                task_id=task_id,
                bash_command=command,
                execution_timeout=_timeout(task_cfg),
            )
            task_objects[task_id] = operator

        # Establecer dependencias
        for task_cfg in cfg.get("tasks", []):
            for upstream_id in task_cfg.get("depends_on", []):
                task_objects[upstream_id] >> task_objects[task_cfg["id"]]

    return dag


# ── Cargar config y registrar DAGs en el globals() de Airflow ────────────────
_raw = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
_defaults = _raw.get("defaults", {})

for _dag_id, _cfg in _raw.get("dags", {}).items():
    globals()[_dag_id] = _build_dag(_dag_id, _cfg, _defaults)
