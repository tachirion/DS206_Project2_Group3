import uuid
import logging
from datetime import datetime

from pipeline_dimensional_data import config
from pipeline_dimensional_data import tasks
from logging import getLogger

logger = getLogger(__name__)

class DimensionalDataFlow:
    def __init__(self, execution_id: str = None):
        self.execution_id = execution_id or str(uuid.uuid4())
        logger.info(f"Creating DimensionalDataFlow (execution_id={self.execution_id})")

    def exec(self, start_date: str, end_date: str) -> dict:
        """
        Executes the pipeline sequentially.
        start_date, end_date: strings in 'YYYY-MM-DD' format expected by the SQL scripts.
        Returns a dict with overall status and per-task results.
        """
        results = {"execution_id": self.execution_id, "start_date": start_date, "end_date": end_date, "tasks": {}}

        for dim_task_key in config.DIM_ORDER:
            task_name = dim_task_key
            logger.info(f"[{self.execution_id}] Starting task: {task_name}")
            task_fn = getattr(tasks, f"task_{task_name.split('dim_')[-1]}", None)
            if not task_fn:
                task_fn = getattr(tasks, f"task_{task_name}", None)
            if not task_fn:
                logger.error(f"[{self.execution_id}] Task function not found for: {task_name}")
                results["tasks"][task_name] = {"success": False, "error": "task function not found"}
                return results

            res = task_fn(start_date, end_date, self.execution_id)
            results["tasks"][task_name] = res
            if not res.get("success"):
                logger.error(f"[{self.execution_id}] Task failed: {task_name} -> {res.get('error')}")
                return results
            logger.info(f"[{self.execution_id}] Task completed: {task_name}")

        logger.info(f"[{self.execution_id}] Starting task: fact_orders")
        res_fact = tasks.task_fact_orders(start_date, end_date, self.execution_id)
        results["tasks"]["fact_orders"] = res_fact
        if not res_fact.get("success"):
            logger.error(f"[{self.execution_id}] Fact task failed -> {res_fact.get('error')}")
            return results
        logger.info(f"[{self.execution_id}] Fact task completed")

        logger.info(f"[{self.execution_id}] Starting task: fact_error")
        res_err = tasks.task_fact_error(start_date, end_date, self.execution_id)
        results["tasks"]["fact_error"] = res_err
        if not res_err.get("success"):
            logger.error(f"[{self.execution_id}] Fact error task failed -> {res_err.get('error')}")
            return results
        logger.info(f"[{self.execution_id}] Fact error task completed")

        return results
