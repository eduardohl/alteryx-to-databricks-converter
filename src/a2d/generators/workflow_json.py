"""Databricks Workflow JSON generator.

Produces a JSON job definition that can be imported into Databricks
via the Jobs API or Terraform.
"""

from __future__ import annotations

import json
import logging

from a2d.config import OutputFormat
from a2d.generators.base import CodeGenerator, GeneratedFile, GeneratedOutput
from a2d.ir.graph import WorkflowDAG

logger = logging.getLogger("a2d.generators.workflow_json")


class WorkflowJsonGenerator(CodeGenerator):
    """Generate Databricks Workflow job definition JSON."""

    def generate(self, dag: WorkflowDAG, workflow_name: str = "workflow") -> GeneratedOutput:
        warnings: list[str] = []

        # Determine notebook path based on output format
        if self.config.output_format == OutputFormat.DLT:
            notebook_path = f"/Workspace/Shared/a2d/{workflow_name}_dlt"
            task = self._build_dlt_task(workflow_name, notebook_path)
        else:
            notebook_path = f"/Workspace/Shared/a2d/{workflow_name}"
            task = self._build_notebook_task(workflow_name, notebook_path)

        job_definition = {
            "name": f"a2d_{workflow_name}",
            "description": f"Migrated from Alteryx workflow: {workflow_name}.yxmd",
            "tags": {
                "source": "alteryx",
                "migrated_by": "a2d",
                "original_workflow": f"{workflow_name}.yxmd",
            },
            "tasks": [task],
            "job_clusters": [
                {
                    "job_cluster_key": "default_cluster",
                    "new_cluster": {
                        "spark_version": f"{self.config.dbr_version}.x-scala2.12",
                        "num_workers": 2,
                        "node_type_id": "Standard_DS3_v2",
                        "spark_conf": {
                            "spark.databricks.delta.preview.enabled": "true",
                        },
                    },
                }
            ],
            "format": "MULTI_TASK",
            "max_concurrent_runs": 1,
        }

        content = json.dumps(job_definition, indent=2) + "\n"

        files = [
            GeneratedFile(
                filename=f"{workflow_name}_workflow.json",
                content=content,
                file_type="json",
            )
        ]

        stats = {
            "total_tasks": 1,
            "notebook_path": notebook_path,
        }

        return GeneratedOutput(files=files, warnings=warnings, stats=stats)

    @staticmethod
    def _build_notebook_task(workflow_name: str, notebook_path: str) -> dict:
        return {
            "task_key": f"{workflow_name}_main",
            "description": f"Run migrated workflow: {workflow_name}",
            "job_cluster_key": "default_cluster",
            "notebook_task": {
                "notebook_path": notebook_path,
                "source": "WORKSPACE",
            },
            "timeout_seconds": 3600,
            "max_retries": 0,
        }

    @staticmethod
    def _build_dlt_task(workflow_name: str, notebook_path: str) -> dict:
        return {
            "task_key": f"{workflow_name}_dlt_pipeline",
            "description": f"Run DLT pipeline: {workflow_name}",
            "pipeline_task": {
                "pipeline_id": "PLACEHOLDER_PIPELINE_ID",
                "full_refresh": False,
            },
            "timeout_seconds": 3600,
            "max_retries": 0,
        }
