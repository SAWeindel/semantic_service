"""SpecialMachine — reprocessing device inside/attached to a TransformerCell.

Exposes:
  execute_special_machine_step — run one additive or subtractive reprocessing step.
  query_machine_status         — report current availability.

Calls (remote):
  TC : report_step_result — push quality data back to TC after each step.
"""

import json
import time
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse

CFOP = "https://w3id.org/circularfactory/Operations#"

_STEP_TIME_SECONDS = 1.5


class SpecialMachine(Service):
    """Special Machine — additive or subtractive reprocessing device."""

    NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances")

    def __init__(
        self,
        sm_id: IRI,
        ogm: OGM,
        tc_id: Optional[IRI] = None,
        machine_type: str = "subtractive",
        host: str = "0.0.0.0",
    ) -> None:
        self.tc_id = IRI(tc_id) if tc_id else None
        self.machine_type = machine_type
        self.busy: bool = False

        super().__init__(service_id=sm_id, ogm=ogm, host=host)

        @self.mw.app.get("/status")
        def get_status() -> dict:
            return {"machine_type": self.machine_type, "busy": self.busy}

    # ------------------------------------------------------------------
    # Service hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        if self.tc_id is None:
            return
        try:
            self.add_remote_workflow(
                key="tc_report_step",
                resource_instance=self.tc_id,
                workflow_class=IRI(f"{CFOP}ReportStepResultWorkflow"),
                logger_suffix="TC-step-result",
            )
        except Exception as e:
            self.logger.warning(f"Could not discover TC step-result workflow: {e}")

    # ------------------------------------------------------------------
    # Exposed workflows
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecuteSpecialMachineStepWorkflow"),
        key="execute_special_machine_step",
    )
    def execute_special_machine_step(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Execute one reprocessing step and report quality results to TC."""
        response_model = self.workflows["execute_special_machine_step"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            process_type = str(getattr(payload, IRI(f"{CFOP}processType").lined))

            if self.busy:
                return response_model(
                    status_code=409,
                    status="busy",
                    message="Machine is currently busy",
                    content="",
                )

            self.busy = True
            self.logger.info(
                f"Executing '{process_type}' ({self.machine_type}) on '{product_id}'"
            )
            time.sleep(_STEP_TIME_SECONDS)

            quality_data = json.dumps({
                "product_id": product_id,
                "machine_type": self.machine_type,
                "process": process_type,
                "status": "done",
                "quality_score": 0.95,
            })
            self.busy = False
            self.logger.info(f"Step '{process_type}' complete for '{product_id}'")

            # Push result back to TC
            self._report_to_tc(product_id, quality_data)

            return response_model(
                status_code=200,
                status="success",
                message=f"Step '{process_type}' completed on '{product_id}'",
                content=quality_data,
            )
        except Exception as e:
            self.busy = False
            self.logger.error(f"execute_special_machine_step error: {e}")
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}QueryMachineStatusWorkflow"),
        key="query_machine_status",
    )
    def query_machine_status(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Return current availability status of this machine."""
        response_model = self.workflows["query_machine_status"].response_model
        try:
            status = "busy" if self.busy else "available"
            return response_model(
                status_code=200,
                status="success",
                message=f"Machine is {status}",
                content=json.dumps({
                    "machine_type": self.machine_type,
                    "status": status,
                }),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _report_to_tc(self, product_id: str, quality_data: str) -> None:
        if "tc_report_step" not in self.remote_workflows:
            return
        try:
            self.remote_workflows["tc_report_step"](
                **{
                    IRI(f"{CFOP}productId").lined: product_id,
                    IRI(f"{CFOP}qualityData").lined: quality_data,
                }
            )
        except Exception as e:
            self.logger.warning(f"Could not report step result to TC: {e}")
