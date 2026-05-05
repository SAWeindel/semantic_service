"""SpecialMachine — KG-driven reprocessing device.

Polls the KG for 'process_step' OperationRecords assigned to this machine.
Writes a QualityRecord and marks the op done after each step.
"""

import json
import time
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse
from station_base import KGPollingMixin
import kg_writer as kgw

CFOP = "https://w3id.org/circularfactory/Operations#"
NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances#StationInstances")

_STEP_TIME = 1.5


class SpecialMachine(KGPollingMixin, Service):
    """Special Machine — additive or subtractive reprocessing device."""

    NAMED_GRAPH = NAMED_GRAPH

    def __init__(
        self,
        sm_id: IRI,
        ogm: OGM,
        tc_id: Optional[IRI] = None,
        machine_type: str = "subtractive",
        host: str = "0.0.0.0",
    ) -> None:
        self.tc_id        = IRI(tc_id) if tc_id else None
        self.machine_type = machine_type
        self.busy         = False
        super().__init__(service_id=sm_id, ogm=ogm, host=host)

        @self.mw.app.get("/status")
        def get_status() -> dict:
            return {"machine_type": self.machine_type, "busy": self.busy}

    def on_start(self) -> None:
        self.register_op_handler("process_step", self._handle_process_step)
        super().on_start()

    # ------------------------------------------------------------------
    # KG-driven handler
    # ------------------------------------------------------------------

    def _handle_process_step(self, record: dict) -> dict:
        product_id   = record["workpiece_ref"]
        process_type = record["parameters"].get("processType", "mill")

        if self.busy:
            raise RuntimeError("Machine is busy — operation will remain pending")

        self.busy = True
        self.logger.info(
            "Executing '%s' (%s) on '%s'", process_type, self.machine_type, product_id
        )
        time.sleep(_STEP_TIME)

        quality = {
            "product_id":    product_id,
            "machine_type":  self.machine_type,
            "process":       process_type,
            "status":        "done",
            "quality_score": 0.95,
        }
        # Write quality record to KG
        kgw.create_quality_record(
            self.ogm, NAMED_GRAPH,
            workpiece_ref=product_id,
            quality_data=quality,
            strategy=process_type,
            quality_score=quality["quality_score"],
        )
        self.busy = False
        self.logger.info("Step '%s' complete for '%s'", process_type, product_id)
        return quality

    # ------------------------------------------------------------------
    # REST workflows — backwards-compatible
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecuteSpecialMachineStepWorkflow"),
        key="execute_special_machine_step",
    )
    def execute_special_machine_step(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["execute_special_machine_step"].response_model
        try:
            product_id   = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            process_type = str(getattr(payload, IRI(f"{CFOP}processType").lined))
            op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="process_step",
                parameters={"processType": process_type},
            )
            status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, op_iri, timeout=30)
            result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, op_iri)
            return response_model(
                status_code=200 if status == "done" else 500,
                status=status,
                message=f"Step '{process_type}' on '{product_id}': {status}",
                content=json.dumps(result or {}),
            )
        except Exception as e:
            self.busy = False
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}QueryMachineStatusWorkflow"),
        key="query_machine_status",
    )
    def query_machine_status(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["query_machine_status"].response_model
        status = "busy" if self.busy else "available"
        return response_model(
            status_code=200, status="success",
            message=f"Machine is {status}",
            content=json.dumps({"machine_type": self.machine_type, "status": status}),
        )
