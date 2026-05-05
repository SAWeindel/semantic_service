"""TransformerCell (TC/C01) — KG-driven process execution.

Polls the KG for:
  * 'disassemble'  — disassemble an assembly (Scenario 1)
  * 'reprocess'    — reprocess via special machines (Scenario 3)
  * 'command'      — execute a named process; failure notifies PH (Scenario 5)

All status transitions, quality updates, and possession changes are written
to the KG.  No direct REST callbacks to PC or PH are needed.
"""

import json
import time
import threading
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse
from station_base import KGPollingMixin
import kg_writer as kgw

CFOP = "https://w3id.org/circularfactory/Operations#"
NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances#StationInstances")

_PROCESS_TIME = 2.0


class TransformerCell(KGPollingMixin, Service):
    """Transformer Cell (TC/C01).

    Reads pending process operations from the KG, executes them, and writes
    all results (quality records, step updates) back to the KG.
    """

    NAMED_GRAPH = NAMED_GRAPH

    def __init__(
        self,
        tc_id: IRI,
        ogm: OGM,
        pc_id: Optional[IRI] = None,
        ph_id: Optional[IRI] = None,
        sm_ids: Optional[list] = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.pc_id  = IRI(pc_id) if pc_id else None
        self.ph_id  = IRI(ph_id) if ph_id else None
        self.sm_ids = [IRI(s) for s in (sm_ids or [])]
        self._lock  = threading.Lock()
        self.door_open      = False
        self.active_processes: dict = {}

        super().__init__(service_id=tc_id, ogm=ogm, host=host)

        @self.mw.app.get("/status")
        def get_status() -> dict:
            with self._lock:
                return {
                    "door_open":        self.door_open,
                    "active_processes": dict(self.active_processes),
                }

    def on_start(self) -> None:
        self.register_op_handler("disassemble", self._handle_disassemble)
        self.register_op_handler("reprocess",   self._handle_reprocess)
        self.register_op_handler("command",     self._handle_command)
        super().on_start()

    # ------------------------------------------------------------------
    # KG-driven handlers
    # ------------------------------------------------------------------

    def _handle_disassemble(self, record: dict) -> dict:
        """Scenario 1: disassemble an assembly, write results to KG."""
        product_id = record["workpiece_ref"]
        self.logger.info("Disassembly started for '%s'", product_id)
        with self._lock:
            self.active_processes[product_id] = "disassembly:running"

        time.sleep(_PROCESS_TIME)

        components = [f"{product_id}_upper", f"{product_id}_lower"]
        self.logger.info("Disassembly done: '%s' → %s", product_id, components)

        # Write quality records for each resulting component
        for comp in components:
            kgw.create_quality_record(
                self.ogm, NAMED_GRAPH,
                workpiece_ref=comp,
                quality_data={"origin": product_id, "state": "disassembled"},
                strategy="visual",
                quality_score=1.0,
            )

        with self._lock:
            self.active_processes[product_id] = "disassembly:done"

        return {"product_id": product_id, "components": components, "status": "done"}

    def _handle_reprocess(self, record: dict) -> dict:
        """Scenario 3: reprocess a component, delegate steps to SpecialMachine via KG."""
        product_id   = record["workpiece_ref"]
        process_type = record["parameters"].get("processType", "mill")

        self.logger.info("Reprocessing '%s' via '%s'", product_id, process_type)
        with self._lock:
            self.active_processes[product_id] = "reprocessing:running"

        # Dispatch to a SpecialMachine via KG (if available)
        sm_op_iri = None
        if self.sm_ids:
            sm_op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.sm_ids[0],
                workpiece_ref=product_id,
                op_type="process_step",
                parameters={"processType": process_type},
            )
            status = kgw.wait_for_operation(
                self.ogm.db, NAMED_GRAPH, sm_op_iri, timeout=30
            )
            sm_result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, sm_op_iri)
            quality_score = (sm_result or {}).get("quality_score", 0.95)
        else:
            time.sleep(_PROCESS_TIME)
            quality_score = 0.95

        quality = {
            "product_id":    product_id,
            "process":       process_type,
            "result":        "reprocessed",
            "quality_score": quality_score,
        }
        # Write quality record
        kgw.create_quality_record(
            self.ogm, NAMED_GRAPH,
            workpiece_ref=product_id,
            quality_data=quality,
            strategy=process_type,
            quality_score=quality_score,
        )

        with self._lock:
            self.active_processes[product_id] = "reprocessing:done"

        self.logger.info("Reprocessing done for '%s'", product_id)
        return quality

    def _handle_command(self, record: dict) -> dict:
        """Scenario 5: execute a Command; on failure dispatch StatusUpdate to PH via KG."""
        product_id   = record["workpiece_ref"]
        component_id = record["parameters"].get("componentId", "")
        process_type = record["parameters"].get("processType", "")

        self.logger.info(
            "Command: product=%s component=%s process=%s",
            product_id, component_id, process_type,
        )
        time.sleep(_PROCESS_TIME)

        # Simulate 'unscrew' always failing (Scenario 5)
        if process_type == "unscrew":
            self.logger.warning("Command '%s' FAILED for '%s'", process_type, component_id)
            # Write StatusUpdate to KG for PH to pick up
            if self.ph_id:
                kgw.create_operation_record(
                    self.ogm, NAMED_GRAPH,
                    station_id=self.ph_id,
                    workpiece_ref=product_id,
                    op_type="status_update",
                    parameters={
                        "componentId": component_id,
                        "processType": process_type,
                        "status":      "failed",
                    },
                )
            # Re-raise so set_operation_failed is called by the mixin
            raise RuntimeError(f"Process '{process_type}' failed on '{component_id}'")

        self.logger.info("Command '%s' succeeded for '%s'", process_type, component_id)
        if self.ph_id:
            kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.ph_id,
                workpiece_ref=product_id,
                op_type="status_update",
                parameters={
                    "componentId": component_id,
                    "processType": process_type,
                    "status":      "done",
                },
            )
        return {
            "product_id":   product_id,
            "component_id": component_id,
            "process_type": process_type,
            "status":       "done",
        }

    # ------------------------------------------------------------------
    # REST workflows — backwards-compatible
    # ------------------------------------------------------------------

    @Service.workflow(workflow_class=IRI(f"{CFOP}OpenDoorWorkflow"), key="open_door")
    def open_door(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["open_door"].response_model
        with self._lock:
            self.door_open = True
        self.logger.info("Door opened")
        return response_model(
            status_code=200, status="success",
            message="Door is now open", content=json.dumps({"door": "open"}),
        )

    @Service.workflow(workflow_class=IRI(f"{CFOP}CloseDoorWorkflow"), key="close_door")
    def close_door(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["close_door"].response_model
        with self._lock:
            self.door_open = False
        self.logger.info("Door closed")
        return response_model(
            status_code=200, status="success",
            message="Door is now closed", content=json.dumps({"door": "closed"}),
        )

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecuteDisassemblyWorkflow"),
        key="execute_disassembly",
    )
    def execute_disassembly(self, payload: WorkflowPayload) -> WorkflowResponse:
        """REST path: create a disassemble OperationRecord and wait."""
        response_model = self.workflows["execute_disassembly"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="disassemble",
            )
            status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, op_iri, timeout=60)
            result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, op_iri)
            return response_model(
                status_code=200 if status == "done" else 500,
                status=status,
                message=f"Disassembly '{product_id}': {status}",
                content=json.dumps(result or {}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecuteReprocessingWorkflow"),
        key="execute_reprocessing",
    )
    def execute_reprocessing(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["execute_reprocessing"].response_model
        try:
            product_id   = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            process_type = str(getattr(payload, IRI(f"{CFOP}processType").lined))
            op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="reprocess",
                parameters={"processType": process_type},
            )
            status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, op_iri, timeout=60)
            result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, op_iri)
            return response_model(
                status_code=200 if status == "done" else 500,
                status=status,
                message=f"Reprocessing '{product_id}' via '{process_type}': {status}",
                content=json.dumps(result or {}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReceiveCommandWorkflow"),
        key="receive_command",
    )
    def receive_command(self, payload: WorkflowPayload) -> WorkflowResponse:
        """REST path: create a command OperationRecord and wait."""
        response_model = self.workflows["receive_command"].response_model
        try:
            product_id   = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            component_id = str(getattr(payload, IRI(f"{CFOP}componentId").lined))
            process_type = str(getattr(payload, IRI(f"{CFOP}processType").lined))
            op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="command",
                parameters={"componentId": component_id, "processType": process_type},
            )
            status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, op_iri, timeout=60)
            result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, op_iri)
            return response_model(
                status_code=200 if status == "done" else 200,
                status=status,
                message=f"Command '{process_type}' for '{component_id}': {status}",
                content=json.dumps(result or {}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}QueryMachineStatusWorkflow"),
        key="query_machine_status",
    )
    def query_machine_status(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["query_machine_status"].response_model
        machine_id = str(getattr(payload, IRI(f"{CFOP}machineId").lined, ""))
        return response_model(
            status_code=200, status="success",
            message=f"Machine '{machine_id}' available",
            content=json.dumps({"machine_id": machine_id, "status": "available"}),
        )

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReportStepResultWorkflow"),
        key="report_step_result",
    )
    def report_step_result(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["report_step_result"].response_model
        try:
            product_id   = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            quality_data = str(getattr(payload, IRI(f"{CFOP}qualityData").lined))
            data = json.loads(quality_data) if quality_data.startswith("{") else {"raw": quality_data}
            kgw.create_quality_record(
                self.ogm, NAMED_GRAPH,
                workpiece_ref=product_id,
                quality_data=data,
                strategy="machine_step",
                quality_score=float(data.get("quality_score", 0.0)),
            )
            return response_model(
                status_code=200, status="success",
                message=f"Step result for '{product_id}' stored",
                content=json.dumps({"product_id": product_id}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")
