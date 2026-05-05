"""TransformerCell (TC/C01) — process execution on workpieces.

Exposes:
  execute_disassembly    — disassemble an assembly (Scenario 1).
  execute_reprocessing   — reprocess a component via special machines (Scenario 3).
  receive_command        — accept a Command from PC or PlanHealer (Scenario 5).
  open_door              — open the automatic door for ILS handover.
  close_door             — close the automatic door.
  query_machine_status   — check SpecialMachine availability.
  report_step_result     — receive a reprocessing step result from a SpecialMachine.

Calls (remote):
  PC : receive_operation_complete — report finished/failed operations.
  PC : receive_quality_update     — push updated quality after reprocessing.
  PH : receive_status_update      — push Command status (Scenario 5).
  SM : execute_special_machine_step — delegate reprocessing steps.
"""

import json
import time
import threading
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse

CFOP = "https://w3id.org/circularfactory/Operations#"

_PROCESS_TIME_SECONDS = 2.0


class TransformerCell(Service):
    """Transformer Cell service (TC/C01).

    Executes all physical processes on workpieces: disassembly, milling,
    unscrewing, etc.  Reports success/failure to PC and PlanHealer.
    """

    NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances")

    def __init__(
        self,
        tc_id: IRI,
        ogm: OGM,
        pc_id: Optional[IRI] = None,
        ph_id: Optional[IRI] = None,
        sm_ids: Optional[list] = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.pc_id = IRI(pc_id) if pc_id else None
        self.ph_id = IRI(ph_id) if ph_id else None
        self.sm_ids: list[IRI] = [IRI(sid) for sid in (sm_ids or [])]
        self._lock = threading.Lock()
        self.door_open: bool = False
        self.active_processes: dict = {}

        super().__init__(service_id=tc_id, ogm=ogm, host=host)

        @self.mw.app.get("/status")
        def get_status() -> dict:
            with self._lock:
                return {
                    "door_open": self.door_open,
                    "active_processes": self.active_processes.copy(),
                }

    # ------------------------------------------------------------------
    # Service hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        if self.pc_id:
            for key, wf_class in [
                ("pc_operation_complete", IRI(f"{CFOP}ReceiveOperationCompleteWorkflow")),
                ("pc_quality_update", IRI(f"{CFOP}ReceiveQualityUpdateWorkflow")),
            ]:
                self._try_discover(key, self.pc_id, wf_class)

        if self.ph_id:
            self._try_discover(
                "ph_status_update",
                self.ph_id,
                IRI(f"{CFOP}ReceiveStatusUpdateWorkflow"),
            )

        for i, sm_id in enumerate(self.sm_ids):
            self._try_discover(
                f"sm_{i}_step",
                sm_id,
                IRI(f"{CFOP}ExecuteSpecialMachineStepWorkflow"),
            )
            self._try_discover(
                f"sm_{i}_status",
                sm_id,
                IRI(f"{CFOP}QueryMachineStatusWorkflow"),
            )

    def _try_discover(self, key: str, resource_id: IRI, wf_class: IRI) -> None:
        try:
            self.add_remote_workflow(
                key=key,
                resource_instance=resource_id,
                workflow_class=wf_class,
                logger_suffix=key,
            )
        except Exception as e:
            self.logger.warning(f"Could not discover remote workflow '{key}': {e}")

    # ------------------------------------------------------------------
    # Exposed workflows
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}OpenDoorWorkflow"),
        key="open_door",
    )
    def open_door(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Open the automatic door to allow ILS access."""
        response_model = self.workflows["open_door"].response_model
        try:
            with self._lock:
                self.door_open = True
            self.logger.info("Door opened")
            return response_model(
                status_code=200,
                status="success",
                message="Door is now open",
                content=json.dumps({"door": "open"}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}CloseDoorWorkflow"),
        key="close_door",
    )
    def close_door(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Close the automatic door after ILS handover."""
        response_model = self.workflows["close_door"].response_model
        try:
            with self._lock:
                self.door_open = False
            self.logger.info("Door closed")
            return response_model(
                status_code=200,
                status="success",
                message="Door is now closed",
                content=json.dumps({"door": "closed"}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecuteDisassemblyWorkflow"),
        key="execute_disassembly",
    )
    def execute_disassembly(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Disassemble an assembly into its sub-components.

        Scenario 1: after transport, TC receives the assembly, disassembles it,
        creates new product instances in the KG, and deactivates the old one.
        """
        response_model = self.workflows["execute_disassembly"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            self.logger.info(f"Disassembly started for '{product_id}'")

            with self._lock:
                self.active_processes[product_id] = "disassembly:running"

            time.sleep(_PROCESS_TIME_SECONDS)

            # Simulate: two new components created, original deactivated
            components = [f"{product_id}_upper", f"{product_id}_lower"]
            self.logger.info(
                f"Disassembly done: '{product_id}' → {components}"
            )

            with self._lock:
                self.active_processes[product_id] = "disassembly:done"

            self._notify_pc_complete(
                operation_id=f"disassembly_{product_id}", status="done"
            )

            return response_model(
                status_code=200,
                status="success",
                message=f"Disassembly complete for '{product_id}'",
                content=json.dumps(
                    {"product_id": product_id, "components": components}
                ),
            )
        except Exception as e:
            self.logger.error(f"execute_disassembly error: {e}")
            with self._lock:
                self.active_processes.pop(str(getattr(payload, IRI(f"{CFOP}productId").lined, "")), None)
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecuteReprocessingWorkflow"),
        key="execute_reprocessing",
    )
    def execute_reprocessing(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Reprocess a component using special machines.

        Scenario 3: TC queries machine status, delegates reprocessing steps,
        and updates the knowledge graph after each step.
        """
        response_model = self.workflows["execute_reprocessing"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            process_type = str(getattr(payload, IRI(f"{CFOP}processType").lined))
            self.logger.info(
                f"Reprocessing '{product_id}' via '{process_type}'"
            )

            with self._lock:
                self.active_processes[product_id] = "reprocessing:running"

            # Query special machine availability
            machine_key = self._find_available_machine()
            if machine_key is None:
                self.logger.warning("No special machine available; executing locally")
                time.sleep(_PROCESS_TIME_SECONDS)
            else:
                self._delegate_step(machine_key, product_id, process_type)

            quality_data = json.dumps({
                "product_id": product_id,
                "process": process_type,
                "result": "reprocessed",
                "quality_score": 0.97,
            })

            with self._lock:
                self.active_processes[product_id] = "reprocessing:done"

            self._push_quality_to_pc(product_id, quality_data)
            self._notify_pc_complete(
                operation_id=f"reprocessing_{product_id}", status="done"
            )

            return response_model(
                status_code=200,
                status="success",
                message=f"Reprocessing '{process_type}' done for '{product_id}'",
                content=quality_data,
            )
        except Exception as e:
            self.logger.error(f"execute_reprocessing error: {e}")
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReceiveCommandWorkflow"),
        key="receive_command",
    )
    def receive_command(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Accept a Command from PC or PlanHealer and execute it.

        Scenario 5: Command = {product, component, process}.
        On failure, TC notifies PH with a StatusUpdate so plan healing begins.
        """
        response_model = self.workflows["receive_command"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            component_id = str(getattr(payload, IRI(f"{CFOP}componentId").lined))
            process_type = str(getattr(payload, IRI(f"{CFOP}processType").lined))

            self.logger.info(
                f"Command received: product='{product_id}', "
                f"component='{component_id}', process='{process_type}'"
            )

            time.sleep(_PROCESS_TIME_SECONDS)

            # Simulate failure for 'unscrew' to demonstrate plan healing
            if process_type == "unscrew":
                self.logger.warning(
                    f"Process '{process_type}' FAILED for component '{component_id}'"
                )
                self._notify_ph_status(product_id, component_id, process_type, "failed")
                return response_model(
                    status_code=200,
                    status="failed",
                    message=f"Process '{process_type}' failed — PlanHealer notified",
                    content=json.dumps({
                        "product_id": product_id,
                        "component_id": component_id,
                        "process_type": process_type,
                        "status": "failed",
                    }),
                )

            self.logger.info(
                f"Command '{process_type}' succeeded for '{component_id}'"
            )
            self._notify_ph_status(product_id, component_id, process_type, "done")
            return response_model(
                status_code=200,
                status="success",
                message=f"Command '{process_type}' executed successfully",
                content=json.dumps({
                    "product_id": product_id,
                    "component_id": component_id,
                    "process_type": process_type,
                    "status": "done",
                }),
            )
        except Exception as e:
            self.logger.error(f"receive_command error: {e}")
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}QueryMachineStatusWorkflow"),
        key="query_machine_status",
    )
    def query_machine_status(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Check whether a SpecialMachine is available for a reprocessing step."""
        response_model = self.workflows["query_machine_status"].response_model
        try:
            machine_id = str(getattr(payload, IRI(f"{CFOP}machineId").lined))
            self.logger.info(f"Status query for machine '{machine_id}'")
            return response_model(
                status_code=200,
                status="success",
                message=f"Machine '{machine_id}' is available",
                content=json.dumps({"machine_id": machine_id, "status": "available"}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReportStepResultWorkflow"),
        key="report_step_result",
    )
    def report_step_result(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Receive a reprocessing step result from a SpecialMachine."""
        response_model = self.workflows["report_step_result"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            quality_data = str(getattr(payload, IRI(f"{CFOP}qualityData").lined))
            self.logger.info(f"Step result for '{product_id}': {quality_data}")
            self._push_quality_to_pc(product_id, quality_data)
            return response_model(
                status_code=200,
                status="success",
                message=f"Step result for '{product_id}' accepted",
                content=json.dumps({"product_id": product_id}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_available_machine(self) -> Optional[str]:
        """Return the key of the first available SpecialMachine remote workflow."""
        for i in range(len(self.sm_ids)):
            key = f"sm_{i}_status"
            if key not in self.remote_workflows:
                continue
            try:
                resp = self.remote_workflows[key](
                    **{
                        IRI(f"{CFOP}machineId").lined: str(self.sm_ids[i]),
                        IRI(f"{CFOP}operationStatus").lined: "query",
                    }
                )
                if resp.is_success:
                    return f"sm_{i}_step"
            except Exception:
                pass
        return None

    def _delegate_step(self, machine_key: str, product_id: str, process_type: str) -> None:
        response = self.remote_workflows[machine_key](
            **{
                IRI(f"{CFOP}productId").lined: product_id,
                IRI(f"{CFOP}processType").lined: process_type,
            }
        )
        self.logger.info(f"SpecialMachine step result: {response.status}")

    def _notify_pc_complete(self, operation_id: str, status: str) -> None:
        if "pc_operation_complete" not in self.remote_workflows:
            return
        try:
            self.remote_workflows["pc_operation_complete"](
                **{
                    IRI(f"{CFOP}operationId").lined: operation_id,
                    IRI(f"{CFOP}operationStatus").lined: status,
                }
            )
        except Exception as e:
            self.logger.warning(f"Could not notify PC: {e}")

    def _push_quality_to_pc(self, product_id: str, quality_data: str) -> None:
        if "pc_quality_update" not in self.remote_workflows:
            return
        try:
            self.remote_workflows["pc_quality_update"](
                **{
                    IRI(f"{CFOP}productId").lined: product_id,
                    IRI(f"{CFOP}qualityData").lined: quality_data,
                }
            )
        except Exception as e:
            self.logger.warning(f"Could not push quality data to PC: {e}")

    def _notify_ph_status(
        self,
        product_id: str,
        component_id: str,
        process_type: str,
        status: str,
    ) -> None:
        if "ph_status_update" not in self.remote_workflows:
            self.logger.debug("PH status-update workflow not registered; skipping")
            return
        try:
            self.remote_workflows["ph_status_update"](
                **{
                    IRI(f"{CFOP}productId").lined: product_id,
                    IRI(f"{CFOP}componentId").lined: component_id,
                    IRI(f"{CFOP}processType").lined: process_type,
                    IRI(f"{CFOP}operationStatus").lined: status,
                }
            )
            self.logger.info(
                f"PH notified: status='{status}' for process='{process_type}'"
            )
        except Exception as e:
            self.logger.warning(f"Could not notify PlanHealer: {e}")
