"""ProductionControl (PC/A01) — central orchestrator.

Exposes:
  receive_quality_update    — IS or TC reports quality results; PC decides next step.
  receive_operation_complete — TC or ILS reports a finished station-level operation.

Calls (remote, discovered from KG):
  IS  : suggest_measurement_strategy
  IS  : perform_inspection
  ILS : execute_transport
  TC  : execute_disassembly
  TC  : execute_reprocessing
  TC  : receive_command
  MS  : execute_pick_and_place
"""

import json
import threading
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse

CFOP = "https://w3id.org/circularfactory/Operations#"


class ProductionControl(Service):
    """Production Control service (PC/A01).

    Acts as the system-level decision-maker.  It exposes two inbound workflows
    so that other stations can push updates to it, and discovers remote workflows
    on connected stations at startup.
    """

    NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances")

    def __init__(
        self,
        pc_id: IRI,
        ogm: OGM,
        ils_id: Optional[IRI] = None,
        tc_id: Optional[IRI] = None,
        is_id: Optional[IRI] = None,
        ms_id: Optional[IRI] = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.ils_id = IRI(ils_id) if ils_id else None
        self.tc_id = IRI(tc_id) if tc_id else None
        self.is_id = IRI(is_id) if is_id else None
        self.ms_id = IRI(ms_id) if ms_id else None
        self._lock = threading.Lock()
        self.pending_operations: dict = {}

        super().__init__(service_id=pc_id, ogm=ogm, host=host)

        @self.mw.app.get("/operations")
        def get_operations() -> dict:
            with self._lock:
                return {"pending_operations": self.pending_operations.copy()}

    # ------------------------------------------------------------------
    # Service hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """Discover remote workflows on all connected stations."""
        self._discover_remote(
            self.ils_id,
            [
                ("ils_execute_transport", IRI(f"{CFOP}ExecuteTransportWorkflow")),
                ("ils_update_possession", IRI(f"{CFOP}UpdatePossessionWorkflow")),
            ],
        )
        self._discover_remote(
            self.tc_id,
            [
                ("tc_disassemble", IRI(f"{CFOP}ExecuteDisassemblyWorkflow")),
                ("tc_reprocess", IRI(f"{CFOP}ExecuteReprocessingWorkflow")),
                ("tc_receive_command", IRI(f"{CFOP}ReceiveCommandWorkflow")),
                ("tc_open_door", IRI(f"{CFOP}OpenDoorWorkflow")),
                ("tc_close_door", IRI(f"{CFOP}CloseDoorWorkflow")),
            ],
        )
        self._discover_remote(
            self.is_id,
            [
                ("is_suggest_strategy", IRI(f"{CFOP}SuggestMeasurementStrategyWorkflow")),
                ("is_perform_inspection", IRI(f"{CFOP}PerformInspectionWorkflow")),
            ],
        )
        self._discover_remote(
            self.ms_id,
            [
                ("ms_pick_and_place", IRI(f"{CFOP}ExecutePickAndPlaceWorkflow")),
            ],
        )

    def _discover_remote(self, resource_id: Optional[IRI], mappings: list) -> None:
        if resource_id is None:
            return
        for key, wf_class in mappings:
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
    # High-level orchestration helpers (Scenario 1, 2, 3)
    # ------------------------------------------------------------------

    def initiate_disassembly(self, product_id: str, pickup_location: str, tc_location: str) -> None:
        """Scenario 1: coordinate transport + disassembly for an assembly."""
        self.logger.info(f"Initiating disassembly for product '{product_id}'")
        self._call_transport(product_id, pickup_location, tc_location)
        self._call_tc_open_door()
        self._call_disassembly(product_id)
        self._call_tc_close_door()

    def initiate_reprocessing(self, product_id: str, from_location: str, tc_location: str, process_type: str) -> None:
        """Scenario 3: coordinate transport + reprocessing for a component."""
        self.logger.info(f"Initiating reprocessing for '{product_id}' via '{process_type}'")
        self._call_transport(product_id, from_location, tc_location)
        self._call_tc_open_door()
        self._call_reprocessing(product_id, process_type)
        self._call_tc_close_door()

    def initiate_inspection(self, product_id: str, transport_location: str) -> Optional[str]:
        """Scenario 2: query IS for strategies, choose one, and trigger inspection."""
        self.logger.info(f"Requesting measurement strategies for '{product_id}'")
        strategies = self._call_suggest_strategy(product_id)
        if not strategies:
            self.logger.error("No measurement strategies returned by IS")
            return None
        chosen = strategies[0]
        self.logger.info(f"Chosen strategy: '{chosen}'")
        self._call_transport(product_id, "storage", transport_location)
        result = self._call_perform_inspection(product_id, chosen)
        return result

    # ------------------------------------------------------------------
    # Remote workflow invocation helpers
    # ------------------------------------------------------------------

    def _call_transport(self, product_id: str, from_loc: str, to_loc: str) -> None:
        if "ils_execute_transport" not in self.remote_workflows:
            self.logger.warning("ILS transport workflow not available")
            return
        response = self.remote_workflows["ils_execute_transport"](
            **{
                IRI(f"{CFOP}productId").lined: product_id,
                IRI(f"{CFOP}fromLocation").lined: from_loc,
                IRI(f"{CFOP}toLocation").lined: to_loc,
            }
        )
        self.logger.info(f"Transport response: {response.status} — {response.message}")

    def _call_disassembly(self, product_id: str) -> None:
        if "tc_disassemble" not in self.remote_workflows:
            self.logger.warning("TC disassembly workflow not available")
            return
        response = self.remote_workflows["tc_disassemble"](
            **{IRI(f"{CFOP}productId").lined: product_id}
        )
        self.logger.info(f"Disassembly response: {response.status} — {response.message}")

    def _call_reprocessing(self, product_id: str, process_type: str) -> None:
        if "tc_reprocess" not in self.remote_workflows:
            self.logger.warning("TC reprocessing workflow not available")
            return
        response = self.remote_workflows["tc_reprocess"](
            **{
                IRI(f"{CFOP}productId").lined: product_id,
                IRI(f"{CFOP}processType").lined: process_type,
            }
        )
        self.logger.info(f"Reprocessing response: {response.status} — {response.message}")

    def _call_tc_open_door(self) -> None:
        if "tc_open_door" not in self.remote_workflows:
            return
        self.remote_workflows["tc_open_door"](
            **{IRI(f"{CFOP}doorAction").lined: "open"}
        )

    def _call_tc_close_door(self) -> None:
        if "tc_close_door" not in self.remote_workflows:
            return
        self.remote_workflows["tc_close_door"](
            **{IRI(f"{CFOP}doorAction").lined: "close"}
        )

    def _call_suggest_strategy(self, product_id: str) -> list:
        if "is_suggest_strategy" not in self.remote_workflows:
            self.logger.warning("IS strategy workflow not available")
            return []
        response = self.remote_workflows["is_suggest_strategy"](
            **{IRI(f"{CFOP}productId").lined: product_id}
        )
        if response.is_success and response.content:
            try:
                data = json.loads(response.content)
                return data.get("strategies", [])
            except json.JSONDecodeError:
                pass
        return []

    def _call_perform_inspection(self, product_id: str, strategy: str) -> Optional[str]:
        if "is_perform_inspection" not in self.remote_workflows:
            return None
        response = self.remote_workflows["is_perform_inspection"](
            **{
                IRI(f"{CFOP}productId").lined: product_id,
                IRI(f"{CFOP}measurementStrategy").lined: strategy,
            }
        )
        if response.is_success:
            return response.content
        return None

    # ------------------------------------------------------------------
    # Exposed workflows — inbound callbacks from other stations
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReceiveQualityUpdateWorkflow"),
        key="receive_quality_update",
    )
    def receive_quality_update(self, payload: WorkflowPayload) -> WorkflowResponse:
        """IS or TC pushes quality results; PC decides on the next production step."""
        response_model = self.workflows["receive_quality_update"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            quality_data = str(getattr(payload, IRI(f"{CFOP}qualityData").lined))
            self.logger.info(f"Quality update received for '{product_id}': {quality_data}")
            with self._lock:
                self.pending_operations[product_id] = {
                    "status": "quality_received",
                    "quality_data": quality_data,
                }
            return response_model(
                status_code=200,
                status="success",
                message=f"Quality data for '{product_id}' accepted",
                content=json.dumps({"product_id": product_id}),
            )
        except Exception as e:
            self.logger.error(f"receive_quality_update error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReceiveOperationCompleteWorkflow"),
        key="receive_operation_complete",
    )
    def receive_operation_complete(self, payload: WorkflowPayload) -> WorkflowResponse:
        """TC or ILS reports that a station-level operation has finished."""
        response_model = self.workflows["receive_operation_complete"].response_model
        try:
            operation_id = str(getattr(payload, IRI(f"{CFOP}operationId").lined))
            status = str(getattr(payload, IRI(f"{CFOP}operationStatus").lined))
            self.logger.info(f"Operation '{operation_id}' completed with status '{status}'")
            with self._lock:
                self.pending_operations[operation_id] = {"status": status}
            return response_model(
                status_code=200,
                status="success",
                message=f"Operation '{operation_id}' acknowledged",
                content=json.dumps({"operation_id": operation_id, "status": status}),
            )
        except Exception as e:
            self.logger.error(f"receive_operation_complete error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )
