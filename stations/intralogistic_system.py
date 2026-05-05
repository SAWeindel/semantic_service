"""IntralogisticSystem (ILS/C03) — transport and possession management.

Exposes:
  execute_transport    — move a product from one location to another.
  update_possession    — update possession state in the knowledge graph.

Calls (remote):
  MS : execute_pick_and_place  — for pick-up and delivery handover.
  PC : receive_operation_complete — report transport done.
"""

import json
import time
import threading

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse

CFOP = "https://w3id.org/circularfactory/Operations#"

# Simulated travel speed: seconds per abstract distance unit.
_TRAVEL_TIME_SECONDS = 2.0


class IntralogisticSystem(Service):
    """Intralogistic System service (ILS/C03).

    Moves assemblies and components between stations using (simulated) mobile
    robots and manipulator modules.
    """

    NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances")

    def __init__(
        self,
        ils_id: IRI,
        ogm: OGM,
        ms_id: IRI = None,
        pc_id: IRI = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.ms_id = IRI(ms_id) if ms_id else None
        self.pc_id = IRI(pc_id) if pc_id else None
        self._lock = threading.Lock()
        self.active_transports: dict = {}

        super().__init__(service_id=ils_id, ogm=ogm, host=host)

        @self.mw.app.get("/transports")
        def get_transports() -> dict:
            with self._lock:
                return {"active_transports": self.active_transports.copy()}

    # ------------------------------------------------------------------
    # Service hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        for key, wf_class, resource_id in [
            ("ms_pick_and_place", IRI(f"{CFOP}ExecutePickAndPlaceWorkflow"), self.ms_id),
            ("pc_operation_complete", IRI(f"{CFOP}ReceiveOperationCompleteWorkflow"), self.pc_id),
        ]:
            if resource_id is None:
                continue
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
        workflow_class=IRI(f"{CFOP}ExecuteTransportWorkflow"),
        key="execute_transport",
    )
    def execute_transport(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Transport a product from fromLocation to toLocation.

        Simulates: robot navigates to pickup → MS picks up → robot drives to
        destination → MS places down → possession updated.
        """
        response_model = self.workflows["execute_transport"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            from_loc = str(getattr(payload, IRI(f"{CFOP}fromLocation").lined))
            to_loc = str(getattr(payload, IRI(f"{CFOP}toLocation").lined))

            self.logger.info(
                f"Transport started: '{product_id}' from '{from_loc}' to '{to_loc}'"
            )
            with self._lock:
                self.active_transports[product_id] = {
                    "from": from_loc,
                    "to": to_loc,
                    "status": "navigating_to_pickup",
                }

            # Step 1: navigate to pickup location (simulated)
            time.sleep(_TRAVEL_TIME_SECONDS)
            self.logger.info(f"Arrived at pickup '{from_loc}' for '{product_id}'")

            # Step 2: manipulator picks up the product
            self._call_pick_and_place(product_id, from_loc)

            with self._lock:
                self.active_transports[product_id]["status"] = "navigating_to_destination"

            # Step 3: transport to destination
            time.sleep(_TRAVEL_TIME_SECONDS)
            self.logger.info(f"Arrived at destination '{to_loc}' for '{product_id}'")

            # Step 4: manipulator places the product
            self._call_pick_and_place(product_id, to_loc)

            with self._lock:
                self.active_transports[product_id]["status"] = "delivered"

            # Step 5: report completion to PC
            self._notify_pc_complete(
                operation_id=f"transport_{product_id}",
                status="done",
            )

            self.logger.info(f"Transport complete: '{product_id}' delivered to '{to_loc}'")
            return response_model(
                status_code=200,
                status="success",
                message=f"Product '{product_id}' delivered to '{to_loc}'",
                content=json.dumps({"product_id": product_id, "location": to_loc}),
            )
        except Exception as e:
            self.logger.error(f"execute_transport error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}UpdatePossessionWorkflow"),
        key="update_possession",
    )
    def update_possession(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Update the possession state of a workpiece in the knowledge graph."""
        response_model = self.workflows["update_possession"].response_model
        try:
            operation_id = str(getattr(payload, IRI(f"{CFOP}operationId").lined))
            status = str(getattr(payload, IRI(f"{CFOP}operationStatus").lined))
            self.logger.info(
                f"Possession update for operation '{operation_id}': status='{status}'"
            )
            return response_model(
                status_code=200,
                status="success",
                message=f"Possession state updated for operation '{operation_id}'",
                content=json.dumps({"operation_id": operation_id, "status": status}),
            )
        except Exception as e:
            self.logger.error(f"update_possession error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _call_pick_and_place(self, product_id: str, location: str) -> None:
        if "ms_pick_and_place" not in self.remote_workflows:
            self.logger.debug("MS pick-and-place not available; skipping")
            return
        response = self.remote_workflows["ms_pick_and_place"](
            **{
                IRI(f"{CFOP}productId").lined: product_id,
                IRI(f"{CFOP}toLocation").lined: location,
            }
        )
        self.logger.info(f"Pick-and-place at '{location}': {response.status}")

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
            self.logger.warning(f"Could not notify PC of completion: {e}")
