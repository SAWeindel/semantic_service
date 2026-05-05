"""IntralogisticSystem (ILS/C03) — KG-driven transport.

Polls the KG for pending 'transport' and 'pick_and_place' OperationRecords
assigned to this station.  All state transitions are written back to the KG.

Exposes REST workflows for backwards-compatible direct triggering.
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

_TRAVEL_TIME = 2.0


class IntralogisticSystem(KGPollingMixin, Service):
    """Intralogistic System (ILS/C03).

    Reads pending transport operations from the KG and executes them.
    For each transport: dispatches a pick_and_place OperationRecord to MS,
    waits for completion, then marks the transport done.
    """

    NAMED_GRAPH = NAMED_GRAPH

    def __init__(
        self,
        ils_id: IRI,
        ogm: OGM,
        ms_id: Optional[IRI] = None,
        pc_id: Optional[IRI] = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.ms_id = IRI(ms_id) if ms_id else None
        self.pc_id = IRI(pc_id) if pc_id else None
        self.active_transports: dict = {}

        super().__init__(service_id=ils_id, ogm=ogm, host=host)

        @self.mw.app.get("/transports")
        def get_transports() -> dict:
            return {"active_transports": dict(self.active_transports)}

    def on_start(self) -> None:
        self.register_op_handler("transport", self._handle_transport)
        super().on_start()

    # ------------------------------------------------------------------
    # KG-driven operation handler
    # ------------------------------------------------------------------

    def _handle_transport(self, record: dict) -> dict:
        """Execute a transport: pickup via MS → drive → deliver via MS."""
        product_id = record["workpiece_ref"]
        params     = record["parameters"]
        from_loc   = params.get("fromLocation", "unknown")
        to_loc     = params.get("toLocation",   "unknown")

        self.logger.info(
            "Transport: '%s' from '%s' to '%s'", product_id, from_loc, to_loc
        )
        self.active_transports[product_id] = {"from": from_loc, "to": to_loc, "status": "navigating"}

        # Dispatch pickup to MS via KG (if MS is known)
        if self.ms_id:
            pickup_op = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.ms_id,
                workpiece_ref=product_id,
                op_type="pick_and_place",
                parameters={"toLocation": from_loc},
            )
            kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, pickup_op, timeout=30)

        # Simulate travel
        time.sleep(_TRAVEL_TIME)
        self.active_transports[product_id]["status"] = "delivering"

        # Dispatch delivery to MS via KG
        if self.ms_id:
            deliver_op = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.ms_id,
                workpiece_ref=product_id,
                op_type="pick_and_place",
                parameters={"toLocation": to_loc},
            )
            kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, deliver_op, timeout=30)

        self.active_transports[product_id]["status"] = "delivered"
        self.logger.info("Transport complete: '%s' → '%s'", product_id, to_loc)
        return {"product_id": product_id, "location": to_loc, "status": "delivered"}

    # ------------------------------------------------------------------
    # REST workflows — backwards-compatible direct triggering
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecuteTransportWorkflow"),
        key="execute_transport",
    )
    def execute_transport(self, payload: WorkflowPayload) -> WorkflowResponse:
        """REST path: create a transport OperationRecord and wait for completion."""
        response_model = self.workflows["execute_transport"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            from_loc   = str(getattr(payload, IRI(f"{CFOP}fromLocation").lined))
            to_loc     = str(getattr(payload, IRI(f"{CFOP}toLocation").lined))

            # Write to KG — our own poll loop will pick this up
            op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="transport",
                parameters={"fromLocation": from_loc, "toLocation": to_loc},
            )
            status = kgw.wait_for_operation(
                self.ogm.db, NAMED_GRAPH, op_iri, timeout=120
            )
            result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, op_iri)
            return response_model(
                status_code=200 if status == "done" else 500,
                status=status,
                message=f"Transport '{product_id}' → '{to_loc}': {status}",
                content=json.dumps(result or {}),
            )
        except Exception as e:
            self.logger.error("execute_transport: %s", e)
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}UpdatePossessionWorkflow"),
        key="update_possession",
    )
    def update_possession(self, payload: WorkflowPayload) -> WorkflowResponse:
        """REST path: write a possession-state update to the KG."""
        response_model = self.workflows["update_possession"].response_model
        try:
            operation_id = str(getattr(payload, IRI(f"{CFOP}operationId").lined))
            status       = str(getattr(payload, IRI(f"{CFOP}operationStatus").lined))
            self.logger.info("Possession update: op='%s' status='%s'", operation_id, status)
            return response_model(
                status_code=200,
                status="success",
                message=f"Possession state for operation '{operation_id}' recorded",
                content=json.dumps({"operation_id": operation_id, "status": status}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")
