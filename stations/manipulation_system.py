"""ManipulationSystem (MS/C06) — KG-driven pick-and-place.

Polls the KG for pending 'pick_and_place' OperationRecords.
"""

import json
import time

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse
from station_base import KGPollingMixin
import kg_writer as kgw

CFOP = "https://w3id.org/circularfactory/Operations#"
NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances#StationInstances")

_OP_TIME = 1.0


class ManipulationSystem(KGPollingMixin, Service):
    """Manipulation System (MS/C06).

    Executes pick-and-place operations.  Discovered via the KG poll loop
    (dispatched by ILS or TC) and also accessible via REST.
    """

    NAMED_GRAPH = NAMED_GRAPH

    def __init__(self, ms_id: IRI, ogm: OGM, host: str = "0.0.0.0") -> None:
        self.operations_count = 0
        super().__init__(service_id=ms_id, ogm=ogm, host=host)

        @self.mw.app.get("/stats")
        def get_stats() -> dict:
            return {"operations_count": self.operations_count}

    def on_start(self) -> None:
        self.register_op_handler("pick_and_place", self._handle_pick_and_place)
        super().on_start()

    # ------------------------------------------------------------------
    # KG-driven handler
    # ------------------------------------------------------------------

    def _handle_pick_and_place(self, record: dict) -> dict:
        product_id  = record["workpiece_ref"]
        to_location = record["parameters"].get("toLocation", "unknown")

        self.logger.info("Pick-and-place: '%s' → '%s'", product_id, to_location)
        time.sleep(_OP_TIME)
        self.operations_count += 1
        self.logger.info("Pick-and-place complete: '%s' at '%s'", product_id, to_location)
        return {"product_id": product_id, "location": to_location}

    # ------------------------------------------------------------------
    # REST workflow — backwards-compatible direct triggering
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecutePickAndPlaceWorkflow"),
        key="execute_pick_and_place",
    )
    def execute_pick_and_place(self, payload: WorkflowPayload) -> WorkflowResponse:
        """REST path: write a pick_and_place OperationRecord and wait."""
        response_model = self.workflows["execute_pick_and_place"].response_model
        try:
            product_id  = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            to_location = str(getattr(payload, IRI(f"{CFOP}toLocation").lined))

            op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="pick_and_place",
                parameters={"toLocation": to_location},
            )
            status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, op_iri, timeout=30)
            result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, op_iri)
            return response_model(
                status_code=200 if status == "done" else 500,
                status=status,
                message=f"Pick-and-place '{product_id}' → '{to_location}': {status}",
                content=json.dumps(result or {}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")
