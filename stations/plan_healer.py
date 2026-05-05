"""PlanHealer (PH/A02) — KG-driven plan healing.

Polls the KG for pending 'status_update' OperationRecords dispatched by TC.
On failure, finds an alternative process and writes a new 'command'
OperationRecord back to TC.  No direct REST calls between PH and TC.
"""

import json
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse
from station_base import KGPollingMixin
import kg_writer as kgw

CFOP = "https://w3id.org/circularfactory/Operations#"
NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances#StationInstances")

_ALTERNATIVES: dict[str, Optional[str]] = {
    "unscrew": "mill",
    "mill":    None,
}


class PlanHealer(KGPollingMixin, Service):
    """Plan Healer (PH/A02).

    Reads status_update OperationRecords written by TC.
    On failure, writes a new command OperationRecord back to TC.
    """

    NAMED_GRAPH = NAMED_GRAPH

    def __init__(
        self,
        ph_id: IRI,
        ogm: OGM,
        tc_id: Optional[IRI] = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.tc_id = IRI(tc_id) if tc_id else None
        self.history: list = []
        super().__init__(service_id=ph_id, ogm=ogm, host=host)

        @self.mw.app.get("/history")
        def get_history() -> dict:
            return {"history": list(self.history)}

    def on_start(self) -> None:
        self.register_op_handler("status_update", self._handle_status_update)
        super().on_start()

    # ------------------------------------------------------------------
    # KG-driven handler
    # ------------------------------------------------------------------

    def _handle_status_update(self, record: dict) -> dict:
        product_id   = record["workpiece_ref"]
        component_id = record["parameters"].get("componentId", "")
        process_type = record["parameters"].get("processType", "")
        status       = record["parameters"].get("status", "")

        entry = {
            "product_id":   product_id,
            "component_id": component_id,
            "process_type": process_type,
            "status":       status,
        }
        self.history.append(entry)

        if status == "done":
            self.logger.info(
                "Command '%s' on '%s' completed successfully", process_type, component_id
            )
            return {**entry, "action": "none"}

        if status == "failed":
            self.logger.warning(
                "Command '%s' on '%s' failed — initiating plan healing",
                process_type, component_id,
            )
            alternative = _ALTERNATIVES.get(process_type)
            if alternative is None:
                self.logger.error(
                    "No alternative for failed process '%s'", process_type
                )
                return {**entry, "action": "no_alternative"}

            self.logger.info(
                "Plan healing: '%s' → '%s'", process_type, alternative
            )
            if self.tc_id:
                kgw.create_operation_record(
                    self.ogm, NAMED_GRAPH,
                    station_id=self.tc_id,
                    workpiece_ref=product_id,
                    op_type="command",
                    parameters={"componentId": component_id, "processType": alternative},
                )
                self.logger.info(
                    "Alternative command '%s' written to KG for TC", alternative
                )
            return {**entry, "action": "healed", "alternative": alternative}

        self.logger.info("Unexpected status '%s' — acknowledged", status)
        return {**entry, "action": "acknowledged"}

    # ------------------------------------------------------------------
    # REST workflow — kept for push-based status delivery
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReceiveStatusUpdateWorkflow"),
        key="receive_status_update",
    )
    def receive_status_update(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Push path: TC sends a StatusUpdate directly via REST."""
        response_model = self.workflows["receive_status_update"].response_model
        try:
            product_id   = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            component_id = str(getattr(payload, IRI(f"{CFOP}componentId").lined))
            process_type = str(getattr(payload, IRI(f"{CFOP}processType").lined))
            status       = str(getattr(payload, IRI(f"{CFOP}operationStatus").lined))

            # Write as a status_update OperationRecord so the KG poll handles it
            kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="status_update",
                parameters={
                    "componentId": component_id,
                    "processType": process_type,
                    "status":      status,
                },
            )
            return response_model(
                status_code=200,
                status="success",
                message="Status update queued for plan healing",
                content=json.dumps({
                    "product_id": product_id,
                    "process_type": process_type,
                    "status": status,
                }),
            )
        except Exception as e:
            self.logger.error("receive_status_update: %s", e)
            return response_model(status_code=500, status="error", message=str(e), content="")
