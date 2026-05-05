"""InspectionSystem (IS/B01) — measurement and quality assessment.

Exposes:
  suggest_measurement_strategy — return candidate strategies for a product (Scenario 2).
  perform_inspection           — execute the chosen strategy and return quality data.
  update_component_quality     — persist quality results to the knowledge graph.

Calls (remote):
  PC : receive_quality_update — push quality results upstream after inspection.
"""

import json
import time
import threading
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse

CFOP = "https://w3id.org/circularfactory/Operations#"

# Available measurement strategies (mock catalogue).
_STRATEGY_CATALOGUE: dict[str, list[str]] = {
    "default": ["CT_scan", "surface_scan", "hardness_test"],
    "bevel_gear": ["CT_scan", "surface_scan"],
    "housing": ["surface_scan", "dimensional_check"],
}

# Simulated measurement duration in seconds.
_MEASUREMENT_TIME_SECONDS = 2.0


class InspectionSystem(Service):
    """Inspection System service (IS/B01).

    Selects and executes measurement strategies.  Quality results are stored
    in the knowledge graph and pushed to the Production Control.
    """

    NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances")

    def __init__(
        self,
        is_id: IRI,
        ogm: OGM,
        pc_id: Optional[IRI] = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.pc_id = IRI(pc_id) if pc_id else None
        self._lock = threading.Lock()
        self.inspections: dict = {}

        super().__init__(service_id=is_id, ogm=ogm, host=host)

        @self.mw.app.get("/inspections")
        def get_inspections() -> dict:
            with self._lock:
                return {"inspections": self.inspections.copy()}

    # ------------------------------------------------------------------
    # Service hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        if self.pc_id is None:
            return
        try:
            self.add_remote_workflow(
                key="pc_quality_update",
                resource_instance=self.pc_id,
                workflow_class=IRI(f"{CFOP}ReceiveQualityUpdateWorkflow"),
                logger_suffix="PC-quality",
            )
        except Exception as e:
            self.logger.warning(f"Could not discover PC quality-update workflow: {e}")

    # ------------------------------------------------------------------
    # Exposed workflows
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}SuggestMeasurementStrategyWorkflow"),
        key="suggest_measurement_strategy",
    )
    def suggest_measurement_strategy(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Return a JSON list of suitable measurement strategies for the product.

        Scenario 2: PC calls this before creating an inspection operation.
        """
        response_model = self.workflows["suggest_measurement_strategy"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            self.logger.info(f"Strategy suggestion requested for '{product_id}'")

            # Select catalogue entry by keyword in product_id, else use default.
            strategies = _STRATEGY_CATALOGUE["default"]
            for keyword, strats in _STRATEGY_CATALOGUE.items():
                if keyword in product_id.lower():
                    strategies = strats
                    break

            self.logger.info(f"Returning strategies for '{product_id}': {strategies}")
            return response_model(
                status_code=200,
                status="success",
                message=f"Found {len(strategies)} strategies for '{product_id}'",
                content=json.dumps({"product_id": product_id, "strategies": strategies}),
            )
        except Exception as e:
            self.logger.error(f"suggest_measurement_strategy error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}PerformInspectionWorkflow"),
        key="perform_inspection",
    )
    def perform_inspection(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Execute the chosen measurement strategy on the product.

        Simulates the measurement, stores results internally, and notifies PC.
        """
        response_model = self.workflows["perform_inspection"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            strategy = str(getattr(payload, IRI(f"{CFOP}measurementStrategy").lined))

            self.logger.info(
                f"Performing '{strategy}' inspection on '{product_id}'"
            )
            # Simulate measurement time
            time.sleep(_MEASUREMENT_TIME_SECONDS)

            quality_data = json.dumps({
                "product_id": product_id,
                "strategy": strategy,
                "result": "OK",
                "score": 0.93,
                "defects": [],
            })
            with self._lock:
                self.inspections[product_id] = {
                    "strategy": strategy,
                    "quality_data": quality_data,
                }

            # Push quality result upstream to PC
            self._push_quality_to_pc(product_id, quality_data)

            return response_model(
                status_code=200,
                status="success",
                message=f"Inspection '{strategy}' completed for '{product_id}'",
                content=quality_data,
            )
        except Exception as e:
            self.logger.error(f"perform_inspection error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}UpdateComponentQualityWorkflow"),
        key="update_component_quality",
    )
    def update_component_quality(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Persist externally provided quality data to the knowledge graph."""
        response_model = self.workflows["update_component_quality"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            quality_data = str(getattr(payload, IRI(f"{CFOP}qualityData").lined))
            self.logger.info(f"Storing quality data for '{product_id}'")
            with self._lock:
                self.inspections[product_id] = {"quality_data": quality_data}
            return response_model(
                status_code=200,
                status="success",
                message=f"Quality data stored for '{product_id}'",
                content=json.dumps({"product_id": product_id}),
            )
        except Exception as e:
            self.logger.error(f"update_component_quality error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
            self.logger.info(f"Quality data for '{product_id}' pushed to PC")
        except Exception as e:
            self.logger.warning(f"Could not push quality data to PC: {e}")
