"""InspectionSystem (IS/B01) — KG-driven measurement.

Polls the KG for:
  * 'suggest_strategy'  — returns available strategies in result JSON
  * 'inspect'           — executes the chosen strategy, writes a QualityRecord
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

_STRATEGY_CATALOGUE: dict[str, list[str]] = {
    "default":    ["CT_scan", "surface_scan", "hardness_test"],
    "bevel_gear": ["CT_scan", "surface_scan"],
    "housing":    ["surface_scan", "dimensional_check"],
}
_MEASUREMENT_TIME = 2.0


class InspectionSystem(KGPollingMixin, Service):
    """Inspection System (IS/B01).

    Reads pending strategy-suggestion and inspection OperationRecords from the
    KG.  Inspection results are persisted as QualityRecords.
    """

    NAMED_GRAPH = NAMED_GRAPH

    def __init__(
        self,
        is_id: IRI,
        ogm: OGM,
        pc_id: Optional[IRI] = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.pc_id = IRI(pc_id) if pc_id else None
        self.inspections: dict = {}
        super().__init__(service_id=is_id, ogm=ogm, host=host)

        @self.mw.app.get("/inspections")
        def get_inspections() -> dict:
            return {"inspections": dict(self.inspections)}

    def on_start(self) -> None:
        self.register_op_handler("suggest_strategy", self._handle_suggest_strategy)
        self.register_op_handler("inspect",          self._handle_inspect)
        self.register_op_handler("update_quality",   self._handle_update_quality)
        super().on_start()

    # ------------------------------------------------------------------
    # KG-driven handlers
    # ------------------------------------------------------------------

    def _handle_suggest_strategy(self, record: dict) -> dict:
        product_id = record["workpiece_ref"]
        strategies = _STRATEGY_CATALOGUE["default"]
        for keyword, strats in _STRATEGY_CATALOGUE.items():
            if keyword in product_id.lower():
                strategies = strats
                break
        self.logger.info("Strategies for '%s': %s", product_id, strategies)
        return {"product_id": product_id, "strategies": strategies}

    def _handle_inspect(self, record: dict) -> dict:
        product_id = record["workpiece_ref"]
        strategy   = record["parameters"].get("measurementStrategy", "CT_scan")

        self.logger.info("Inspecting '%s' with '%s'", product_id, strategy)
        time.sleep(_MEASUREMENT_TIME)

        quality = {
            "product_id": product_id,
            "strategy":   strategy,
            "result":     "OK",
            "score":      0.93,
            "defects":    [],
        }
        self.inspections[product_id] = quality

        # Persist quality record to KG
        qr_iri = kgw.create_quality_record(
            self.ogm, NAMED_GRAPH,
            workpiece_ref=product_id,
            quality_data=quality,
            strategy=strategy,
            quality_score=quality["score"],
        )
        self.logger.info("QualityRecord %s created for '%s'", qr_iri, product_id)
        return quality

    def _handle_update_quality(self, record: dict) -> dict:
        product_id   = record["workpiece_ref"]
        quality_data = record["parameters"].get("qualityData", "{}")
        try:
            data = json.loads(quality_data)
        except json.JSONDecodeError:
            data = {"raw": quality_data}

        kgw.create_quality_record(
            self.ogm, NAMED_GRAPH,
            workpiece_ref=product_id,
            quality_data=data,
            strategy="external",
            quality_score=float(data.get("score", 0.0)),
        )
        return {"product_id": product_id, "stored": True}

    # ------------------------------------------------------------------
    # REST workflows — backwards-compatible
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}SuggestMeasurementStrategyWorkflow"),
        key="suggest_measurement_strategy",
    )
    def suggest_measurement_strategy(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["suggest_measurement_strategy"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="suggest_strategy",
            )
            status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, op_iri, timeout=30)
            result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, op_iri)
            return response_model(
                status_code=200 if status == "done" else 500,
                status=status,
                message=f"Strategies for '{product_id}'",
                content=json.dumps(result or {}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}PerformInspectionWorkflow"),
        key="perform_inspection",
    )
    def perform_inspection(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["perform_inspection"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            strategy   = str(getattr(payload, IRI(f"{CFOP}measurementStrategy").lined))
            op_iri = kgw.create_operation_record(
                self.ogm, NAMED_GRAPH,
                station_id=self.service_id,
                workpiece_ref=product_id,
                op_type="inspect",
                parameters={"measurementStrategy": strategy},
            )
            status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, op_iri, timeout=60)
            result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, op_iri)
            return response_model(
                status_code=200 if status == "done" else 500,
                status=status,
                message=f"Inspection '{strategy}' for '{product_id}': {status}",
                content=json.dumps(result or {}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}UpdateComponentQualityWorkflow"),
        key="update_component_quality",
    )
    def update_component_quality(self, payload: WorkflowPayload) -> WorkflowResponse:
        response_model = self.workflows["update_component_quality"].response_model
        try:
            product_id   = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            quality_data = str(getattr(payload, IRI(f"{CFOP}qualityData").lined))
            kgw.create_quality_record(
                self.ogm, NAMED_GRAPH,
                workpiece_ref=product_id,
                quality_data=json.loads(quality_data) if quality_data.startswith("{") else {"raw": quality_data},
                strategy="push",
                quality_score=0.0,
            )
            return response_model(
                status_code=200, status="success",
                message=f"Quality stored for '{product_id}'",
                content=json.dumps({"product_id": product_id}),
            )
        except Exception as e:
            return response_model(status_code=500, status="error", message=str(e), content="")
