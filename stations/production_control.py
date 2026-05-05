"""ProductionControl (PC/A01) — central orchestrator.

Decentralized design
--------------------
PC writes OperationRecords to the KG and lets each station's poll loop pick
them up.  It monitors completion by watching the KG rather than receiving
direct REST callbacks.  The REST workflows it exposes (receive_quality_update,
receive_operation_complete) remain available for backwards-compatible push
notifications, but they now also write their payloads to the KG.
"""

import json
import threading
import time
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse
import kg_writer as kgw

CFOP = "https://w3id.org/circularfactory/Operations#"
NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances#StationInstances")


class ProductionControl(Service):
    """Production Control (PC/A01).

    Dispatches operations by writing OperationRecords to the KG.
    Stations pick these up via their own poll loops.
    """

    NAMED_GRAPH = NAMED_GRAPH

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
        self.tc_id  = IRI(tc_id)  if tc_id  else None
        self.is_id  = IRI(is_id)  if is_id  else None
        self.ms_id  = IRI(ms_id)  if ms_id  else None
        self._lock = threading.Lock()

        super().__init__(service_id=pc_id, ogm=ogm, host=host)

        @self.mw.app.get("/operations")
        def get_operations() -> dict:
            """Live view of all operation records from the KG."""
            try:
                sparql = f"""
                    SELECT ?op ?status ?opType ?workpiece WHERE {{
                        GRAPH <{NAMED_GRAPH}> {{
                            ?op a <{kgw.OP_RECORD_CLASS}> .
                            ?op <{kgw.P_REC_STATUS}> ?status .
                            OPTIONAL {{ ?op <{kgw.P_REC_OP_TYPE}> ?opType . }}
                            OPTIONAL {{ ?op <{kgw.P_REC_WORKPIECE}> ?workpiece . }}
                        }}
                    }}
                """
                res = self.ogm.db.query(sparql, convert_bindings=True)
                rows = res.get("results", {}).get("bindings", [])
                return {
                    "operations": [
                        {
                            "op": str(r["op"]),
                            "status": str(r.get("status", "")),
                            "op_type": str(r.get("opType", "")),
                            "workpiece": str(r.get("workpiece", "")),
                        }
                        for r in rows
                    ]
                }
            except Exception as e:
                return {"error": str(e)}

    # ------------------------------------------------------------------
    # Service hooks — no remote workflow discovery needed for KG-based PC
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        self.logger.info("ProductionControl started — using KG-based operation dispatch")

    # ------------------------------------------------------------------
    # Scenario orchestration helpers
    # ------------------------------------------------------------------

    def initiate_disassembly(
        self, product_id: str, pickup_location: str, tc_location: str
    ) -> IRI:
        """Scenario 1: dispatch transport + disassembly via KG."""
        self.logger.info("Initiating disassembly for '%s'", product_id)
        transport_op = kgw.create_operation_record(
            self.ogm,
            NAMED_GRAPH,
            station_id=self.ils_id,
            workpiece_ref=product_id,
            op_type="transport",
            parameters={"fromLocation": pickup_location, "toLocation": tc_location},
        )
        # Wait for transport before dispatching disassembly
        status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, transport_op, timeout=120)
        if status != "done":
            self.logger.error("Transport failed for '%s' (status=%s)", product_id, status)
            return transport_op

        disassembly_op = kgw.create_operation_record(
            self.ogm,
            NAMED_GRAPH,
            station_id=self.tc_id,
            workpiece_ref=product_id,
            op_type="disassemble",
        )
        self.logger.info("Disassembly op %s dispatched for '%s'", disassembly_op, product_id)
        return disassembly_op

    def initiate_reprocessing(
        self,
        product_id: str,
        from_location: str,
        tc_location: str,
        process_type: str,
    ) -> IRI:
        """Scenario 3: dispatch transport + reprocessing via KG."""
        self.logger.info("Initiating reprocessing of '%s' via '%s'", product_id, process_type)
        transport_op = kgw.create_operation_record(
            self.ogm,
            NAMED_GRAPH,
            station_id=self.ils_id,
            workpiece_ref=product_id,
            op_type="transport",
            parameters={"fromLocation": from_location, "toLocation": tc_location},
        )
        status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, transport_op, timeout=120)
        if status != "done":
            self.logger.error("Transport failed for '%s'", product_id)
            return transport_op

        reprocess_op = kgw.create_operation_record(
            self.ogm,
            NAMED_GRAPH,
            station_id=self.tc_id,
            workpiece_ref=product_id,
            op_type="reprocess",
            parameters={"processType": process_type},
        )
        self.logger.info("Reprocessing op %s dispatched for '%s'", reprocess_op, product_id)
        return reprocess_op

    def initiate_inspection(
        self, product_id: str, transport_location: str
    ) -> Optional[str]:
        """Scenario 2: ask IS for strategies, choose one, dispatch inspection."""
        self.logger.info("Initiating inspection for '%s'", product_id)

        # Ask IS for strategies via KG
        strategy_op = kgw.create_operation_record(
            self.ogm,
            NAMED_GRAPH,
            station_id=self.is_id,
            workpiece_ref=product_id,
            op_type="suggest_strategy",
        )
        status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, strategy_op, timeout=30)
        if status != "done":
            self.logger.error("Strategy suggestion failed for '%s'", product_id)
            return None

        result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, strategy_op)
        strategies = (result or {}).get("strategies", [])
        if not strategies:
            self.logger.error("No strategies returned for '%s'", product_id)
            return None

        chosen = strategies[0]
        self.logger.info("Chosen strategy for '%s': %s", product_id, chosen)

        # Transport to measurement station
        transport_op = kgw.create_operation_record(
            self.ogm,
            NAMED_GRAPH,
            station_id=self.ils_id,
            workpiece_ref=product_id,
            op_type="transport",
            parameters={"fromLocation": "storage", "toLocation": transport_location},
        )
        kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, transport_op, timeout=120)

        # Dispatch inspection
        inspect_op = kgw.create_operation_record(
            self.ogm,
            NAMED_GRAPH,
            station_id=self.is_id,
            workpiece_ref=product_id,
            op_type="inspect",
            parameters={"measurementStrategy": chosen},
        )
        status = kgw.wait_for_operation(self.ogm.db, NAMED_GRAPH, inspect_op, timeout=60)
        result = kgw.get_operation_result(self.ogm.db, NAMED_GRAPH, inspect_op)
        self.logger.info("Inspection done for '%s': %s", product_id, result)
        return json.dumps(result) if result else None

    def initiate_command(
        self, product_id: str, component_id: str, process_type: str
    ) -> IRI:
        """Scenario 5: dispatch a Command to TC via KG."""
        self.logger.info(
            "Dispatching command: product=%s component=%s process=%s",
            product_id, component_id, process_type,
        )
        op = kgw.create_operation_record(
            self.ogm,
            NAMED_GRAPH,
            station_id=self.tc_id,
            workpiece_ref=product_id,
            op_type="command",
            parameters={"componentId": component_id, "processType": process_type},
        )
        return op

    # ------------------------------------------------------------------
    # Exposed REST workflows — kept for push-based notifications
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReceiveQualityUpdateWorkflow"),
        key="receive_quality_update",
    )
    def receive_quality_update(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Push path: IS or TC sends quality results directly to PC."""
        response_model = self.workflows["receive_quality_update"].response_model
        try:
            product_id   = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            quality_data = str(getattr(payload, IRI(f"{CFOP}qualityData").lined))
            self.logger.info("Quality push received for '%s'", product_id)
            # Persist to KG
            kgw.create_quality_record(
                self.ogm,
                NAMED_GRAPH,
                workpiece_ref=product_id,
                quality_data=json.loads(quality_data) if quality_data.startswith("{") else {"raw": quality_data},
                strategy="push",
                quality_score=0.0,
            )
            return response_model(
                status_code=200,
                status="success",
                message=f"Quality data for '{product_id}' written to KG",
                content=json.dumps({"product_id": product_id}),
            )
        except Exception as e:
            self.logger.error("receive_quality_update: %s", e)
            return response_model(status_code=500, status="error", message=str(e), content="")

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReceiveOperationCompleteWorkflow"),
        key="receive_operation_complete",
    )
    def receive_operation_complete(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Push path: TC or ILS signals that a station-level operation finished."""
        response_model = self.workflows["receive_operation_complete"].response_model
        try:
            operation_id = str(getattr(payload, IRI(f"{CFOP}operationId").lined))
            status       = str(getattr(payload, IRI(f"{CFOP}operationStatus").lined))
            self.logger.info("Operation '%s' completed with status '%s'", operation_id, status)
            return response_model(
                status_code=200,
                status="success",
                message=f"Operation '{operation_id}' acknowledged",
                content=json.dumps({"operation_id": operation_id, "status": status}),
            )
        except Exception as e:
            self.logger.error("receive_operation_complete: %s", e)
            return response_model(status_code=500, status="error", message=str(e), content="")
