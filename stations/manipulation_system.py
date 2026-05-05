"""ManipulationSystem (MS/C06) — pick-and-place operations.

Exposes:
  execute_pick_and_place — pick up a product and place it at the target location.

The MS may be a standalone module used by the ILS or integrated into a station
(e.g. a TC with its own robotic arm).  Either way it exposes the same workflow.
"""

import json
import time

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse

CFOP = "https://w3id.org/circularfactory/Operations#"

# Simulated pick-and-place duration in seconds.
_OPERATION_TIME_SECONDS = 1.0


class ManipulationSystem(Service):
    """Manipulation System service (MS/C06).

    Executes all manipulation (pick-and-place) operations, either as part of
    the ILS mobile robot or as a fixed robotic arm inside a station.
    """

    NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances")

    def __init__(
        self,
        ms_id: IRI,
        ogm: OGM,
        host: str = "0.0.0.0",
    ) -> None:
        self.operations_count: int = 0
        super().__init__(service_id=ms_id, ogm=ogm, host=host)

        @self.mw.app.get("/stats")
        def get_stats() -> dict:
            return {"operations_count": self.operations_count}

    # ------------------------------------------------------------------
    # Exposed workflows
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ExecutePickAndPlaceWorkflow"),
        key="execute_pick_and_place",
    )
    def execute_pick_and_place(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Pick up a product and place it at the specified target location.

        Simulates gripper movement: approach → grasp → transport → release.
        """
        response_model = self.workflows["execute_pick_and_place"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            to_location = str(getattr(payload, IRI(f"{CFOP}toLocation").lined))

            self.logger.info(
                f"Pick-and-place: moving '{product_id}' to '{to_location}'"
            )

            # Simulate gripper operation
            time.sleep(_OPERATION_TIME_SECONDS)
            self.operations_count += 1

            self.logger.info(
                f"Pick-and-place complete: '{product_id}' placed at '{to_location}'"
            )
            return response_model(
                status_code=200,
                status="success",
                message=f"Product '{product_id}' placed at '{to_location}'",
                content=json.dumps(
                    {"product_id": product_id, "location": to_location}
                ),
            )
        except Exception as e:
            self.logger.error(f"execute_pick_and_place error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )
