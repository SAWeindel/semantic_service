"""PlanHealer (PH/A02) — process alternative finder.

Exposes:
  receive_status_update — receive success/failure status from TC (Scenario 5).

Calls (remote):
  TC : receive_command  — send an alternative command after plan healing.

Plan healing logic: if TC reports a failure for a known process, the PH
substitutes an alternative from its internal replacement map and sends the
new Command back to TC.
"""

import json
import threading
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from semantic_service import Service, WorkflowPayload, WorkflowResponse

CFOP = "https://w3id.org/circularfactory/Operations#"

# Alternative process map: failed_process → replacement_process.
# Reflects Scenario 5: unscrewing fails → milling as fallback.
_ALTERNATIVES: dict[str, str] = {
    "unscrew": "mill",
    "mill": None,           # no further alternative
    "disassemble": None,
}


class PlanHealer(Service):
    """Plan Healer service (PH/A02).

    Monitors TC commands.  On failure, it finds an alternative process and
    reissues a new Command to the TC — transparent to the original requester.
    """

    NAMED_GRAPH = IRI("https://w3id.org/circularfactory/OperationsInstances")

    def __init__(
        self,
        ph_id: IRI,
        ogm: OGM,
        tc_id: Optional[IRI] = None,
        host: str = "0.0.0.0",
    ) -> None:
        self.tc_id = IRI(tc_id) if tc_id else None
        self._lock = threading.Lock()
        self.history: list = []

        super().__init__(service_id=ph_id, ogm=ogm, host=host)

        @self.mw.app.get("/history")
        def get_history() -> dict:
            with self._lock:
                return {"history": list(self.history)}

    # ------------------------------------------------------------------
    # Service hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        if self.tc_id is None:
            return
        try:
            self.add_remote_workflow(
                key="tc_receive_command",
                resource_instance=self.tc_id,
                workflow_class=IRI(f"{CFOP}ReceiveCommandWorkflow"),
                logger_suffix="TC-command",
            )
        except Exception as e:
            self.logger.warning(f"Could not discover TC command workflow: {e}")

    # ------------------------------------------------------------------
    # Exposed workflows
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{CFOP}ReceiveStatusUpdateWorkflow"),
        key="receive_status_update",
    )
    def receive_status_update(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Handle a StatusUpdate from TC.

        If status == 'failed', look up an alternative process and send a new
        Command back to TC.  If status == 'done', log success.
        """
        response_model = self.workflows["receive_status_update"].response_model
        try:
            product_id = str(getattr(payload, IRI(f"{CFOP}productId").lined))
            component_id = str(getattr(payload, IRI(f"{CFOP}componentId").lined))
            process_type = str(getattr(payload, IRI(f"{CFOP}processType").lined))
            status = str(getattr(payload, IRI(f"{CFOP}operationStatus").lined))

            entry = {
                "product_id": product_id,
                "component_id": component_id,
                "process_type": process_type,
                "status": status,
            }
            with self._lock:
                self.history.append(entry)

            if status == "done":
                self.logger.info(
                    f"Command '{process_type}' on '{component_id}' completed successfully"
                )
                return response_model(
                    status_code=200,
                    status="success",
                    message="Status acknowledged",
                    content=json.dumps(entry),
                )

            if status == "failed":
                self.logger.warning(
                    f"Command '{process_type}' on '{component_id}' failed — initiating plan healing"
                )
                alternative = _ALTERNATIVES.get(process_type)
                if alternative is None:
                    self.logger.error(
                        f"No alternative available for failed process '{process_type}'"
                    )
                    return response_model(
                        status_code=200,
                        status="no_alternative",
                        message=f"No alternative process for '{process_type}'",
                        content=json.dumps(entry),
                    )

                self.logger.info(
                    f"Plan healing: replacing '{process_type}' with '{alternative}'"
                )
                self._send_alternative_command(product_id, component_id, alternative)
                return response_model(
                    status_code=200,
                    status="healed",
                    message=(
                        f"Plan healed: '{process_type}' replaced by '{alternative}'"
                    ),
                    content=json.dumps({**entry, "alternative": alternative}),
                )

            # Unknown status
            self.logger.info(f"Unexpected status '{status}' — acknowledging")
            return response_model(
                status_code=200,
                status="success",
                message=f"Status '{status}' acknowledged",
                content=json.dumps(entry),
            )

        except Exception as e:
            self.logger.error(f"receive_status_update error: {e}")
            return response_model(
                status_code=500, status="error", message=str(e), content=""
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _send_alternative_command(
        self,
        product_id: str,
        component_id: str,
        alternative: str,
    ) -> None:
        """Issue a new Command to the TC with the alternative process."""
        if "tc_receive_command" not in self.remote_workflows:
            self.logger.warning("TC command workflow not available; cannot send alternative")
            return
        try:
            response = self.remote_workflows["tc_receive_command"](
                **{
                    IRI(f"{CFOP}productId").lined: product_id,
                    IRI(f"{CFOP}componentId").lined: component_id,
                    IRI(f"{CFOP}processType").lined: alternative,
                }
            )
            self.logger.info(
                f"Alternative command '{alternative}' sent to TC: {response.status}"
            )
        except Exception as e:
            self.logger.error(f"Failed to send alternative command to TC: {e}")
