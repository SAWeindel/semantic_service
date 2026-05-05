"""Consumer — a Service that periodically requests new recordings and reads them.

Workflow
--------
consume_record : (recordId: IRI) -> void
    Fetches the hw:Record instance from GraphDB via OGM and prints its fields.

Poll loop (started in on_start):
    Every poll_interval seconds the Consumer calls the Recorder's create_record
    workflow, passing its own service IRI and a random description string.
    The Recorder will asynchronously create the record and call back with
    consume_record once it is ready.
"""

import json
import random
import threading

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM
from kapps_ogm.utils.class_scope import ClassScope

from semantic_service import Service, WorkflowPayload, WorkflowResponse

HW = "http://demo.org/Handover#"

_DESCRIPTIONS = [
    "sensor reading alpha",
    "batch result beta",
    "diagnostic snapshot gamma",
    "telemetry burst delta",
    "status report epsilon",
    "calibration check zeta",
    "heartbeat eta",
]

# Fixed ClassScope for hw:Record — only the three data properties
_RECORD_SCOPE = ClassScope.from_property_chains(
    [
        [IRI("hasDescription", HW)],
        [IRI("hasRecordDate", HW)],
        [IRI("hasData", HW)],
    ]
)


class Consumer(Service):
    """Requests recordings from a Recorder and reads each record from GraphDB."""

    NAMED_GRAPH = IRI("http://demo.org/HandoverInstances")

    def __init__(
        self,
        consumer_id: IRI,
        ogm: OGM,
        recorder_id: IRI,
        poll_interval: float = 5.0,
        host: str = "0.0.0.0",
    ) -> None:
        self.recorder_id = IRI(recorder_id)
        self.poll_interval = poll_interval
        self._stop_polling = threading.Event()

        super().__init__(service_id=consumer_id, ogm=ogm, host=host)

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """Discover the Recorder's create_record workflow and start the poll loop."""
        self.add_remote_workflow(
            key="create_record",
            resource_instance=self.recorder_id,
            workflow_class=IRI("CreateRecordWorkflow", HW),
        )
        self.logger.info("Remote create_record workflow discovered")

        threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name=f"Consumer-{self.service_id.fragment}-poll",
        ).start()

    def on_stop(self) -> None:
        self._stop_polling.set()

    # ------------------------------------------------------------------
    # Workflow
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{HW}ConsumeRecordWorkflow"), key="consume_record"
    )
    def consume_record_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Fetch a hw:Record from GraphDB by IRI and print its contents."""
        response_model = self.workflows["consume_record"].response_model

        record_id = IRI(str(getattr(payload, IRI("recordId", HW).lined)))

        try:
            record_node = self.ogm.fetch(
                instance_iri=record_id,
                class_scope=_RECORD_SCOPE,
                materialize=True,
            )
            data = record_node.instance.model_dump()

            description = (data.get(IRI("hasDescription", HW).lined) or ["?"])[0]
            date = (data.get(IRI("hasRecordDate", HW).lined) or ["?"])[0]
            value = (data.get(IRI("hasData", HW).lined) or [float("nan")])[0]

            self.logger.info(
                f"\n{'─'*60}\n"
                f"  [RECORD RECEIVED]\n"
                f"  id          : {record_id.fragment}\n"
                f"  description : {description}\n"
                f"  date        : {date}\n"
                f"  data        : {value:.6f}\n"
                f"{'─'*60}"
            )

            return response_model(
                status_code=200,
                status="success",
                message=f"Consumed record {record_id.fragment}",
                content=json.dumps(
                    {
                        "description": str(description),
                        "date": str(date),
                        "data": value,
                    }
                ),
            )
        except Exception as e:
            self.logger.error(f"Failed to consume record {record_id}: {e}")
            return response_model(
                status_code=500,
                status="error",
                message=str(e),
            )

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        while not self._stop_polling.is_set():
            try:
                self._request_recording()
            except Exception as e:
                self.logger.error(f"Poll round error: {e}")
            self._stop_polling.wait(self.poll_interval)

    def _request_recording(self) -> None:
        """Call the Recorder's create_record workflow with a random description."""
        description = random.choice(_DESCRIPTIONS)
        response = self.remote_workflows["create_record"](
            **{
                IRI("consumerServiceId", HW).lined: self.service_id,
                IRI("description", HW).lined: description,
            }
        )
        if response.is_success:
            self.logger.info(
                f"Recording requested: '{description}' → {response.status}"
            )
        else:
            self.logger.warning(
                f"create_record failed: {response.status_code} {response.status}"
            )
