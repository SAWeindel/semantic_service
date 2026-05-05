"""Recorder — a Service that creates hw:Record instances in GraphDB on request.

Workflow
--------
create_record : (consumerServiceId: IRI, description: str) -> void (202 Accepted)
    Returns immediately and spawns a background thread that:
      1. Sleeps 1 s to simulate processing time.
      2. Creates a hw:Record via OGM (description + random float + timestamp).
      3. Discovers the Consumer's consume_record workflow from GraphDB and calls it
         with the new record IRI.
"""

import copy
import json
import random
import threading
import time
from datetime import datetime, timezone

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM
from kapps_ogm.node.core import Node
from kapps_ogm.utils.class_scope import ClassScope

from semantic_service import Service, WorkflowPayload, WorkflowResponse

HW = "http://demo.org/Handover#"


class Recorder(Service):
    """Creates data records in GraphDB and notifies the requesting Consumer."""

    NAMED_GRAPH = IRI("http://demo.org/HandoverInstances")

    def __init__(self, recorder_id: IRI, ogm: OGM, host: str = "0.0.0.0") -> None:
        self._record_count: int = 0
        self._count_lock = threading.Lock()
        # Cache of per-consumer remote consume_record proxies, keyed by consumer IRI string
        self._consumer_proxies: dict[str, object] = {}
        self._proxy_lock = threading.Lock()

        super().__init__(service_id=recorder_id, ogm=ogm, host=host)

    # ------------------------------------------------------------------
    # Workflow
    # ------------------------------------------------------------------

    @Service.workflow(
        workflow_class=IRI(f"{HW}CreateRecordWorkflow"), key="create_record"
    )
    def create_record_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Accept immediately; delegate record creation to a background thread."""
        response_model = self.workflows["create_record"].response_model

        consumer_service_id = IRI(
            str(getattr(payload, IRI("consumerServiceId", HW).lined))
        )
        description = str(getattr(payload, IRI("description", HW).lined))

        threading.Thread(
            target=self._record_and_notify,
            args=(consumer_service_id, description),
            daemon=True,
            name=f"Recorder-{self.service_id.fragment}-record",
        ).start()

        return response_model(
            status_code=202,
            status="accepted",
            message=f"Recording task started for '{description}'",
        )

    # ------------------------------------------------------------------
    # Background task
    # ------------------------------------------------------------------

    def _record_and_notify(self, consumer_id: IRI, description: str) -> None:
        self.logger.info(
            f"Recording task started: '{description}' → notify {consumer_id.fragment}"
        )
        time.sleep(1)

        # Build the Record data dict using lined IRI keys (OGM convention)
        record_data = {
            IRI("hasDescription", HW).lined: [description],
            IRI("hasRecordDate", HW).lined: [datetime.now(timezone.utc).isoformat()],
            IRI("hasData", HW).lined: [random.random()],
        }

        # Derive ClassScope from the data structure
        temp_node = Node(data=copy.deepcopy(record_data), ogm=self.ogm)
        class_scope = ClassScope.from_node_data(temp_node)

        # Persist record via OGM
        record_node = self.ogm.create(
            class_iri=IRI("Record", HW),
            data=record_data,
            class_scope=class_scope,
            named_graph=self.named_graph,
            persist=True,
        )

        with self._count_lock:
            self._record_count += 1

        self.logger.info(f"Record persisted: {record_node.id}")

        # Discover (or reuse cached) consume_record proxy for this Consumer
        try:
            consume_wf = self._get_consume_proxy(consumer_id)
            response = consume_wf(
                **{
                    IRI("recordId", HW).lined: record_node.id,
                }
            )
            self.logger.info(
                f"Consumer notified: {response.status_code} {response.status}"
            )
        except Exception as e:
            self.logger.error(f"Failed to notify consumer {consumer_id.fragment}: {e}")

    def _get_consume_proxy(self, consumer_id: IRI):
        """Return a cached remote consume_record proxy, discovering it on first access."""
        key = str(consumer_id)
        with self._proxy_lock:
            if key not in self._consumer_proxies:
                proxy = self.add_remote_workflow(
                    key=f"consume_{consumer_id.fragment}",
                    resource_instance=consumer_id,
                    workflow_class=IRI("ConsumeRecordWorkflow", HW),
                )
                self._consumer_proxies[key] = proxy
            return self._consumer_proxies[key]
