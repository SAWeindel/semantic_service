"""station_base.py — KG-polling mixin for all station services.

StationBase extends Service with:
  * a background thread that polls the KG for pending OperationRecords
  * a dispatch table (op_type → handler) stations register their handlers into
  * automatic status transitions: pending→running on claim, running→done/failed after handler

Stations subclass both StationBase and Service (in that order).
They register handlers in __init__ via ``self.register_op_handler(op_type, fn)``
where fn(op_record: dict) -> dict | None (result to write).
"""

from __future__ import annotations

import threading
import time
import logging
from typing import Callable, Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

import kg_writer as kgw

_POLL_INTERVAL = 3.0  # seconds between KG polls per station

logger = logging.getLogger("station_base")


class KGPollingMixin:
    """Mixin that adds a background KG-polling loop to a Service subclass.

    Must be mixed in before Service in the MRO so that on_start/on_stop
    are called by Service.__init__'s hook mechanism.

    Usage in a station:
        class MyStation(KGPollingMixin, Service):
            NAMED_GRAPH = ...

            def __init__(self, ...):
                self._op_handlers: dict[str, Callable] = {}
                # register BEFORE super().__init__ so on_start sees them
                super().__init__(...)

            def on_start(self):
                super().on_start()
                self.register_op_handler("my_op", self._handle_my_op)

            def _handle_my_op(self, record: dict) -> Optional[dict]:
                ...
                return {"result": "ok"}
    """

    # Subclass must set this (mirrors Service.NAMED_GRAPH)
    NAMED_GRAPH: IRI

    def __init__(self, *args, **kwargs):
        self._op_handlers: dict[str, Callable[[dict], Optional[dict]]] = {}
        self._stop_poll = threading.Event()
        self._poll_thread: Optional[threading.Thread] = None
        super().__init__(*args, **kwargs)

    def register_op_handler(
        self, op_type: str, handler: Callable[[dict], Optional[dict]]
    ) -> None:
        """Register a handler for a given operation type string."""
        self._op_handlers[op_type] = handler

    # ------------------------------------------------------------------
    # Service hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """Start the KG-polling background thread."""
        super_on_start = getattr(super(), "on_start", None)
        if super_on_start:
            super_on_start()
        self._stop_poll.clear()
        self._poll_thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name=f"{self.__class__.__name__}-kg-poll",
        )
        self._poll_thread.start()
        self.logger.info(
            "%s KG-polling started (interval=%.1fs)",
            self.__class__.__name__,
            _POLL_INTERVAL,
        )

    def on_stop(self) -> None:
        """Stop the polling thread."""
        self._stop_poll.set()
        super_on_stop = getattr(super(), "on_stop", None)
        if super_on_stop:
            super_on_stop()

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        while not self._stop_poll.is_set():
            try:
                self._claim_and_execute_pending()
            except Exception as e:
                self.logger.error("KG poll error: %s", e)
            self._stop_poll.wait(_POLL_INTERVAL)

    def _claim_and_execute_pending(self) -> None:
        """Find all pending OperationRecords for this station and execute them."""
        pending = kgw.query_pending_operations(
            self.ogm.db,
            self.NAMED_GRAPH,
            station_id=self.service_id,
        )
        for record in pending:
            op_iri   = record["op_iri"]
            op_type  = record["op_type"]
            handler  = self._op_handlers.get(op_type)
            if handler is None:
                self.logger.warning(
                    "No handler for op_type='%s' (op=%s) — skipping", op_type, op_iri
                )
                continue

            # Claim the operation: pending → running (if another station beat us, skip)
            try:
                kgw.set_operation_running(self.ogm.db, self.NAMED_GRAPH, op_iri)
            except Exception:
                # Another instance already claimed it
                continue

            self.logger.info(
                "Claimed operation %s  type='%s'  workpiece='%s'",
                op_iri, op_type, record["workpiece_ref"],
            )

            try:
                result = handler(record)
                kgw.set_operation_done(
                    self.ogm.db, self.NAMED_GRAPH, op_iri, result=result
                )
            except Exception as e:
                self.logger.error(
                    "Handler for op_type='%s' raised: %s", op_type, e
                )
                kgw.set_operation_failed(
                    self.ogm.db, self.NAMED_GRAPH, op_iri, reason=str(e)
                )
