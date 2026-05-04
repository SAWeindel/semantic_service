"""User — a Service that polls a Calculator's workflows on a regular timer.

On every poll tick the User picks a random arithmetic workflow, calls it on the
Calculator, and logs the result.  It tracks its own invocation count in
``self.number_of_calculations``, exposed at GET /stats.

Remote workflows are discovered from the knowledge graph in ``on_start()`` via
:meth:`Service.add_remote_workflow`.  If discovery fails at startup the error is
logged and the poll loop will retry on the first tick.
"""

import json
import random
import threading
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from Service.Workflow import WorkflowPayload, WorkflowResponse
from Service.Service import Service

CALC = "http://demo.org/Calculator#"

# Workflow keys the User knows about
_WORKFLOW_KEYS = ["add", "subtract", "random", "void", "repeat"]

# Corresponding OWL class IRIs
_WORKFLOW_CLASSES = {
    "add": IRI(f"{CALC}AddWorkflow"),
    "subtract": IRI(f"{CALC}SubtractWorkflow"),
    "random": IRI(f"{CALC}RandomWorkflow"),
    "void": IRI(f"{CALC}VoidWorkflow"),
    "repeat": IRI(f"{CALC}RepeatWorkflow"),
}


class User(Service):
    """User service that periodically invokes a remote Calculator."""

    NAMED_GRAPH = IRI("http://demo.org/CalculatorInstances")

    def __init__(
        self,
        user_id: IRI,
        ogm: OGM,
        calculator_id: IRI,
        poll_interval: float = 5.0,
        host: str = "0.0.0.0",
    ) -> None:
        # Domain state must be set before super().__init__() calls on_start hooks.
        self.calculator_id = IRI(calculator_id)
        self.poll_interval = poll_interval
        self.number_of_calculations: int = 0
        self._calc_lock = threading.Lock()
        self._stop_polling = threading.Event()

        super().__init__(service_id=user_id, ogm=ogm, host=host)

        # Expose live counter via REST: GET /stats
        service_ref = self

        @self.mw.app.get("/stats")
        def get_stats() -> dict:
            return {"number_of_calculations": service_ref.number_of_calculations}

    # ------------------------------------------------------------------
    # Service hooks
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """Discover remote Calculator workflows and begin polling."""
        try:
            for key, wf_class in _WORKFLOW_CLASSES.items():
                self.add_remote_workflow(
                    key=key,
                    resource_instance=self.calculator_id,
                    workflow_class=wf_class,
                    logger_suffix=f"Calc-{key}",
                )
            self.logger.info("Remote workflows discovered successfully")
        except Exception as e:
            self.logger.warning(
                f"Remote workflow discovery failed at startup: {e}. "
                "Will retry on first poll tick."
            )

        poll_thread = threading.Thread(
            target=self._poll_loop,
            daemon=True,
            name=f"User-{self.service_id.fragment}-poll",
        )
        poll_thread.start()

    def on_stop(self) -> None:
        """Signal the poll loop to stop."""
        self._stop_polling.set()

    # ------------------------------------------------------------------
    # Poll loop
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        while not self._stop_polling.is_set():
            try:
                self._do_one_round()
            except Exception as e:
                self.logger.error(f"Poll round raised an unhandled exception: {e}")
            self._stop_polling.wait(self.poll_interval)

    def _do_one_round(self) -> None:
        """Pick a random workflow, call it, and log the result."""
        # Lazy discovery retry if on_start failed
        if not self.remote_workflows:
            try:
                for key, wf_class in _WORKFLOW_CLASSES.items():
                    self.add_remote_workflow(
                        key=key,
                        resource_instance=self.calculator_id,
                        workflow_class=wf_class,
                    )
            except Exception as e:
                self.logger.error(f"Lazy remote workflow discovery failed: {e}")
                return

        op = random.choice(_WORKFLOW_KEYS)

        try:
            if op in ("add", "subtract"):
                a = random.choice((random.uniform(-100.0, 100.0), 42.0))
                b = random.uniform(-100.0, 100.0)
                response = self.remote_workflows[op](
                    **{
                        IRI(f"{CALC}operandA").lined: a,
                        IRI(f"{CALC}operandB").lined: b,
                    }
                )
            elif op == "random":
                response = self.remote_workflows["random"]()
            elif op == "void":
                v = random.choice((random.uniform(-100.0, 100.0), 42.0))
                response = self.remote_workflows["void"](
                    **{IRI(f"{CALC}value").lined: v}
                )
            else:  # repeat
                words = ["hello", "world", "foo", "bar", "baz", "kit"]
                text = random.choice(words)
                n = random.randint(0, 42)
                response = self.remote_workflows["repeat"](
                    **{
                        IRI(f"{CALC}inputString").lined: text,
                        IRI(f"{CALC}repeatCount").lined: n,
                    }
                )

            self.logger.info(f"[{op}] {response}")

            if response.is_success:
                with self._calc_lock:
                    self.number_of_calculations += 1

                # Extract the numeric result from content when present
                if response.content:
                    try:
                        result_data = json.loads(response.content)
                        self.logger.info(
                            f"[{op}] result = {result_data.get('result', '—')}"
                        )
                    except json.JSONDecodeError:
                        pass
            else:
                self.logger.warning(
                    f"[{op}] non-success: {response.status_code} {response.status}"
                )

        except Exception as e:
            self.logger.error(f"[{op}] invocation raised: {e}")
