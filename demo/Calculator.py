"""Calculator — a Service that exposes five workflows.

Workflows
---------
add       : (operandA: float, operandB: float) -> float
subtract  : (operandA: float, operandB: float) -> float
random    : ()                                 -> float   (random value in [0, 1))
void      : (value: float)                     -> ()      (receives and discards)
repeat    : (inputString: str, repeatCount: int) -> str   (str repeated n times)

If any float input equals exactly 42.0 an error response is returned.
For repeat, repeatCount must be in [0, 42] (inclusive), else a 400 is returned.

The number of successful workflow invocations is tracked in
``self.number_of_calculations`` and exposed at GET /stats.
"""

import json
import random as _random
import threading
from typing import Optional

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from Service.Workflow import WorkflowPayload, WorkflowResponse
from Service.Service import Service

CALC = "http://demo.org/Calculator#"


class Calculator(Service):
    """Arithmetic calculator exposed as a Service."""

    NAMED_GRAPH = IRI("http://demo.org/CalculatorInstances")

    def __init__(
        self,
        calculator_id: IRI,
        ogm: OGM,
        host: str = "0.0.0.0",
    ) -> None:
        self.number_of_calculations: int = 0
        self._calc_lock = threading.Lock()

        super().__init__(service_id=calculator_id, ogm=ogm, host=host)

        # Expose live counter via REST: GET /stats
        service_ref = self

        @self.mw.app.get("/stats")
        def get_stats() -> dict:
            return {"number_of_calculations": service_ref.number_of_calculations}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _increment(self) -> None:
        with self._calc_lock:
            self.number_of_calculations += 1

    @staticmethod
    def _check_forbidden(*values: float) -> Optional[str]:
        """Return an error message if any value equals 42.0, else None."""
        for v in values:
            if v == 42.0:
                return "Input value 42.0 is forbidden"
        return None

    # ------------------------------------------------------------------
    # Workflows
    # ------------------------------------------------------------------

    @Service.workflow(workflow_class=IRI(f"{CALC}AddWorkflow"), key="add")
    def add_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Add two floats: result = operandA + operandB."""
        response_model = self.workflows["add"].response_model
        try:
            a = float(getattr(payload, IRI(f"{CALC}operandA").lined))
            b = float(getattr(payload, IRI(f"{CALC}operandB").lined))
            if err := self._check_forbidden(a, b):
                return response_model(
                    status_code=400,
                    status="forbidden_input",
                    message=err,
                    content="",
                )
            result = a + b
            self._increment()
            self.logger.info(f"add({a}, {b}) = {result}")
            return response_model(
                status_code=200,
                status="success",
                message=f"{a} + {b} = {result}",
                content=json.dumps({"result": result}),
            )
        except Exception as e:
            self.logger.error(f"add_workflow error: {e}")
            return response_model(
                status_code=500,
                status="error",
                message=str(e),
                content="",
            )

    @Service.workflow(workflow_class=IRI(f"{CALC}SubtractWorkflow"), key="subtract")
    def subtract_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Subtract b from a: result = operandA - operandB."""
        response_model = self.workflows["subtract"].response_model
        try:
            a = float(getattr(payload, IRI(f"{CALC}operandA").lined))
            b = float(getattr(payload, IRI(f"{CALC}operandB").lined))
            if err := self._check_forbidden(a, b):
                return response_model(
                    status_code=400,
                    status="forbidden_input",
                    message=err,
                    content="",
                )
            result = a - b
            self._increment()
            self.logger.info(f"subtract({a}, {b}) = {result}")
            return response_model(
                status_code=200,
                status="success",
                message=f"{a} - {b} = {result}",
                content=json.dumps({"result": result}),
            )
        except Exception as e:
            self.logger.error(f"subtract_workflow error: {e}")
            return response_model(
                status_code=500,
                status="error",
                message=str(e),
                content="",
            )

    @Service.workflow(workflow_class=IRI(f"{CALC}RandomWorkflow"), key="random")
    def random_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Return a random float in [0, 1). Takes no input."""
        response_model = self.workflows["random"].response_model
        try:
            value = _random.random()
            self._increment()
            self.logger.info(f"random() = {value}")
            return response_model(
                status_code=200,
                status="success",
                message=f"random = {value}",
                content=json.dumps({"result": value}),
            )
        except Exception as e:
            self.logger.error(f"random_workflow error: {e}")
            return response_model(
                status_code=500,
                status="error",
                message=str(e),
                content="",
            )

    @Service.workflow(workflow_class=IRI(f"{CALC}VoidWorkflow"), key="void")
    def void_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Receive a float and discard it. Returns no result."""
        response_model = self.workflows["void"].response_model
        try:
            value = float(getattr(payload, IRI(f"{CALC}value").lined))
            if err := self._check_forbidden(value):
                return response_model(
                    status_code=400,
                    status="forbidden_input",
                    message=err,
                    content="",
                )
            self._increment()
            self.logger.info(f"void({value}) — discarded")
            return response_model(
                status_code=200,
                status="success",
                message=f"Received {value} and discarded it",
                content="",
            )
        except Exception as e:
            self.logger.error(f"void_workflow error: {e}")
            return response_model(
                status_code=500,
                status="error",
                message=str(e),
                content="",
            )

    @Service.workflow(workflow_class=IRI(f"{CALC}RepeatWorkflow"), key="repeat")
    def repeat_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
        """Repeat inputString n times (repeatCount in [0, 42])."""
        response_model = self.workflows["repeat"].response_model
        try:
            text = str(getattr(payload, IRI(f"{CALC}inputString").lined))
            n = int(getattr(payload, IRI(f"{CALC}repeatCount").lined))
            if n < 0 or n > 42:
                return response_model(
                    status_code=400,
                    status="invalid_input",
                    message=f"repeatCount must be between 0 and 42, got {n}",
                    content="",
                )
            result = text * n
            self._increment()
            self.logger.info(f"repeat({text!r}, {n}) = {result!r}")
            return response_model(
                status_code=200,
                status="success",
                message=f"Repeated {text!r} {n} times",
                content=json.dumps({"result": result}),
            )
        except Exception as e:
            self.logger.error(f"repeat_workflow error: {e}")
            return response_model(
                status_code=500,
                status="error",
                message=str(e),
                content="",
            )
