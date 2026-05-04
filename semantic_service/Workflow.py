from typing import Optional, Any, Dict
from datetime import datetime, timezone
import semantic_middleware as smw
from graph_db_interface import IRI
from pydantic import BaseModel, Field
import requests
import json

from kapps_ogm.ogm import OGM
from kapps_ogm.utils.class_scope import ClassScope

import logging

from .WorkflowModelBuilder import (
    build_payload_model,
    build_response_model,
    WorkflowPayload,
    WorkflowResponse,
)


class Workflow:
    workflow_method: callable
    workflow_class: IRI
    resource_instance: IRI
    payload_model: Optional[WorkflowPayload] = None
    response_model: Optional[WorkflowResponse] = None
    _workflow_url: str = None
    workflow_name: str = None
    _workflow_instance: IRI = None
    _middleware: smw.Middleware = None
    _is_remote: bool = False
    _ogm: Optional[OGM] = None

    def __init__(
        self,
        workflow_method: callable,
        workflow_class: IRI,
        resource_instance: IRI,
        ogm: OGM,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize a Workflow.

        Args:
            workflow_method: The callable implementing the workflow logic
            workflow_class: IRI of the workflow class in the ontology
            resource_instance: IRI of the resource instance this workflow belongs to
            payload_model: Optional Pydantic model for payload validation.
                          If None and ogm is provided, will be built dynamically from ontology.
            response_model: Optional Pydantic model for response validation.
                           If None and ogm is provided, will be built dynamically from ontology.
            ogm: OGM instance for dynamic model generation (required if models not provided)
            logger: Optional logger instance
        """
        self.workflow_method = workflow_method
        self.workflow_class = IRI(workflow_class)
        self.resource_instance = IRI(resource_instance)
        self._ogm = ogm
        self.workflow_name = self.workflow_class.lined
        self.logger = logger or logging.getLogger(
            f"Workflow-{self.resource_instance.fragment}-{self.workflow_class.fragment}"
        )

        self.payload_model = build_payload_model(ogm, workflow_class)
        self.response_model = build_response_model(ogm, workflow_class)

        if self.workflow_method is None:

            workflow_instance = self

            def remote_method(payload: WorkflowPayload) -> WorkflowResponse:
                return workflow_instance._call_remote(payload)

            self.workflow_method = remote_method

    @property
    def has_empty_payload(self) -> bool:
        """Determine if the workflow accepts an empty payload (i.e. has no fields)."""
        if not self.payload_model:
            raise ValueError("Payload model not defined")
        return len(self.payload_model.model_fields) == 0

    def _call_remote(self, payload: WorkflowPayload) -> WorkflowResponse:
        """
        Execute a remote workflow call and robustly parse the response.

        Handles all failure scenarios:
        - Connection errors / timeouts
        - Non-JSON or empty responses
        - HTTP-level errors (4xx/5xx from transport)
        - Application-level errors embedded in the JSON body
        """
        try:
            http_response = requests.post(
                self._workflow_url,
                json=payload.model_dump(),
                timeout=None,
            )
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Remote connection failed: {e}")
            return self.response_model(
                status_code=503,
                status="connection_error",
                message=f"Could not connect to remote workflow at {self._workflow_url}: {e}",
                content=json.dumps({"error": str(e)}),
            )
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Remote request failed: {e}")
            return self.response_model(
                status_code=500,
                status="request_error",
                message=f"Remote request failed: {e}",
                content=json.dumps({"error": str(e)}),
            )

        # Parse body: semantic fields live in the JSON body, not HTTP transport.
        # The middleware always returns HTTP 200; actual status/message are in the body.
        try:
            response_data = http_response.json()
        except Exception:
            # Non-JSON body (e.g. HTML error page, empty body)
            return self.response_model(
                status_code=http_response.status_code,
                status="non_json_response",
                message=http_response.text or "Empty response body",
                content=json.dumps({"raw": http_response.text}),
            )

        # If the HTTP transport itself signals an error (gateway timeout, etc.),
        # and the body has no semantic status, surface the transport error.
        if http_response.status_code >= 400 and "status" not in response_data:
            return self.response_model(
                status_code=http_response.status_code,
                status="http_error",
                message=f"HTTP {http_response.status_code}: {http_response.reason}",
                content=json.dumps(response_data),
            )

        expected_fields = set(
            k
            for k in self.response_model.model_fields.keys()
            if k not in {"status_code", "status", "message", "content", "timestamp"}
        )
        return self.response_model(
            status_code=response_data.get("status_code", http_response.status_code),
            status=response_data.get("status", http_response.reason or "unknown"),
            message=response_data.get("message", ""),
            content=response_data.get("content", ""),
            **{k: v for k, v in response_data.items() if k in expected_fields},
        )

    @classmethod
    def fetch_remote_workflow(
        cls,
        resource_instance: IRI,
        workflow_class: IRI,
        ogm: OGM,
        logger: Optional[logging.Logger] = None,
    ) -> "Workflow":
        """
        Fetch a remote workflow from the knowledge graph.

        Args:
            resource_instance: IRI of the resource instance
            workflow_class: IRI of the workflow class
            ogm: OGM instance for querying and dynamic model generation
            payload_model: Optional Pydantic model for payload.
                          If None, will be built dynamically from ontology.
            response_model: Optional Pydantic model for response.
                           If None, will be built dynamically from ontology.
            logger: Optional logger instance

        Returns:
            Workflow instance configured for remote invocation
        """
        query = f"""
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT DISTINCT ?workflow_url
        WHERE {{
            {{
                <{resource_instance}> <http://w3id.org/circularfactory/Workflow#hasWorkflow> ?workflow .
            }}
            UNION
            {{
                ?workflow <http://w3id.org/circularfactory/Workflow#isWorkflowOf> <{resource_instance}> .
            }}
            ?workflow rdf:type <{workflow_class}> .
            ?workflow <http://w3id.org/circularfactory/Workflow#accessibleAt> ?workflow_url .
        }}
        """
        results = ogm.db.query(query, convert_bindings=True)
        workflows = [
            result["workflow_url"] for result in results["results"]["bindings"]
        ]
        if not workflows:
            raise ValueError(
                f"No workflow of class {workflow_class} found for resource instance {resource_instance}"
            )
        if len(workflows) > 1:
            raise ValueError(
                f"Multiple workflows of class {workflow_class} found for resource instance {resource_instance}, expected only one."
            )
        workflow_url = workflows[0]

        logger = logger or logging.getLogger(
            f"Workflow-Remote@{resource_instance.fragment}-{workflow_class.fragment}"
        )

        workflow = cls(
            workflow_method=None,
            workflow_class=workflow_class,
            resource_instance=resource_instance,
            ogm=ogm,
            logger=logger,
        )

        # def remote_method(payload: WorkflowPayload) -> WorkflowResponse:
        #     response = requests.post(
        #         workflow_url,
        #         json=payload.model_dump(),
        #         timeout=None,
        #     )
        #     # Convert received JSON to proper response model instance
        #     response_data = response.json()
        #     expected_fields = set(
        #         k
        #         for k in workflow.response_model.model_fields.keys()
        #         if k not in {"status_code", "status", "message", "content", "timestamp"}
        #     )
        #     return workflow.response_model(
        #         status_code=response.status_code,
        #         status=response.reason if response.reason else "Unknown",
        #         message=response.text if response.text else "No message",
        #         content=json.dumps(response_data),
        #         **{k: v for k, v in response_data.items() if k in expected_fields},
        #     )

        # workflow.workflow_method = remote_method
        workflow._workflow_url = workflow_url
        workflow._is_remote = True
        return workflow

    def register_in_middleware(self, mw: smw.Middleware):
        if self._is_remote:
            raise ValueError("Remote workflows cannot be registered in middleware.")

        self._middleware = mw

        # Create dedicated wrapper to preserve self reference
        # Middleware expects a function that takes payload dict/model and returns response dict/model
        workflow_instance = self  # Capture in closure

        def workflow_wrapper(payload: WorkflowPayload) -> WorkflowResponse:
            """Wrapper that preserves the Workflow instance reference."""
            return workflow_instance(payload=payload)

        # Patch annotations with the concrete dynamic models so that
        # generate_workflow_endpoint / FastAPI deserializes the request body
        # into the correct specific payload model (with all its fields),
        # rather than the base WorkflowPayload which has no fields.
        workflow_wrapper.__annotations__["payload"] = self.payload_model
        workflow_wrapper.__annotations__["return"] = self.response_model

        mw.workflow(name=self.workflow_name)(workflow_wrapper)
        self.logger.info(f"Registered in middleware")

    def register_in_graph_db(self, host_url: str, ogm: OGM, named_graph: IRI = None):
        if self._is_remote:
            raise ValueError("Remote workflows cannot be registered in graph database.")
        if not self._middleware:
            raise ValueError(
                "Middleware must be registered before registering workflow in graph database."
            )

        self._workflow_url = f"{host_url}/workflows/{self.workflow_name}/execute"

        data = {
            IRI("http://w3id.org/circularfactory/Workflow#isWorkflowOf"): [
                {
                    "id": self.resource_instance,
                }
            ],
            IRI("http://w3id.org/circularfactory/Workflow#accessibleAt"): [
                self._workflow_url
            ],
        }
        class_scope = ClassScope.from_data_dict(data)

        workflow_node = ogm.create(
            class_iri=self.workflow_class,
            class_scope=class_scope,
            data=data,
            named_graph=named_graph,
        )

        # ogm.db.triple_add(
        #     (
        #         self.resource_instance,
        #         IRI("http://w3id.org/circularfactory/Workflow#hasWorkflow"),
        #         workflow_node.id,
        #     ),
        #     named_graph=named_graph,
        # )

        self._workflow_instance = workflow_node.id
        self.logger.info(f"Registered in knowledge graph")

    def deregister_in_middleware(self, ogm: OGM):
        if self._is_remote:
            raise ValueError("Remote workflows cannot be deregistered.")
        self._workflow_instance = None
        self.logger.warning(f"Workflow middleware deregistration not yet implemented")

    def deregister_in_graph_db(self, ogm: OGM, named_graph: IRI = None):
        """Remove workflow registration from the knowledge graph."""
        if self._is_remote:
            raise ValueError("Remote workflows cannot be cleaned from graph database.")
        if not self._workflow_instance:
            self.logger.info("Workflow not registered in graph, skipping cleanup")
            return

        self.logger.warning(f"Workflow GraphDB deregistration not yet implemented")

    def __call__(
        self, payload: Optional[WorkflowPayload] = None, **kwargs
    ) -> WorkflowResponse:
        """
        Execute the workflow with either a payload instance or keyword arguments.

        Args:
            payload: Optional payload instance. If None, constructs from kwargs.
            **kwargs: Keyword arguments to construct payload (only if payload is None)

        Returns:
            WorkflowResponse instance

        Raises:
            ValueError: If neither payload nor kwargs provided, or both provided

        Examples:
            # Using payload instance
            workflow(ReservePayload(box_iri="...", source="...", target="..."))

            # Using kwargs (more convenient)
            workflow(box_iri="...", source="...", target="...")
        """

        if not payload and not kwargs:
            if self.has_empty_payload:
                payload = self.payload_model()  # Create empty payload instance
            else:
                raise ValueError(
                    "Must provide either a payload instance or keyword arguments"
                )
        if payload and kwargs:
            raise ValueError(
                "Cannot provide both payload instance and keyword arguments"
            )

        if payload:
            if not isinstance(payload, self.payload_model):
                raise ValueError(
                    f"Invalid payload type, expected {self.payload_model}, got {type(payload)}"
                )
        else:
            payload = self.payload_model(**kwargs)

        self.logger.info(
            f"Invoking with payload:\n{json.dumps(payload.model_dump(), indent=2)}"
        )
        response = self.workflow_method(payload=payload)
        self.logger.info(f"Completed with response: {response}")
        return response
