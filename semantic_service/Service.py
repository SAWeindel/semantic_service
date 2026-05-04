"""
Service — abstract base class for ontology-driven, middleware-backed services.

Usage
-----
class MyService(Service):
    NAMED_GRAPH = IRI("http://example.org/Instances")

    @Service.workflow(workflow_class=IRI("http://example.org#MyWorkflow"), key="my_workflow")
    def my_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
        ...

svc = MyService(
    service_id=IRI("http://example.org/Instances#MyService1"),
    ogm=ogm,
)
svc.start()
"""

import logging
import socket
import threading
import time
from typing import Optional, Callable, Any

import uvicorn
import semantic_middleware as smw

from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from .Workflow import Workflow

# ---------------------------------------------------------------------------
# Decorator helper
# ---------------------------------------------------------------------------


class _WorkflowDefinition:
    """Holds the information captured by the @Service.workflow decorator."""

    def __init__(
        self, method: Callable, workflow_class: IRI, key: Optional[str] = None
    ):
        self.method = method
        self.workflow_class = IRI(workflow_class)
        # Key used in self.workflows dict; defaults to method name if not provided.
        self.key: str = key if key is not None else method.__name__


# ---------------------------------------------------------------------------
# Service base class
# ---------------------------------------------------------------------------


class Service:
    """
    Base class for ontology-driven, middleware-backed services.

    Each concrete subclass represents one logical service that:
    - Holds an OGM connection to the knowledge graph.
    - Runs a uvicorn/FastAPI REST server backed by semantic_middleware.
    - Exposes one or more workflows registered both in middleware and graph DB.
    - Can hold proxies to remote workflows of other services.

    Subclass responsibilities
    -------------------------
    - Set NAMED_GRAPH at class level (or pass named_graph to __init__).
    - Decorate workflow handler methods with @Service.workflow(workflow_class=...).
    - Optionally override _setup_middleware_data_model() for richer data exposure.
    - Optionally override on_start() / on_stop() for domain-specific logic.
    - Call self.add_remote_workflow() inside on_start() to register remote proxies.

    Lifecycle
    ---------
    __init__  →  _setup_middleware_data_model()  →  _register_decorated_workflows()
              →  register each in middleware
    start()   →  server thread  →  URL resolution  →  register in graph DB  →  on_start()
    stop()    →  on_stop()  →  server shutdown  →  deregister from graph DB
    """

    # ------------------------------------------------------------------
    # Class-level configuration
    # ------------------------------------------------------------------

    NAMED_GRAPH: Optional[IRI] = None
    """Named graph IRI for workflow registration. Must be set by subclass."""

    # Single shared port counter across ALL Service subclass instances.
    _port_counter: int = 8000
    _port_lock: threading.Lock = threading.Lock()

    # ------------------------------------------------------------------
    # @workflow decorator
    # ------------------------------------------------------------------

    @staticmethod
    def workflow(workflow_class: IRI, key: Optional[str] = None) -> Callable:
        """
        Decorator that marks a method as a workflow handler.

        The decorated method will be wrapped in a Workflow object at __init__ time,
        registered in the middleware, and published to the knowledge graph on start().

        Parameters
        ----------
        workflow_class:
            IRI of the OWL class for this workflow in the ontology.
        key:
            Key under which the Workflow object is stored in self.workflows.
            Defaults to the method name if not provided.

        Example
        -------
        @Service.workflow(workflow_class=IRI("http://...#ReserveWorkflow"), key="reserve")
        def reserve_workflow(self, payload: WorkflowPayload) -> WorkflowResponse:
            ...
        """

        def decorator(method: Callable) -> Callable:
            method._service_workflow_def = _WorkflowDefinition(
                method=method,
                workflow_class=workflow_class,
                key=key,
            )
            return method

        return decorator

    # ------------------------------------------------------------------
    # Port counter
    # ------------------------------------------------------------------

    @classmethod
    def _get_next_port(cls) -> int:
        """Thread-safe port allocation shared across all Service subclass instances."""
        with Service._port_lock:
            Service._port_counter += 1
            return Service._port_counter

    # ------------------------------------------------------------------
    # Constructor
    # ------------------------------------------------------------------

    def __init__(
        self,
        service_id: IRI,
        ogm: OGM,
        host: str = "0.0.0.0",
        named_graph: Optional[IRI] = None,
    ):
        """
        Parameters
        ----------
        service_id:
            IRI of this service instance in the knowledge graph.
        ogm:
            Connected OGM instance used for all graph operations.
        host:
            Host to bind the uvicorn server to.  "0.0.0.0" exposes the service
            on all interfaces; the public URL is resolved to the machine hostname.
            Use "localhost" for single-machine mode.
        named_graph:
            Named graph for workflow registration.  Falls back to NAMED_GRAPH
            class attribute if not supplied.
        """
        if ogm is None:
            raise ValueError("OGM instance is required.")

        self.service_id: IRI = IRI(service_id)
        self.ogm: OGM = ogm
        self.host: str = host
        self.named_graph: IRI = named_graph or self.NAMED_GRAPH
        if self.named_graph is None:
            raise ValueError(
                "named_graph must be provided either via the named_graph argument "
                "or the NAMED_GRAPH class attribute."
            )

        # Network
        self.port: int = self._get_next_port()
        self.url: Optional[str] = None

        # Server state
        self.server_thread: Optional[threading.Thread] = None
        self.server: Optional[uvicorn.Server] = None
        self.running: bool = False

        # Middleware
        self.mw: smw.Middleware = smw.Middleware()

        # Workflow registries
        self.workflows: dict[str, Workflow] = {}
        """Local workflows, keyed by the 'key' argument of @Service.workflow."""

        self.remote_workflows: dict = {}
        """
        Remote workflow proxies.  The concrete type and key convention is
        determined by the subclass.  Service.add_remote_workflow() stores flat
        string-keyed entries; subclasses may use any structure they need.
        """

        # Logger pair — consistent with existing module convention
        self.logger_parent: logging.Logger = logging.getLogger(
            f"{type(self).__name__}-{self.service_id.fragment}"
        )
        self.logger: logging.Logger = self.logger_parent.getChild("SubProjectLogic")

        # Subclass-specific data model loading (override to customise)
        self._setup_middleware_data_model()

        # Materialise Workflow objects from all @Service.workflow-decorated methods
        self._register_decorated_workflows()

        # Register each local workflow in the middleware
        for wf in self.workflows.values():
            wf.register_in_middleware(self.mw)

        self.logger.info("Initialization complete")

    # ------------------------------------------------------------------
    # Internal: workflow materialisation from decorator tags
    # ------------------------------------------------------------------

    def _register_decorated_workflows(self) -> None:
        """
        Walk the MRO and create a Workflow object for every method tagged by
        @Service.workflow, storing it in self.workflows under the configured key.
        """
        seen: set[str] = set()
        for cls in type(self).__mro__:
            for attr_name, attr_value in vars(cls).items():
                if attr_name in seen:
                    continue
                seen.add(attr_name)

                definition: Optional[_WorkflowDefinition] = getattr(
                    attr_value, "_service_workflow_def", None
                )
                if definition is None:
                    continue

                bound_method = getattr(self, attr_name)
                wf = Workflow(
                    workflow_method=bound_method,
                    workflow_class=definition.workflow_class,
                    resource_instance=self.service_id,
                    ogm=self.ogm,
                    logger=self.logger_parent.getChild(attr_name),
                )
                self.workflows[definition.key] = wf

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    def _setup_middleware_data_model(self) -> None:
        """
        Load the data model into self.mw.

        Default: minimal empty DataModel, suitable for workflow-only services
        (e.g. MockWMS).  Override in subclasses that need to expose instance
        data or require OGM-based model generation (e.g. FlexConveyor).
        """
        data_model = smw.DataModel()
        self.mw.load_data_model(
            name=str(self.service_id),
            data_model=data_model,
            persist_instances=False,
        )

    def on_start(self) -> None:
        """
        Called after the server is up and all local workflows are registered in
        the knowledge graph.

        Override for domain-specific post-start logic such as discovering remote
        workflows, building routing tables, or scheduling initial tasks.
        """
        pass

    def on_stop(self) -> None:
        """
        Called at the beginning of stop(), before the server is asked to exit.

        Override for domain-specific cleanup such as draining queues or
        cancelling background threads.
        """
        pass

    # ------------------------------------------------------------------
    # Remote workflow helpers
    # ------------------------------------------------------------------

    def add_remote_workflow(
        self,
        key: str,
        resource_instance: IRI,
        workflow_class: IRI,
        logger_suffix: Optional[str] = None,
    ) -> Workflow:
        """
        Fetch a remote workflow proxy from the knowledge graph and store it.

        Parameters
        ----------
        key:
            Arbitrary string key under which the proxy is stored in
            self.remote_workflows.
        resource_instance:
            IRI of the remote resource that owns the workflow.
        workflow_class:
            IRI of the workflow class to fetch.
        logger_suffix:
            Label for the child logger; defaults to key.

        Returns
        -------
        The created remote Workflow proxy.
        """
        suffix = logger_suffix or key
        wf = Workflow.fetch_remote_workflow(
            resource_instance=IRI(resource_instance),
            workflow_class=IRI(workflow_class),
            ogm=self.ogm,
            logger=self.logger_parent.getChild(f"Remote-{suffix}"),
        )
        self.remote_workflows[key] = wf
        return wf

    # ------------------------------------------------------------------
    # Lifecycle: start / stop
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Full startup sequence:
        1. Spawn uvicorn server thread.
        2. Wait briefly for the server to bind.
        3. Resolve the public URL.
        4. Register all local workflows in the knowledge graph.
        5. Call on_start() for domain-specific post-start work.
        6. Log the startup banner.
        """
        if self.running:
            self.logger.warning(f"Already running at {self.url}")
            return

        self.running = True
        self.server_thread = threading.Thread(
            target=self._run_server,
            daemon=False,
            name=f"{type(self).__name__}-{self.service_id.fragment}",
        )
        self.server_thread.start()
        time.sleep(1)  # Allow uvicorn to bind to the port

        # Resolve public URL
        if self.host == "0.0.0.0":
            hostname = socket.gethostname()
            self.url = f"http://{hostname}:{self.port}"
        else:
            self.url = f"http://{self.host}:{self.port}"

        # Register local workflows in the knowledge graph
        for wf in self.workflows.values():
            wf.register_in_graph_db(
                host_url=self.url,
                ogm=self.ogm,
                named_graph=self.named_graph,
            )

        # Domain-specific post-start hook
        self.on_start()

        self.logger.info(
            f"\n{'='*70}\n"
            f"  {type(self).__name__} REST API Started\n"
            f"  Service ID:    {self.service_id}\n"
            f"  Accessible at: {self.url}\n"
            f"  GUI access:    http://localhost:{self.port}/docs\n"
            f"  Workflows:     "
            + f",\n{'':17}".join(wf.workflow_name for wf in self.workflows.values())
            + f"\n{'='*70}"
        )

    def _run_server(self) -> None:
        """Target for the server thread — runs uvicorn synchronously."""
        config = uvicorn.Config(
            app=self.mw.app,
            host=self.host,
            port=self.port,
            log_level="info",
        )
        self.server = uvicorn.Server(config)
        try:
            self.server.run()
        except Exception as e:
            self.logger.error(f"Server error: {e}")
        finally:
            self.running = False

    def stop(self) -> None:
        """
        Graceful shutdown sequence:
        1. Call on_stop() for domain-specific pre-shutdown cleanup.
        2. Signal uvicorn to exit and join the server thread.
        3. Deregister all local workflows from the knowledge graph.
        """
        self.on_stop()

        if not self.running:
            self.logger.warning("Not running; skipping server shutdown.")
            self._deregister_all_workflows()
            return

        self.running = False
        if self.server is not None:
            self.server.should_exit = True
        if self.server_thread:
            self.server_thread.join(timeout=10)
            if self.server_thread.is_alive() and self.server is not None:
                self.server.force_exit = True
                self.server_thread.join(timeout=2)

        self._deregister_all_workflows()
        self.logger.info(f"{type(self).__name__} stopped")

    def _deregister_all_workflows(self) -> None:
        """Remove all local workflow registrations from the knowledge graph."""
        for wf in self.workflows.values():
            try:
                wf.deregister_in_graph_db(
                    ogm=self.ogm,
                    named_graph=self.named_graph,
                )
            except Exception as e:
                self.logger.warning(
                    f"Error deregistering workflow {wf.workflow_name}: {e}"
                )
