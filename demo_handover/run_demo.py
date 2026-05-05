"""Handover demo — run_demo.py

Prerequisites
-------------
1. Set environment variables for GraphDB:
       GRAPHDB_URL        e.g. https://graphdb.iam-mms.kit.edu/
       GRAPHDB_USERNAME
       GRAPHDB_PASSWORD
       GRAPHDB_REPOSITORY

2. Upload ontology to GraphDB:
       ontology/Handover.ttl

3. Install (python 3.13+):
       pip install -e .   (from semantic_service root)

4. Run:
       python run_demo.py

What this script does
---------------------
* Connects to GraphDB.
* Clears the HandoverInstances named graph.
* Creates Recorder1 and Consumer1 instances via OGM (ogm.create).
* Starts the Recorder service (REST).
* Starts the Consumer service (REST); the Consumer discovers the Recorder's
  create_record workflow from GraphDB and begins polling every 5 s.
* Each poll: Consumer calls create_record → Recorder stores a hw:Record
  (after 1 s) → Recorder calls Consumer's consume_record → Consumer fetches
  and prints the record from GraphDB.
* Blocks until Ctrl-C.
"""

import atexit
import copy
import logging
import signal
import sys

from graph_db_interface import GraphDB
from graph_db_interface.utils.graph_db_credentials import GraphDBCredentials
from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM
from kapps_ogm.node.core import Node
from kapps_ogm.utils.class_scope import ClassScope

from Consumer import Consumer
from Recorder import Recorder

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HW = "http://demo.org/Handover#"
INST = "http://demo.org/HandoverInstances#"
NAMED_GRAPH = IRI("http://demo.org/HandoverInstances")

RECORDER_ID = IRI(f"{INST}Recorder1")
CONSUMER_ID = IRI(f"{INST}Consumer1")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_demo")


def _scope_from_data(ogm: OGM, data: dict) -> ClassScope:
    """Derive a ClassScope from a data dict (mirrors kapps_ogm demo convention)."""
    temp_node = Node(data=copy.deepcopy(data), ogm=ogm)
    return ClassScope.from_node_data(temp_node)


def create_instances(ogm: OGM) -> None:
    """Clear the named graph and create Recorder1 + Consumer1 via OGM."""
    ogm.db.clear_graph(NAMED_GRAPH)
    logger.info("Named graph cleared: <%s>", NAMED_GRAPH)

    # Recorder instance
    recorder_data = {
        "id": str(RECORDER_ID),
        IRI("hasRecordCount", HW).lined: [0],
    }
    ogm.create(
        class_iri=IRI("Recorder", HW),
        data=recorder_data,
        class_scope=_scope_from_data(ogm, recorder_data),
        named_graph=NAMED_GRAPH,
        persist=True,
    )
    logger.info("Recorder instance created: <%s>", RECORDER_ID)

    # Consumer instance
    consumer_data = {
        "id": str(CONSUMER_ID),
        IRI("hasRecordCount", HW).lined: [0],
    }
    ogm.create(
        class_iri=IRI("Consumer", HW),
        data=consumer_data,
        class_scope=_scope_from_data(ogm, consumer_data),
        named_graph=NAMED_GRAPH,
        persist=True,
    )
    logger.info("Consumer instance created: <%s>", CONSUMER_ID)


def main() -> None:
    # ------------------------------------------------------------------
    # Connect to GraphDB
    # ------------------------------------------------------------------
    logger.info("Connecting to GraphDB...")
    try:
        credentials = GraphDBCredentials.from_env()
    except ValueError as e:
        logger.error("Missing environment variable: %s", e)
        sys.exit(1)

    db = GraphDB(credentials=credentials)
    ogm = OGM(db=db)
    logger.info("Connected to %s / %s", credentials.base_url, credentials.repository)

    # ------------------------------------------------------------------
    # Seed instance data via OGM
    # ------------------------------------------------------------------
    create_instances(ogm)

    # ------------------------------------------------------------------
    # Instantiate services
    # ------------------------------------------------------------------
    recorder = Recorder(recorder_id=RECORDER_ID, ogm=ogm)
    consumer = Consumer(
        consumer_id=CONSUMER_ID,
        ogm=ogm,
        recorder_id=RECORDER_ID,
        poll_interval=5.0,
    )

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------
    def _shutdown(*_args) -> None:
        logger.info("Shutting down…")
        consumer.stop()
        recorder.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    atexit.register(lambda: (consumer.stop(), recorder.stop()))

    # ------------------------------------------------------------------
    # Start services
    # ------------------------------------------------------------------
    recorder.start()
    consumer.start()

    print()
    print("=" * 60)
    print("  Handover demo running")
    print("=" * 60)
    print(f"  Recorder REST : http://localhost:{recorder.port}/docs")
    print(f"  Consumer REST : http://localhost:{consumer.port}/docs")
    print()
    print("  Flow: Consumer polls → Recorder creates hw:Record in GraphDB")
    print("        → Recorder calls Consumer.consume_record → Consumer prints record")
    print()
    print("  Press Ctrl-C to stop.")
    print("=" * 60)
    print()

    try:
        recorder.server_thread.join()
    except KeyboardInterrupt:
        _shutdown()


if __name__ == "__main__":
    main()
