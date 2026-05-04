"""Calculator demo — run_demo.py

Prerequisites
-------------
1. Set environment variables for GraphDB:
       GRAPHDB_URL        e.g. http://localhost:7200
       GRAPHDB_USERNAME
       GRAPHDB_PASSWORD
       GRAPHDB_REPOSITORY

2. Upload ontologies via your external tool:
       ontology/Calculator.ttl          → default graph (TBox / schema)
       ontology/CalculatorInstances.ttl → named graph <http://demo.org/CalculatorInstances>

3. Run:
       python run_demo.py

What this script does
---------------------
* Connects to GraphDB using the environment variables above.
* Creates the Calculator1 and User1 instance triples in the named graph
  (safe to run repeatedly — uses check_exist=False so GraphDB silently
  ignores duplicate triples).
* Starts the Calculator service (REST on port 8001 by default).
* Starts the User service (REST on port 8002 by default); the User
  discovers the Calculator's workflows from the knowledge graph and starts
  calling them on a 5-second timer.
* Prints the API URLs and blocks until Ctrl-C.
"""

import atexit
import logging
import os
import signal
import sys

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
from graph_db_interface import GraphDB
from graph_db_interface.utils.graph_db_credentials import GraphDBCredentials
from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from Usecase_Demo_Calculator.Calculator import Calculator
from Usecase_Demo_Calculator.User import User

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NAMED_GRAPH = IRI("http://demo.org/CalculatorInstances")
CALC = "http://demo.org/Calculator#"
INST = "http://demo.org/CalculatorInstances#"

CALCULATOR_ID = IRI(f"{INST}Calculator1")
USER_ID = IRI(f"{INST}User1")

RDF_TYPE = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
HAS_NUMBER_OF_USES = IRI(f"{CALC}hasNumberOfUses")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_demo")


def create_instances(ogm: OGM) -> None:
    """Create Calculator1 and User1 triples in the named graph.

    Uses check_exist=False so the INSERT DATA is sent unconditionally;
    GraphDB silently handles duplicate triples (RDF set semantics).
    """
    ogm.db.clear_graph(NAMED_GRAPH)  # Clear existing instances for a clean demo run
    instance_triples = [
        (CALCULATOR_ID, RDF_TYPE, IRI(f"{CALC}Calculator")),
        (CALCULATOR_ID, HAS_NUMBER_OF_USES, 0),
        (USER_ID, RDF_TYPE, IRI(f"{CALC}User")),
        (USER_ID, HAS_NUMBER_OF_USES, 0),
    ]
    try:
        ogm.db.triples_add(
            instance_triples,
            named_graph=NAMED_GRAPH,
        )
        logger.info("Instance triples added to <%s>", NAMED_GRAPH)
    except Exception as e:
        logger.warning("Could not write instance triples (may already exist): %s", e)


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
    # Seed instance data
    # ------------------------------------------------------------------
    create_instances(ogm)

    # ------------------------------------------------------------------
    # Instantiate services
    # ------------------------------------------------------------------
    calculator = Calculator(calculator_id=CALCULATOR_ID, ogm=ogm)
    user = User(
        user_id=USER_ID,
        ogm=ogm,
        calculator_id=CALCULATOR_ID,
        poll_interval=5.0,
    )

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------
    def _shutdown(*_args) -> None:
        logger.info("Shutting down…")
        user.stop()
        calculator.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    atexit.register(lambda: (user.stop(), calculator.stop()))

    # ------------------------------------------------------------------
    # Start services
    # ------------------------------------------------------------------
    calculator.start()
    user.start()

    print()
    print("=" * 60)
    print("  Calculator demo running")
    print("=" * 60)
    print(f"  Calculator REST:  http://localhost:{calculator.port}/docs")
    print(f"  Calculator stats: http://localhost:{calculator.port}/stats")
    print(f"  User REST:        http://localhost:{user.port}/docs")
    print(f"  User stats:       http://localhost:{user.port}/stats")
    print()
    print("  Press Ctrl-C to stop.")
    print("=" * 60)
    print()

    # Block the main thread until the calculator server exits naturally.
    try:
        calculator.server_thread.join()
    except KeyboardInterrupt:
        _shutdown()


if __name__ == "__main__":
    main()
