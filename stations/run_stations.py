"""run_stations.py — launch all Circular Factory station services.

Prerequisites
-------------
1. Set environment variables for GraphDB:
       GRAPHDB_URL        e.g. http://localhost:7200
       GRAPHDB_USERNAME
       GRAPHDB_PASSWORD
       GRAPHDB_REPOSITORY

2. Install:
       poetry install

3. Run (from the repo root):
       python stations/run_stations.py

What this script does
---------------------
* Connects to GraphDB using environment variables.
* Uploads ontology TTL files to GraphDB (idempotent — safe to re-run).
* Creates RDF instance triples for all station individuals.
* Starts all station services (each on its own port).
* Executes a short demonstration of Scenario 1 (disassembly) and
  Scenario 5 (plan healing) after all services are up.
* Blocks until Ctrl-C.

Port assignments
----------------
  ProductionControl    8010
  IntralogisticSystem  8011
  ManipulationSystem   8012
  InspectionSystem     8013
  TransformerCell      8014
  PlanHealer           8015
  SpecialMachine       8016
"""

import atexit
import logging
import os
import signal
import sys
import time


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
from graph_db_interface import GraphDB
from graph_db_interface.utils.graph_db_credentials import GraphDBCredentials
from graph_db_interface.utils.iri import IRI
from kapps_ogm import OGM

from production_control import ProductionControl
from intralogistic_system import IntralogisticSystem
from manipulation_system import ManipulationSystem
from inspection_system import InspectionSystem
from transformer_cell import TransformerCell
from plan_healer import PlanHealer
from special_machine import SpecialMachine



# ---------------------------------------------------------------------------
# Namespace constants
# ---------------------------------------------------------------------------
CFOP = "https://w3id.org/circularfactory/Operations#"
INST = "https://w3id.org/circularfactory/OperationsInstances#"
RDF_TYPE = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
NAMED_GRAPH = IRI(f"{INST}StationInstances")

# Instance IRIs
PC_ID = IRI(f"{INST}ProductionControl1")
ILS_ID = IRI(f"{INST}IntralogisticSystem1")
MS_ID = IRI(f"{INST}ManipulationSystem1")
IS_ID = IRI(f"{INST}InspectionSystem1")
TC_ID = IRI(f"{INST}TransformerCell1")
PH_ID = IRI(f"{INST}PlanHealer1")
SM_ID = IRI(f"{INST}SpecialMachine1")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_stations")


# ---------------------------------------------------------------------------
# Ontology upload
# ---------------------------------------------------------------------------

# Paths are resolved relative to the repo root, not this script's directory.
_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
_ONTOLOGY_FILES = [
    os.path.join(_REPO_ROOT, "ontology", "Workflow.ttl"),
    os.path.join(_REPO_ROOT, "ontology", "circular_factory_operations.ttl"),
]


def upload_ontologies(db) -> None:
    """Upload all ontology TTL files to GraphDB's default graph.

    The OGM's ClassSpec.specify() queries the default graph for
    ``rdf:type owl:Class`` triples. Unless the ontologies are present
    in GraphDB the workflow registration in Service.start() will fail
    with "Class <IRI> has no rdf:type defined."

    This function is idempotent: GraphDB merges (POST) rather than
    replacing, so re-running is safe.
    """
    for path in _ONTOLOGY_FILES:
        abs_path = os.path.abspath(path)
        if not os.path.exists(abs_path):
            logger.warning("Ontology file not found, skipping: %s", abs_path)
            continue
        with open(abs_path, "r", encoding="utf-8") as fh:
            content = fh.read()
        ok = db.import_statements(content=content, content_type="application/x-turtle")
        if ok:
            logger.info("Ontology uploaded: %s", os.path.basename(abs_path))
        else:
            logger.error("Failed to upload ontology: %s", abs_path)


# ---------------------------------------------------------------------------
# Instance seeding
# ---------------------------------------------------------------------------

def create_instances(ogm: OGM) -> None:
    ogm.db.clear_graph(NAMED_GRAPH)
    triples  = [
        (PC_ID,  RDF_TYPE, IRI(f"{CFOP}ProductionControl")),
        (ILS_ID, RDF_TYPE, IRI(f"{CFOP}IntralogisticSystem")),
        (MS_ID,  RDF_TYPE, IRI(f"{CFOP}ManipulationSystem")),
        (IS_ID,  RDF_TYPE, IRI(f"{CFOP}InspectionSystem")),
        (TC_ID,  RDF_TYPE, IRI(f"{CFOP}TransformerCell")),
        (PH_ID,  RDF_TYPE, IRI(f"{CFOP}PlanHealer")),
        (SM_ID,  RDF_TYPE, IRI(f"{CFOP}SpecialMachine")),
    ]
    ogm.db.triples_add(triples, named_graph=NAMED_GRAPH) # type: ignore
    logger.info("Station instance triples written to <%s>", NAMED_GRAPH)


# ---------------------------------------------------------------------------
# Demo sequences
# ---------------------------------------------------------------------------

def demo_scenario_1(pc: ProductionControl) -> None:
    """Scenario 1: transport + disassembly of an angle grinder housing."""
    logger.info("=== Scenario 1: Transport + Disassembly ===")
    try:
        pc.initiate_disassembly(
            product_id="AngleGrinder_Housing_001",
            pickup_location="arrival_point",
            tc_location="transformer_cell_1",
        )
    except Exception as e:
        logger.error(f"Scenario 1 failed: {e}")


def demo_scenario_2(pc: ProductionControl) -> None:
    """Scenario 2: measurement strategy selection + CT inspection."""
    logger.info("=== Scenario 2: Inspection Strategy + Measurement ===")
    try:
        result = pc.initiate_inspection(
            product_id="BevelGear_001",
            transport_location="ct_machine",
        )
        logger.info(f"Inspection result: {result}")
    except Exception as e:
        logger.error(f"Scenario 2 failed: {e}")


def demo_scenario_3(pc: ProductionControl) -> None:
    """Scenario 3: reprocessing of a bevel gear component."""
    logger.info("=== Scenario 3: Reprocessing ===")
    try:
        pc.initiate_reprocessing(
            product_id="BevelGear_001_lower",
            from_location="inspection_station",
            tc_location="transformer_cell_1",
            process_type="mill",
        )
    except Exception as e:
        logger.error(f"Scenario 3 failed: {e}")


def demo_scenario_5(pc: ProductionControl) -> None:
    """Scenario 5: PC writes a 'command' OperationRecord; TC fails and PH heals via KG."""
    logger.info("=== Scenario 5: Plan Healing (unscrew → mill) ===")
    try:
        op = pc.initiate_command(
            product_id="AngleGrinder_AG123",
            component_id="Screw_S05",
            process_type="unscrew",
        )
        logger.info("Command op %s dispatched; TC will fail and PH will write 'mill' op", op)
    except Exception as e:
        logger.error("Scenario 5 failed: %s", e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    load_dotenv()

    logger.info("Connecting to GraphDB…")
    try:
        credentials = GraphDBCredentials.from_env()
    except ValueError as e:
        logger.error("Missing environment variable: %s", e)
        sys.exit(1)

    db = GraphDB(credentials=credentials)
    ogm = OGM(db=db)
    logger.info("Connected to %s / %s", credentials.base_url, credentials.repository)

    # Upload ontologies FIRST — OGM needs class definitions in GraphDB before
    # any Service.start() call, which calls Workflow.register_in_graph_db() →
    # ogm.create() → ClassSpec.specify() → db.triples_get(sub=<WorkflowClass>,
    # pred="rdf:type"), which fails with "no rdf:type defined" if the ontology
    # is not present.
    upload_ontologies(db)

    create_instances(ogm)

    # Instantiate all station services (no remote discovery yet — that happens in on_start)
    sm = SpecialMachine(
        sm_id=SM_ID, ogm=ogm, tc_id=TC_ID, machine_type="subtractive", host="0.0.0.0"
    )
    ms = ManipulationSystem(ms_id=MS_ID, ogm=ogm, host="0.0.0.0")
    ils = IntralogisticSystem(ils_id=ILS_ID, ogm=ogm, ms_id=MS_ID, pc_id=PC_ID, host="0.0.0.0")
    is_ = InspectionSystem(is_id=IS_ID, ogm=ogm, pc_id=PC_ID, host="0.0.0.0")
    ph = PlanHealer(ph_id=PH_ID, ogm=ogm, tc_id=TC_ID, host="0.0.0.0")
    tc = TransformerCell(
        tc_id=TC_ID,
        ogm=ogm,
        pc_id=PC_ID,
        ph_id=PH_ID,
        sm_ids=[SM_ID],
        host="0.0.0.0",
    )
    pc = ProductionControl(
        pc_id=PC_ID,
        ogm=ogm,
        ils_id=ILS_ID,
        tc_id=TC_ID,
        is_id=IS_ID,
        ms_id=MS_ID,
        host="0.0.0.0",
    )

    all_services = [sm, ms, ils, is_, ph, tc, pc]
    ports = {
        pc:  8010,
        ils: 8011,
        ms:  8012,
        is_: 8013,
        tc:  8014,
        ph:  8015,
        sm:  8016,
    }

    # Graceful shutdown
    def _shutdown(*_args) -> None:
        logger.info("Shutting down all station services…")
        for svc in reversed(all_services):
            try:
                svc.stop()
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    atexit.register(lambda: [svc.stop() for svc in all_services])

    # Start all services (each binds its own port via Service.start())
    for svc in all_services:
        svc.port = ports[svc]
        svc.start()
        logger.info(f"  {svc.__class__.__name__} started on port {ports[svc]}")

    # Give services a moment to fully initialise before running demos
    time.sleep(3)

    print()
    print("=" * 64)
    print("  Circular Factory Station Services")
    print("=" * 64)
    for svc, port in ports.items():
        print(f"  {svc.__class__.__name__:<25} http://localhost:{port}/docs")
    print()
    print("  Press Ctrl-C to stop all services.")
    print("=" * 64)
    print()

    # Run demo scenarios — each dispatches OperationRecords to the KG;
    # stations pick them up via their background poll loops.
    import threading
    def _run_demos():
        time.sleep(2)  # let all poll loops start
        demo_scenario_1(pc)
        time.sleep(2)
        demo_scenario_2(pc)
        time.sleep(2)
        demo_scenario_3(pc)
        time.sleep(2)
        demo_scenario_5(pc)
    threading.Thread(target=_run_demos, daemon=True, name="demo-runner").start()

    # Block on PC server thread
    try:
        pc.server_thread.join() # type: ignore
    except KeyboardInterrupt:
        _shutdown()


if __name__ == "__main__":
    main()
