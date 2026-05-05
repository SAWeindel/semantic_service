"""kg_writer.py — Knowledge-Graph state persistence helpers.

All station state transitions are written here so that the KG is the
single source of truth.  Stations poll ``query_pending_operations()``
instead of relying solely on direct REST calls.

OGM usage notes
---------------
* ``ogm.create()`` persists a new record node; the class_scope is derived
  from the data dict using ``ClassScope.from_node_data`` so only the
  supplied properties are included.
* Status updates use ``db.triples_update()`` directly — it's an atomic
  old→new swap and avoids a full OGM fetch+diff cycle.
* Polling uses a raw SPARQL query via ``db.query()`` for flexibility.
"""

from __future__ import annotations

import copy
import json
import logging
from typing import Optional

from rdflib import Literal
from graph_db_interface import IRI
from graph_db_interface.utils.types import GraphNameLike
from kapps_ogm import OGM
from kapps_ogm.node.core import Node
from kapps_ogm.utils.class_scope import ClassScope

logger = logging.getLogger("kg_writer")

# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------
CFOP = "https://w3id.org/circularfactory/Operations#"

# OperationRecord class + properties
OP_RECORD_CLASS = IRI(f"{CFOP}OperationRecord")
P_REC_WORKPIECE = IRI(f"{CFOP}rec_workpieceRef")
P_REC_STATUS = IRI(f"{CFOP}rec_status")
P_REC_STATION = IRI(f"{CFOP}rec_assignedStationId")
P_REC_OP_TYPE = IRI(f"{CFOP}rec_operationType")
P_REC_PARAMS = IRI(f"{CFOP}rec_parameters")
P_REC_RESULT = IRI(f"{CFOP}rec_resultData")

# QualityRecord class + properties
QR_CLASS = IRI(f"{CFOP}QualityRecord")
P_QR_WORKPIECE = IRI(f"{CFOP}qr_workpieceRef")
P_QR_DATA = IRI(f"{CFOP}qr_qualityData")
P_QR_STRATEGY = IRI(f"{CFOP}qr_strategy")
P_QR_SCORE = IRI(f"{CFOP}qr_qualityScore")

# rdf:type predicate (for raw triple assertions)
RDF_TYPE = IRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _scope_from_data(ogm: OGM, data: dict) -> ClassScope:
    """Derive a ClassScope from a data dict (mirrors the demo pattern)."""
    temp = Node(data=copy.deepcopy(data), ogm=ogm)
    return ClassScope.from_node_data(temp)


# ---------------------------------------------------------------------------
# OperationRecord writes
# ---------------------------------------------------------------------------

def create_operation_record(
    ogm: OGM,
    named_graph: GraphNameLike,
    *,
    station_id: IRI,
    workpiece_ref: str,
    op_type: str,
    parameters: Optional[dict] = None,
) -> IRI:
    """Create an OperationRecord in the KG with status='pending'.

    Returns the IRI of the newly created record.
    """
    params_json = json.dumps(parameters or {})
    data = {
        P_REC_STATUS.lined:    ["pending"],
        P_REC_STATION.lined:   [str(station_id)],
        P_REC_WORKPIECE.lined: [workpiece_ref],
        P_REC_OP_TYPE.lined:   [op_type],
        P_REC_PARAMS.lined:    [params_json],
    }
    class_scope = _scope_from_data(ogm, data)
    node = ogm.create(
        class_iri=OP_RECORD_CLASS,
        data=data,
        class_scope=class_scope,
        named_graph=named_graph,
    )
    logger.info(
        "OperationRecord created: %s  station=%s  type=%s  workpiece=%s",
        node.id,
        station_id,
        op_type,
        workpiece_ref,
    )
    return node.id


def set_operation_running(
    db,
    named_graph: GraphNameLike,
    op_iri: IRI,
) -> None:
    """Atomically transition status pending → running."""
    db.triples_update(
        old_triples=[(op_iri, P_REC_STATUS, Literal("pending"))],
        new_triples=[(op_iri, P_REC_STATUS, Literal("running"))],
        named_graph=named_graph,
    )
    logger.info("OperationRecord %s → running", op_iri)


def set_operation_done(
    db,
    named_graph: GraphNameLike,
    op_iri: IRI,
    result: Optional[dict] = None,
) -> None:
    """Atomically transition status running → done and persist result JSON."""
    db.triples_update(
        old_triples=[(op_iri, P_REC_STATUS, Literal("running"))],
        new_triples=[(op_iri, P_REC_STATUS, Literal("done"))],
        named_graph=named_graph,
    )
    if result:
        db.triples_add(
            [(op_iri, P_REC_RESULT, Literal(json.dumps(result)))],
            named_graph=named_graph,
            check_exist=False,
        )
    logger.info("OperationRecord %s → done", op_iri)


def set_operation_failed(
    db,
    named_graph: GraphNameLike,
    op_iri: IRI,
    reason: str = "",
) -> None:
    """Atomically transition status running → failed."""
    db.triples_update(
        old_triples=[(op_iri, P_REC_STATUS, Literal("running"))],
        new_triples=[(op_iri, P_REC_STATUS, Literal("failed"))],
        named_graph=named_graph,
    )
    if reason:
        db.triples_add(
            [(op_iri, P_REC_RESULT, Literal(json.dumps({"error": reason})))],
            named_graph=named_graph,
            check_exist=False,
        )
    logger.info("OperationRecord %s → failed  reason=%s", op_iri, reason)


# ---------------------------------------------------------------------------
# OperationRecord polling
# ---------------------------------------------------------------------------

def query_pending_operations(
    db,
    named_graph: GraphNameLike,
    station_id: IRI,
) -> list[dict]:
    """Return all OperationRecords in 'pending' status assigned to station_id.

    Each result dict has keys: op_iri, workpiece_ref, op_type, parameters.
    """
    sparql = f"""
        SELECT ?op ?workpieceRef ?opType ?params WHERE {{
            GRAPH <{named_graph}> {{
                ?op a <{OP_RECORD_CLASS}> .
                ?op <{P_REC_STATION}> "{station_id}" .
                ?op <{P_REC_STATUS}> "pending" .
                ?op <{P_REC_WORKPIECE}> ?workpieceRef .
                ?op <{P_REC_OP_TYPE}> ?opType .
                OPTIONAL {{ ?op <{P_REC_PARAMS}> ?params . }}
            }}
        }}
    """
    result = db.query(sparql, convert_bindings=True)
    rows = result.get("results", {}).get("bindings", [])
    records = []
    for row in rows:
        params_raw = row.get("params", "")
        if hasattr(params_raw, "__str__"):
            params_raw = str(params_raw)
        try:
            params = json.loads(params_raw) if params_raw else {}
        except (json.JSONDecodeError, TypeError):
            params = {}
        records.append({
            "op_iri":        IRI(str(row["op"])),
            "workpiece_ref": str(row["workpieceRef"]),
            "op_type":       str(row["opType"]),
            "parameters":    params,
        })
    return records


def get_operation_result(
    db,
    named_graph: GraphNameLike,
    op_iri: IRI,
) -> Optional[dict]:
    """Read the result JSON written by the executing station (if present)."""
    triples = db.triples_get(
        sub=op_iri, pred=P_REC_RESULT, named_graph=named_graph
    )
    if not triples:
        return None
    try:
        return json.loads(str(triples[0][2]))
    except (json.JSONDecodeError, IndexError):
        return None


def wait_for_operation(
    db,
    named_graph: GraphNameLike,
    op_iri: IRI,
    poll_interval: float = 1.0,
    timeout: float = 60.0,
) -> str:
    """Block until the operation reaches 'done' or 'failed'.  Returns final status."""
    import time
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        triples = db.triples_get(sub=op_iri, pred=P_REC_STATUS, named_graph=named_graph)
        if triples:
            status = str(triples[0][2])
            if status in ("done", "failed"):
                return status
        time.sleep(poll_interval)
    logger.warning("Timeout waiting for operation %s", op_iri)
    return "timeout"


# ---------------------------------------------------------------------------
# QualityRecord writes
# ---------------------------------------------------------------------------

def create_quality_record(
    ogm: OGM,
    named_graph: GraphNameLike,
    *,
    workpiece_ref: str,
    quality_data: dict,
    strategy: str = "",
    quality_score: float = 0.0,
) -> IRI:
    """Persist a QualityRecord for a workpiece inspection/reprocessing result."""
    data = {
        P_QR_WORKPIECE.lined: [workpiece_ref],
        P_QR_DATA.lined:      [json.dumps(quality_data)],
        P_QR_STRATEGY.lined:  [strategy],
        P_QR_SCORE.lined:     [quality_score],
    }
    class_scope = _scope_from_data(ogm, data)
    node = ogm.create(
        class_iri=QR_CLASS,
        data=data,
        class_scope=class_scope,
        named_graph=named_graph,
    )
    logger.info(
        "QualityRecord created: %s  workpiece=%s  score=%.2f",
        node.id,
        workpiece_ref,
        quality_score,
    )
    return node.id


def query_quality_records(
    db,
    named_graph: GraphNameLike,
    workpiece_ref: str,
) -> list[dict]:
    """Return all QualityRecords for a given workpiece."""
    sparql = f"""
        SELECT ?qr ?data ?strategy ?score WHERE {{
            GRAPH <{named_graph}> {{
                ?qr a <{QR_CLASS}> .
                ?qr <{P_QR_WORKPIECE}> "{workpiece_ref}" .
                OPTIONAL {{ ?qr <{P_QR_DATA}> ?data . }}
                OPTIONAL {{ ?qr <{P_QR_STRATEGY}> ?strategy . }}
                OPTIONAL {{ ?qr <{P_QR_SCORE}> ?score . }}
            }}
        }}
    """
    result = db.query(sparql, convert_bindings=True)
    rows = result.get("results", {}).get("bindings", [])
    records = []
    for row in rows:
        data_raw = str(row.get("data", "{}"))
        try:
            data = json.loads(data_raw)
        except json.JSONDecodeError:
            data = {}
        records.append({
            "qr_iri":    IRI(str(row["qr"])),
            "data":      data,
            "strategy":  str(row.get("strategy", "")),
            "score":     float(str(row.get("score", 0.0))),
        })
    return records
