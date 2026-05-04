from typing import Type
from datetime import datetime, timezone
from pydantic import BaseModel, Field, create_model
from graph_db_interface import IRI
from kapps_ogm.ogm import OGM
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# Base Pydantic Models
# ============================================================================


class WorkflowPayload(BaseModel):
    """Base class for all workflow payload structures."""

    pass


class WorkflowResponse(BaseModel):
    """
    Base response structure for all workflows following REST best practices.

    Fields:
        status_code: HTTP status code (200, 400, 500, etc.)
        status: Machine-readable status string (e.g., 'success', 'error', 'timeout')
        message: Human-readable description
        content: Workflow-specific response content json
        timestamp: ISO 8601 timestamp of response generation
    """

    status_code: int
    status: str
    message: str
    content: str = ""
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def is_success(self) -> bool:
        """Convenience property to check if the response indicates success."""
        return 200 <= self.status_code < 300

    def __str__(self) -> str:
        base = f"[{self.timestamp}] {self.status_code} {self.status}: {self.message}"
        if not self.is_success and self.content:
            try:
                import json

                formatted = json.dumps(json.loads(self.content), indent=2)
            except Exception:
                formatted = self.content
            return f"{base}\n  content: {formatted}"
        return base


# ============================================================================
# Dynamic Model Building Functions
# ============================================================================


def build_payload_model(
    ogm: OGM,
    workflow_class_iri: IRI,
) -> Type[WorkflowPayload]:
    payload_model = _create_model(
        ogm,
        workflow_class_iri,
        workflow_property_iri=IRI("http://w3id.org/circularfactory/Workflow#accepts"),
        base_model=WorkflowPayload,
    )

    logger.debug(
        f"Created payload model '{payload_model.__name__}' with {len(payload_model.model_fields)} fields"
    )
    return payload_model


def build_response_model(
    ogm: OGM,
    workflow_class_iri: IRI,
) -> Type[WorkflowResponse]:
    response_model = _create_model(
        ogm,
        workflow_class_iri,
        workflow_property_iri=IRI("http://w3id.org/circularfactory/Workflow#returns"),
        base_model=WorkflowResponse,
    )

    logger.debug(
        f"Created response model '{response_model.__name__}' with {len(response_model.model_fields)} fields"
    )
    return response_model


def _create_model(
    ogm: OGM,
    workflow_class_iri: IRI,
    workflow_property_iri: IRI,
    base_model: Type[BaseModel],
) -> Type[BaseModel]:
    model_name = workflow_class_iri.lined + "_" + workflow_property_iri.fragment
    model_creation_dict = {}

    # Query for OWL restrictions defining property constraints
    query = f"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    
    SELECT ?property ?datatype WHERE {{
        <{workflow_class_iri}> rdfs:subClassOf ?workflowRestriction .
        ?workflowRestriction a owl:Restriction ;
                            owl:onProperty <{workflow_property_iri}> ;
                            owl:hasValue ?payload .
        ?payload rdfs:subClassOf ?payloadRestriction .
        ?payloadRestriction a owl:Restriction ;
                            owl:onProperty ?property ;
                            owl:hasValue ?datatype .
    }}
    """

    result = ogm.db.query(query, convert_bindings=True)
    bindings = result.get("results", {}).get("bindings", [])

    for binding in bindings:
        property_iri = binding.get("property")
        python_type = binding.get("datatype")

        if not property_iri or not python_type:
            raise Exception(
                f"Invalid OWL restriction for class {workflow_class_iri}: missing property or datatype"
            )

        # Extract field name from property IRI using graph_db_interface utility
        field_name = property_iri.lined

        # If datatype is a reference, use IRI as type
        if isinstance(python_type, IRI):
            python_type = IRI

        # Create field definition: (type, default_value)
        # Using ... as default means required field in Pydantic
        model_creation_dict[field_name] = (python_type, ...)

        logger.debug(
            f"Extracted field: {field_name} -> {python_type} from {property_iri}"
        )

    # Create the Pydantic model dynamically
    model = create_model(
        model_name,
        __base__=base_model,
        **model_creation_dict,
    )

    return model
