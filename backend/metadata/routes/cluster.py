import logging

from fastapi import APIRouter, Request
from sqlalchemy.exc import SQLAlchemyError

from metadata.cluster import cluster_summary, list_nodes
from metadata.errors import error_response
from metadata.schemas import ClusterSummary, NodeList

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1", tags=["cluster"])


def unavailable():
    return error_response(503, "METADATA_UNAVAILABLE", "Metadata chưa sẵn sàng.")


@router.get("/nodes", response_model=NodeList)
def nodes(request: Request):
    state = request.app.state
    if not state.initialized:
        return unavailable()
    try:
        with state.session_factory.begin() as session:
            return list_nodes(session)
    except SQLAlchemyError as exc:
        logger.warning("Cannot read node snapshots (%s)", type(exc).__name__)
        return unavailable()


@router.get("/cluster", response_model=ClusterSummary)
def cluster(request: Request):
    state = request.app.state
    if not state.initialized:
        return unavailable()
    try:
        with state.session_factory.begin() as session:
            return cluster_summary(session, state.settings, state.operation_lock.locked())
    except SQLAlchemyError as exc:
        logger.warning("Cannot read cluster snapshot (%s)", type(exc).__name__)
        return unavailable()
