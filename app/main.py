"""
main fastapi application for the distributed key-value store.
provides rest api endpoints for leader and follower nodes.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel

from app.config import get_settings
from app.replicator import replicator
from app.store import store

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# request/response models
class WriteRequest(BaseModel):
    """request body for write operations."""
    value: Any


class ReplicateRequest(BaseModel):
    """request body for internal replication."""
    key: str
    value: Any


class WriteResponse(BaseModel):
    """response for write operations."""
    success: bool
    key: str
    quorum_acks: int
    message: str


class ReadResponse(BaseModel):
    """response for read operations."""
    key: str
    value: Any | None
    found: bool


class StoreResponse(BaseModel):
    """response containing all store data."""
    node: str
    data: dict[str, Any]
    size: int


class HealthResponse(BaseModel):
    """health check response."""
    status: str
    node: str
    role: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    """handle startup and shutdown events."""
    settings = get_settings()
    logger.info(
        f"starting {settings.role} node: {settings.node_name} "
        f"on {settings.host}:{settings.port}"
    )
    if settings.is_leader:
        logger.info(
            f"leader config: quorum={settings.quorum}, "
            f"delay=[{settings.delay_min}ms, {settings.delay_max}ms], "
            f"followers={settings.follower_urls}"
        )
    yield
    # cleanup
    await replicator.close()
    logger.info(f"shutting down {settings.node_name}")


# create app
app = FastAPI(
    title="distributed key-value store",
    description="single-leader replication with semi-synchronous strategy",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """health check endpoint."""
    settings = get_settings()
    return HealthResponse(
        status="healthy",
        node=settings.node_name,
        role=settings.role,
    )


@app.post("/store/{key}", response_model=WriteResponse)
async def write_key(key: str, request: WriteRequest) -> WriteResponse:
    """
    write a value to the store.
    
    on the leader: writes locally and replicates to followers.
    on followers: rejected (writes must go to leader).
    """
    settings = get_settings()

    if not settings.is_leader:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="writes are only accepted on the leader node",
        )

    logger.info(f"received write request: {key} = {request.value}")

    # write to local store first
    await store.set(key, request.value)

    # replicate to followers
    success, ack_count, failed = await replicator.replicate(key, request.value)

    if not success:
        # rollback local write if quorum not met
        await store.delete(key)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"replication failed: only {ack_count}/{settings.quorum} acks received",
        )

    return WriteResponse(
        success=True,
        key=key,
        quorum_acks=ack_count,
        message=f"write successful with {ack_count} acknowledgments",
    )


@app.post("/internal/replicate", status_code=status.HTTP_200_OK)
async def receive_replication(request: ReplicateRequest) -> dict[str, str]:
    """
    internal endpoint for receiving replications from leader.
    used by followers to apply writes from the leader.
    """
    settings = get_settings()

    if settings.is_leader:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="leader should not receive replication requests",
        )

    logger.info(f"received replication: {request.key} = {request.value}")

    await store.set(request.key, request.value)

    return {"status": "ok", "key": request.key}


@app.get("/store/{key}", response_model=ReadResponse)
async def read_key(key: str) -> ReadResponse:
    """
    read a value from the store.
    works on both leader and followers.
    """
    value = await store.get(key)
    found = value is not None

    logger.debug(f"read request: {key} -> found={found}")

    return ReadResponse(key=key, value=value, found=found)


@app.get("/store", response_model=StoreResponse)
async def read_all() -> StoreResponse:
    """
    read all data from the store.
    useful for consistency checks.
    """
    settings = get_settings()
    data = await store.get_all()
    size = await store.size()

    return StoreResponse(
        node=settings.node_name,
        data=data,
        size=size,
    )


@app.delete("/store/{key}")
async def delete_key(key: str) -> dict[str, Any]:
    """
    delete a key from the store.
    only works on leader (would need replication for full implementation).
    """
    settings = get_settings()

    if not settings.is_leader:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="deletes are only accepted on the leader node",
        )

    deleted = await store.delete(key)

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"key '{key}' not found",
        )

    return {"success": True, "key": key, "message": "key deleted"}


def main() -> None:
    """run the application with uvicorn."""
    import uvicorn
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=False,
    )


if __name__ == "__main__":
    main()
