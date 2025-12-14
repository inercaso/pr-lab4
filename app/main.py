"""
main fastapi application for the distributed key-value store.
provides rest api endpoints for leader and follower nodes.
supports both basic (last-writer-wins) and versioned write modes.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, status
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


# per-key version counters (leader only)
_version_counters: dict[str, int] = {}
_version_lock = asyncio.Lock()


async def get_next_version(key: str) -> int:
    """get next version number for a key (leader only)."""
    async with _version_lock:
        _version_counters[key] = _version_counters.get(key, 0) + 1
        return _version_counters[key]


# request/response models
class WriteRequest(BaseModel):
    """request body for write operations."""

    value: Any


class ReplicateRequest(BaseModel):
    """request body for internal replication."""

    key: str
    value: Any
    version: int | None = None  # optional version for versioned mode


class WriteResponse(BaseModel):
    """response for write operations."""

    success: bool
    key: str
    quorum_acks: int
    message: str
    version: int | None = None  # included in versioned mode


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
async def write_key(
    key: str,
    request: WriteRequest,
    versioned: bool = Query(default=False, description="use versioned writes"),
) -> WriteResponse:
    """
    write a value to the store.

    on the leader: writes locally and replicates to followers.
    on followers: rejected (writes must go to leader).

    args:
        key: the key to write
        request: the write request containing the value
        versioned: if true, use version-based conflict resolution
    """
    settings = get_settings()

    if not settings.is_leader:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="writes are only accepted on the leader node",
        )

    version = None
    if versioned:
        version = await get_next_version(key)
        logger.info(f"received versioned write: {key} = {request.value} (v{version})")
        # versioned write to local store
        await store.set_versioned(key, request.value, version)
    else:
        logger.info(f"received write request: {key} = {request.value}")
        # basic write to local store (last-writer-wins)
        await store.set(key, request.value)

    # replicate to followers
    success, ack_count, failed = await replicator.replicate(
        key, request.value, version=version
    )

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
        version=version,
    )


@app.post("/internal/replicate", status_code=status.HTTP_200_OK)
async def receive_replication(request: ReplicateRequest) -> dict[str, Any]:
    """
    internal endpoint for receiving replications from leader.
    used by followers to apply writes from the leader.

    supports both basic and versioned modes:
    - basic: always applies write (last-writer-wins)
    - versioned: only applies if version > current version
    """
    settings = get_settings()

    if settings.is_leader:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="leader should not receive replication requests",
        )

    if request.version is not None:
        # versioned mode - only accept if version > current
        logger.info(
            f"received versioned replication: {request.key} = {request.value} (v{request.version})"
        )
        accepted = await store.set_versioned(
            request.key, request.value, request.version
        )
        if accepted:
            return {"status": "ok", "key": request.key, "accepted": True}
        else:
            return {
                "status": "ok",
                "key": request.key,
                "accepted": False,
                "reason": "stale",
            }
    else:
        # basic mode - always apply (last-writer-wins)
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
