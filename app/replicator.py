"""
replicator module for semi-synchronous replication.
handles broadcasting writes to followers with configurable quorum.
implements network delay simulation as per lab requirements.
"""

import asyncio
import logging
from random import uniform
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)


class ReplicationResult:
    """result of a replication attempt to a single follower."""

    def __init__(self, follower_url: str, success: bool, error: str | None = None):
        self.follower_url = follower_url
        self.success = success
        self.error = error


class Replicator:
    """
    handles semi-synchronous replication to followers.
    
    semi-synchronous means we wait for a configurable number of
    followers (quorum) to acknowledge before returning success.
    remaining followers receive updates asynchronously.
    """

    def __init__(self) -> None:
        """initialize replicator with settings."""
        self._settings = get_settings()
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """get or create http client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def close(self) -> None:
        """close http client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _simulate_network_delay(self) -> float:
        """
        simulate network delay before sending replication request.
        delay is random in [delay_min, delay_max] milliseconds.
        
        returns:
            actual delay applied in seconds
        """
        delay_ms = uniform(self._settings.delay_min, self._settings.delay_max)
        delay_sec = delay_ms / 1000.0
        await asyncio.sleep(delay_sec)
        return delay_sec

    async def _replicate_to_follower(
        self, follower_url: str, key: str, value: Any
    ) -> ReplicationResult:
        """
        replicate a single write to one follower.
        
        args:
            follower_url: base url of the follower
            key: key to replicate
            value: value to replicate
            
        returns:
            replicationresult indicating success or failure
        """
        try:
            # simulate network delay before sending
            delay = await self._simulate_network_delay()
            logger.debug(
                f"replicated to {follower_url} after {delay:.3f}s delay"
            )

            client = await self._get_client()
            response = await client.post(
                f"{follower_url}/internal/replicate",
                json={"key": key, "value": value},
            )

            if response.status_code == 200:
                logger.info(f"replication to {follower_url} succeeded")
                return ReplicationResult(follower_url, success=True)
            else:
                error_msg = f"status {response.status_code}"
                logger.warning(f"replication to {follower_url} failed: {error_msg}")
                return ReplicationResult(follower_url, success=False, error=error_msg)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"replication to {follower_url} failed: {error_msg}")
            return ReplicationResult(follower_url, success=False, error=error_msg)

    async def replicate(self, key: str, value: Any) -> tuple[bool, int, list[str]]:
        """
        replicate write to all followers using semi-synchronous strategy.
        
        semi-synchronous replication:
        1. send replication requests to all followers concurrently
        2. wait until quorum number of followers acknowledge
        3. return success immediately when quorum is met
        4. remaining replications continue in background
        
        args:
            key: key to replicate
            value: value to replicate
            
        returns:
            tuple of (success, ack_count, failed_followers)
            - success: true if quorum was met
            - ack_count: number of successful acknowledgments
            - failed_followers: list of follower urls that failed
        """
        follower_urls = self._settings.follower_urls
        quorum = self._settings.quorum

        if not follower_urls:
            logger.warning("no followers configured, skipping replication")
            return True, 0, []

        logger.info(
            f"starting replication of {key} to {len(follower_urls)} followers, "
            f"quorum={quorum}"
        )

        # create tasks for all followers
        tasks = {
            asyncio.create_task(
                self._replicate_to_follower(url, key, value)
            ): url
            for url in follower_urls
        }

        pending = set(tasks.keys())
        successful_acks = 0
        failed_followers = []
        quorum_met = False

        # wait for responses until quorum is met or all complete
        while pending and not quorum_met:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )

            for task in done:
                result = task.result()
                if result.success:
                    successful_acks += 1
                    logger.debug(
                        f"received ack from {result.follower_url} "
                        f"({successful_acks}/{quorum} for quorum)"
                    )
                    if successful_acks >= quorum:
                        quorum_met = True
                        break
                else:
                    failed_followers.append(result.follower_url)

        # if quorum met, let remaining tasks complete in background
        if pending:
            logger.debug(
                f"quorum met, {len(pending)} replications continuing in background"
            )
            # create background task to handle remaining replications
            asyncio.create_task(self._complete_remaining(pending, failed_followers))

        success = successful_acks >= quorum
        logger.info(
            f"replication completed: success={success}, "
            f"acks={successful_acks}/{quorum}, "
            f"failed={len(failed_followers)}"
        )

        return success, successful_acks, failed_followers

    async def _complete_remaining(
        self, pending: set, failed_followers: list[str]
    ) -> None:
        """
        complete remaining replication tasks in background.
        
        args:
            pending: set of pending tasks
            failed_followers: list to append failures to (for logging)
        """
        for task in asyncio.as_completed(pending):
            try:
                result = await task
                if not result.success:
                    failed_followers.append(result.follower_url)
                    logger.warning(
                        f"background replication to {result.follower_url} failed"
                    )
            except Exception as e:
                logger.error(f"background replication task failed: {e}")


# global replicator instance
replicator = Replicator()
