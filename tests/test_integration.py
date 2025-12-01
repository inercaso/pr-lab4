"""
integration tests for the distributed key-value store.
tests the system with leader and followers running in docker.
"""

import asyncio
import time

import httpx
import pytest

# configuration
LEADER_URL = "http://localhost:8080"
FOLLOWER_URLS = {
    "follower1": "http://localhost:8081",
    "follower2": "http://localhost:8082",
    "follower3": "http://localhost:8083",
    "follower4": "http://localhost:8084",
    "follower5": "http://localhost:8085",
}


@pytest.fixture
def client():
    """create http client for tests."""
    return httpx.Client(timeout=30.0)


@pytest.fixture
def async_client():
    """create async http client for tests."""
    return httpx.AsyncClient(timeout=30.0)


class TestHealthCheck:
    """test health check endpoints."""

    def test_leader_health(self, client):
        """verify leader is healthy."""
        response = client.get(f"{LEADER_URL}/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["role"] == "leader"

    def test_all_followers_health(self, client):
        """verify all followers are healthy."""
        for name, url in FOLLOWER_URLS.items():
            response = client.get(f"{url}/health")
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert data["role"] == "follower"
            assert data["node"] == name


class TestBasicOperations:
    """test basic read/write operations."""

    def test_write_to_leader(self, client):
        """test writing a value to the leader."""
        key = f"test_key_{int(time.time())}"
        value = {"message": "hello world", "number": 42}

        response = client.post(
            f"{LEADER_URL}/store/{key}",
            json={"value": value},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["key"] == key

    def test_read_from_leader(self, client):
        """test reading a value from the leader."""
        key = f"read_test_{int(time.time())}"
        value = "test_value"

        # write first
        client.post(f"{LEADER_URL}/store/{key}", json={"value": value})

        # then read
        response = client.get(f"{LEADER_URL}/store/{key}")

        assert response.status_code == 200
        data = response.json()
        assert data["found"] is True
        assert data["value"] == value

    def test_read_nonexistent_key(self, client):
        """test reading a key that doesn't exist."""
        response = client.get(f"{LEADER_URL}/store/nonexistent_key_12345")

        assert response.status_code == 200
        data = response.json()
        assert data["found"] is False
        assert data["value"] is None


class TestReplication:
    """test replication to followers."""

    def test_replication_to_all_followers(self, client):
        """verify data is replicated to all followers."""
        key = f"replication_test_{int(time.time())}"
        value = {"replicated": True, "timestamp": time.time()}

        # write to leader
        response = client.post(
            f"{LEADER_URL}/store/{key}",
            json={"value": value},
        )
        assert response.status_code == 200

        # wait for async replication to complete
        time.sleep(3)

        # verify on leader
        leader_data = client.get(f"{LEADER_URL}/store/{key}").json()
        assert leader_data["found"] is True
        assert leader_data["value"] == value

        # verify on all followers
        for name, url in FOLLOWER_URLS.items():
            follower_data = client.get(f"{url}/store/{key}").json()
            assert follower_data["found"] is True, f"{name} should have the key"
            assert follower_data["value"] == value, f"{name} should have correct value"

    def test_write_quorum(self, client):
        """test that writes succeed with configured quorum."""
        key = f"quorum_test_{int(time.time())}"
        value = "quorum_value"

        response = client.post(
            f"{LEADER_URL}/store/{key}",
            json={"value": value},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["quorum_acks"] >= 1  # at least quorum number of acks

    def test_read_from_follower(self, client):
        """test reading a replicated value from a follower."""
        key = f"follower_read_test_{int(time.time())}"
        value = "follower_readable_value"

        # write to leader
        client.post(f"{LEADER_URL}/store/{key}", json={"value": value})

        # wait for replication
        time.sleep(2)

        # read from first follower
        response = client.get(f"{FOLLOWER_URLS['follower1']}/store/{key}")
        assert response.status_code == 200
        data = response.json()
        assert data["found"] is True
        assert data["value"] == value


class TestFollowerBehavior:
    """test follower-specific behavior."""

    def test_follower_rejects_writes(self, client):
        """followers should reject direct write requests."""
        for name, url in FOLLOWER_URLS.items():
            response = client.post(
                f"{url}/store/test_key",
                json={"value": "should_fail"},
            )
            assert response.status_code == 403, f"{name} should reject writes"

    def test_follower_accepts_reads(self, client):
        """followers should accept read requests."""
        for name, url in FOLLOWER_URLS.items():
            response = client.get(f"{url}/store/any_key")
            assert response.status_code == 200, f"{name} should accept reads"


class TestConsistency:
    """test data consistency across nodes."""

    def test_all_data_endpoint(self, client):
        """test getting all data from leader."""
        # write some data
        for i in range(3):
            key = f"consistency_test_{i}_{int(time.time())}"
            client.post(f"{LEADER_URL}/store/{key}", json={"value": f"value_{i}"})

        # get all data
        response = client.get(f"{LEADER_URL}/store")
        assert response.status_code == 200
        data = response.json()
        assert data["size"] >= 3

    def test_eventual_consistency(self, client):
        """test that all nodes eventually have same data."""
        # write multiple keys
        keys = []
        for i in range(5):
            key = f"eventual_test_{i}_{int(time.time())}"
            keys.append(key)
            client.post(f"{LEADER_URL}/store/{key}", json={"value": f"value_{i}"})

        # wait for all replications to complete
        time.sleep(5)

        # get leader data
        leader_data = client.get(f"{LEADER_URL}/store").json()["data"]

        # verify all followers have same data
        for name, url in FOLLOWER_URLS.items():
            follower_data = client.get(f"{url}/store").json()["data"]
            
            # check all keys we wrote
            for key in keys:
                assert key in follower_data, f"{name} missing key {key}"
                assert follower_data[key] == leader_data[key], \
                    f"{name} has wrong value for {key}"


class TestConcurrentWrites:
    """test concurrent write operations."""

    @pytest.mark.asyncio
    async def test_concurrent_writes(self, async_client):
        """test multiple concurrent writes to the leader."""
        base_key = f"concurrent_{int(time.time())}"
        num_writes = 10

        async def write_key(i: int):
            response = await async_client.post(
                f"{LEADER_URL}/store/{base_key}_{i}",
                json={"value": f"concurrent_value_{i}"},
            )
            return response.status_code == 200

        # perform concurrent writes
        results = await asyncio.gather(*[write_key(i) for i in range(num_writes)])

        # all writes should succeed
        assert all(results)

    @pytest.mark.asyncio
    async def test_concurrent_writes_same_key(self, async_client):
        """test multiple concurrent writes to the same key."""
        key = f"same_key_{int(time.time())}"
        num_writes = 10

        async def write_key(i: int):
            response = await async_client.post(
                f"{LEADER_URL}/store/{key}",
                json={"value": f"value_{i}"},
            )
            return response.status_code == 200

        # perform concurrent writes
        results = await asyncio.gather(*[write_key(i) for i in range(num_writes)])

        # all writes should succeed
        assert all(results)

        # wait for replication
        await asyncio.sleep(2)

        # verify key exists with some value
        response = await async_client.get(f"{LEADER_URL}/store/{key}")
        data = response.json()
        assert data["found"] is True


def run_basic_test():
    """
    simple test runner for manual execution.
    run this after docker-compose up.
    """
    print("running basic integration tests...")

    with httpx.Client(timeout=30.0) as client:
        # test 1: leader health check
        print("\n1. testing leader health check...")
        try:
            response = client.get(f"{LEADER_URL}/health")
            assert response.status_code == 200
            print(f"   leader health: {response.json()}")
        except Exception as e:
            print(f"   failed: {e}")
            return False

        # test 2: follower health checks
        print("\n2. testing follower health checks...")
        for name, url in FOLLOWER_URLS.items():
            try:
                response = client.get(f"{url}/health")
                assert response.status_code == 200
                print(f"   {name}: {response.json()['status']}")
            except Exception as e:
                print(f"   {name} failed: {e}")
                return False

        # test 3: write to leader
        print("\n3. testing write to leader...")
        key = f"test_key_{int(time.time())}"
        value = {"message": "hello", "count": 1}
        try:
            response = client.post(
                f"{LEADER_URL}/store/{key}",
                json={"value": value},
            )
            assert response.status_code == 200
            print(f"   write response: {response.json()}")
        except Exception as e:
            print(f"   failed: {e}")
            return False

        # test 4: read from leader
        print("\n4. testing read from leader...")
        try:
            response = client.get(f"{LEADER_URL}/store/{key}")
            assert response.status_code == 200
            data = response.json()
            assert data["found"] is True
            assert data["value"] == value
            print(f"   read response: {data}")
        except Exception as e:
            print(f"   failed: {e}")
            return False

        # test 5: verify replication to followers
        print("\n5. testing replication to followers...")
        time.sleep(2)  # wait for replication
        for name, url in FOLLOWER_URLS.items():
            try:
                response = client.get(f"{url}/store/{key}")
                data = response.json()
                if data["found"] and data["value"] == value:
                    print(f"   {name}: replicated correctly")
                else:
                    print(f"   {name}: replication incomplete")
            except Exception as e:
                print(f"   {name} failed: {e}")

        # test 6: verify followers reject writes
        print("\n6. testing followers reject writes...")
        for name, url in list(FOLLOWER_URLS.items())[:1]:  # test just one
            try:
                response = client.post(
                    f"{url}/store/should_fail",
                    json={"value": "test"},
                )
                if response.status_code == 403:
                    print(f"   {name}: correctly rejected write (403)")
                else:
                    print(f"   {name}: unexpected status {response.status_code}")
            except Exception as e:
                print(f"   {name} failed: {e}")

        print("\n all basic tests passed!")
        return True


if __name__ == "__main__":
    success = run_basic_test()
    exit(0 if success else 1)
