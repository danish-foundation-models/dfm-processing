import pytest
from unittest.mock import create_autospec
from dask.distributed import Client
from distributed import Future

from dfm_processing.data_pipeline.config import ClusterConfig
from dfm_processing.data_pipeline.cluster import (
    create_client,
    submit_job,
)


@pytest.fixture
def mock_client(mocker):
    """Fixture providing a mock Dask Client"""
    mock = create_autospec(Client)
    mocker.patch("dfm_processing.data_pipeline.cluster.Client", new=mock)
    return mock


@pytest.fixture
def cluster_config():
    """Fixture providing base cluster configuration"""
    return ClusterConfig(
        type="distributed",
        scheduler_host="localhost",
        scheduler_port=8786,
        scheduler_file=None,
        n_workers=3,
    )


@pytest.mark.parametrize(
    "config_kwargs, expected",
    [
        # Test case 1: Local cluster: expect a LocalCluster with the specified number of workers.
        (
            {"n_workers": 3, "scheduler_file": None, "type": "local"},
            {"branch": "local", "n_workers": 3},
        ),
        # Test case 2: Distributed with scheduler_file: expect Client(scheduler_file=...)
        (
            {
                "n_workers": 3,
                "scheduler_file": "/path/to/scheduler.json",
                "type": "distributed",
            },
            {"branch": "distributed", "scheduler_file": "/path/to/scheduler.json"},
        ),
        # Test case 3: Distributed without scheduler_file: expect Client(address=...)
        (
            {},  # using ClusterConfig() defaults; assume default type is "distributed",
            {"branch": "local", "address": "localhost:8786", "n_workers": 5},
        ),
    ],
)
def test_create_client_parametrized(mock_client, config_kwargs, expected):
    """
    Test create_client using parameterized configuration.

    For a local configuration, it verifies that Client was created with a LocalCluster
    with the expected number of workers. For a distributed configuration, it checks that
    the correct keyword argument (scheduler_file or address) was passed to Client.
    """
    if config_kwargs:
        # Provide some standard parameters along with overrides.
        config = ClusterConfig(
            scheduler_host="localhost",
            scheduler_port=8786,
            **config_kwargs,
        )
    else:
        # When no parameters are passed, rely on the defaults defined in ClusterConfig.
        config = ClusterConfig()

    client = create_client(config)

    if expected["branch"] == "local":
        # For a local cluster, create_client passes a LocalCluster instance as the first argument.
        cluster_arg = mock_client.call_args[0][0]
        from dask.distributed import LocalCluster

        assert isinstance(cluster_arg, LocalCluster)
        assert len(cluster_arg.workers) == expected["n_workers"]
    elif expected["branch"] == "distributed":
        # For distributed clusters, we expect either a scheduler_file or an address.
        if "scheduler_file" in expected:
            mock_client.assert_called_once_with(
                scheduler_file=expected["scheduler_file"]
            )
        elif "address" in expected:
            mock_client.assert_called_once_with(address=expected["address"])
    else:
        pytest.fail("Unexpected configuration branch.")

    # Also assert that the returned client is indeed an instance of Client.
    from dask.distributed import Client

    assert isinstance(client, Client)


# Tests for submit_job
def test_submit_job_with_arguments(mock_client):
    mock_future = create_autospec(Future)
    mock_client.submit.return_value = mock_future

    def test_job(a, b, c):
        return a + b + c

    future = submit_job(mock_client, test_job, 1, 2, 3)

    # Verify submission parameters
    mock_client.submit.assert_called_once_with(test_job, 1, 2, 3)
    assert future == mock_future
    assert isinstance(future, Future)


def test_submit_job_no_arguments(mock_client):
    mock_future = create_autospec(Future)
    mock_client.submit.return_value = mock_future

    def test_job():
        return 42

    future = submit_job(mock_client, test_job)

    mock_client.submit.assert_called_once_with(test_job)
    assert future == mock_future


def test_submit_job_with_keyword_arguments(mock_client):
    mock_future = create_autospec(Future)
    mock_client.submit.return_value = mock_future

    def test_job(a, b=0):
        return a + b

    # Note: The current implementation doesn't support kwargs, so they should fail
    with pytest.raises(TypeError):
        submit_job(mock_client, test_job, 1, b=2)  # type: ignore


def test_submit_job_error_handling(mock_client):
    mock_client.submit.side_effect = RuntimeError("Cluster disconnected")

    with pytest.raises(RuntimeError):
        submit_job(mock_client, print, "test")


# Edge case tests
def test_submit_non_callable(mock_client):
    # Configure the mock to simulate Dask's behavior
    mock_client.submit.side_effect = TypeError("First argument must be callable")

    with pytest.raises(TypeError):
        submit_job(mock_client, "not_a_function")  # type: ignore


def test_client_closed_before_submit(mock_client):
    mock_client.submit.side_effect = ValueError("Client closed")

    with pytest.raises(ValueError):
        submit_job(mock_client, print, "test")
