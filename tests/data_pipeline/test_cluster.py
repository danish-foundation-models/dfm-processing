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
    "config_kwargs, expected_kwargs",
    [
        # Test case 1: Local cluster (explicit scheduler_file=None, n_workers=3)
        (
            {"n_workers": 3, "scheduler_file": None},
            {"n_workers": 3, "scheduler_file": None},
        ),
        # Test case 2: Connecting to an existing cluster (non-None scheduler_file)
        (
            {"n_workers": 3, "scheduler_file": "/path/to/scheduler.json"},
            {"n_workers": 3, "scheduler_file": "/path/to/scheduler.json"},
        ),
        # Test case 3: Using default configuration (i.e. no parameters passed; ClusterConfig() uses defaults)
        # Note: Assuming the default for n_workers is 5 and scheduler_file is None.
        ({}, {"n_workers": 5, "scheduler_file": None}),
    ],
)
def test_create_client_parametrized(mock_client, config_kwargs, expected_kwargs):
    """
    Test create_client using parameterized configuration.

    The test creates a ClusterConfig either by explicitly providing parameters
    or by using the default constructor. It then verifies that create_client passes
    the expected keyword arguments to the Client.
    """
    # If config_kwargs is provided, include standard values along with overrides.
    if config_kwargs:
        config = ClusterConfig(
            type="distributed",
            scheduler_host="localhost",
            scheduler_port=8786,
            **config_kwargs,
        )
    else:
        # When no overrides are provided, rely on the defaults in ClusterConfig.
        config = ClusterConfig()

    client = create_client(config)
    mock_client.assert_called_once_with(**expected_kwargs)
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
        submit_job(mock_client, test_job, 1, b=2)


def test_submit_job_error_handling(mock_client):
    mock_client.submit.side_effect = RuntimeError("Cluster disconnected")

    with pytest.raises(RuntimeError):
        submit_job(mock_client, print, "test")


# Edge case tests
def test_submit_non_callable(mock_client):
    # Configure the mock to simulate Dask's behavior
    mock_client.submit.side_effect = TypeError("First argument must be callable")

    with pytest.raises(TypeError):
        submit_job(mock_client, "not_a_function")


def test_client_closed_before_submit(mock_client):
    mock_client.submit.side_effect = ValueError("Client closed")

    with pytest.raises(ValueError):
        submit_job(mock_client, print, "test")
