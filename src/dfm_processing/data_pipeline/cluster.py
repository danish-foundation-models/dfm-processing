"""Module containing methods for creating and managing a cluster of workers."""

from typing import Callable
from dask.distributed import Client, LocalCluster
from distributed import Future


from dfm_processing.data_pipeline.config import ClusterConfig


def create_client(config: ClusterConfig) -> Client:
    """Build a Dask Cluster Client based on desired configuration.

    Args:
        config: A configuration object defining the Dask cluster.

    Raises:
        ValueError: Something is wrong with the configuration.

    Returns:
        The Dask Client used to submit jobs.
    """
    if config.type == "local" and config.n_workers:
        cluster = LocalCluster(
            name="LocalCluster DFM",
            n_workers=config.n_workers,
            threads_per_worker=config.worker_threads,
            host="localhost",
        )
        client = Client(cluster)
    elif config.type == "distributed" and config.scheduler_file:
        client = Client(scheduler_file=config.scheduler_file)
    elif config.type == "distributed" and not config.scheduler_file:
        client = Client(address=f"{config.scheduler_host}:{config.scheduler_port}")
    else:
        raise ValueError(f"Something in your configuration is wrong: {config}")
    return client


def submit_job(client: Client, job: Callable, *args):
    future: Future = client.submit(job, *args)
    return future
