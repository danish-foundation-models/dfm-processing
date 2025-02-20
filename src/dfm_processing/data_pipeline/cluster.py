"""Module containing methods for creating and managing a cluster of workers."""

from typing import Callable
from dask.distributed import Client
from distributed import Future


from dfm_processing.data_pipeline.config import ClusterConfig


def create_client(config: ClusterConfig) -> Client:
    if config.type == "local" and config.n_workers:
        client = Client(n_workers=config.n_workers)
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
