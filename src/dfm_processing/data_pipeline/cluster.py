"""Module containing methods for creating and managing a cluster of workers."""

from typing import Callable
from dask.distributed import Client


from dfm_processing.data_pipeline.config import ClusterConfig


def create_client(config: ClusterConfig) -> Client:
    client = Client(n_workers=config.n_workers, scheduler_file=config.scheduler_file)
    return client


def submit_job(client: Client, job: Callable):
    pass
