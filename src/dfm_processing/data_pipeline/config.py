"""This module contains a pydantic class to hold config options."""

from pathlib import Path
from typing import Literal
from pydantic import BaseModel, Field
import yaml


def load_yml_config(path: Path):
    """Classmethod returns YAML config"""
    try:
        return yaml.safe_load(path.read_text())

    except FileNotFoundError as error:
        message = "Error: yml config file not found."
        # logger.exception(message)
        raise FileNotFoundError(error, message) from error


class Dataset(BaseModel):
    """Dataset config object containing various info about a specific dataset."""

    name: str = Field(help="Name of the dataset to be processed")
    input_dir: str = Field(help="Path to the directory keeping the raw data")
    glob_pattern: str | None = Field(
        None, help="Glob pattern to use to select a subset of files in `input_dir`"
    )
    output_dir: str = Field(help="Path to the directory to store the processed data")
    exclusion_dir: str = Field(help="Path to the directory to store excluded data")
    logging_dir: str = Field(help="Path to the directory to save logs to")
    debug: bool = Field(False, help="Print debug messages from the datasets")


class ExecutorConfig(BaseModel):
    """Executor config object, holding various properties of an executor."""

    n_workers: int = Field(1, help="Number of workers to process the data in parallel")
    n_tasks: int = Field(1, help="Number of tasks to divide the data into")
    debug: bool = Field(False, help="Print debug message for the executor")


class ClusterConfig(BaseModel):
    """Dask Cluster configurations."""

    type: Literal["local", "distributed"] = Field(
        "local", help="Whether to run the cluster locally or in a distributed setting."
    )  # distributed
    scheduler_host: str | None = Field("localhost", help="")
    scheduler_port: int | None = Field(8786, help="")
    scheduler_file: str | None = Field(
        None, help="Path to a scheduler file to connect to cluster"
    )
    n_workers: int = Field(5, help="")
    worker_threads: int = Field(
        3, help="Number of worker threads. I.e. number of parallel tasks per worker."
    )


class SentenceDeduplication(BaseModel):
    """Sentence Deduplication configuration."""

    input_dir: str = Field("output/", help="")
    gllob_pattern: str = Field("**/*.parquet", help="")
    dedup_dir: str = Field("dedup/")
    exclusion_dir: str = Field("exclusions/sent_deup")
    output_dir: str = Field("sent_dedup/")
    logging_dir: str = Field("logs/sent_dedup")


class MinHashDeduplication(BaseModel):
    """MinHash Deduplication configuration."""

    input_dir: str = Field("sent_dedup/", help="")
    gllob_pattern: str = Field("**/*.parquet", help="")
    dedup_dir: str = Field("dedup/")
    exclusion_dir: str = Field("exclusions/minhash_dedup")
    output_dir: str = Field("minhash_dedup/")
    logging_dir: str = Field("logs/sent_dedup")


class PipelineConfig(BaseModel):
    """Config object to hold the various configuration options. Enables loading from yaml file."""

    datasets: list[Dataset] = Field(
        help="Name and path to datasets to be processed",
    )

    executor: ExecutorConfig = Field(help="Settings for the executor of the pipeline")

    sentence_deduplication: SentenceDeduplication = Field(
        help="Settings for sentence deduplication."
    )
    minhash_deduplication: MinHashDeduplication = Field(
        help="Settings for minhash deduplication."
    )

    cluster: ClusterConfig = Field(help="Configurations for the dask cluster")
