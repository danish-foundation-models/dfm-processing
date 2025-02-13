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
    name: str = Field(help="Name of the dataset to be processed")
    input_dir: str = Field(help="Path to the directory keeping the raw data")
    output_dir: str = Field(help="Path to the directory to store the processed data")
    exclusion_dir: str = Field(help="Path to the directory to store excluded data")
    logging_dir: str = Field(help="Path to the directory to save logs to")


class ExecutorConfig(BaseModel):
    type: Literal["local", "dask"] = Field()
    n_workers: int = Field(1, help="Number of workers to process the data in parallel")
    n_tasks: int = Field(1, help="Number of tasks to divide the data into")


class PipelineConfig(BaseModel):
    """Config object to hold the various configuration options. Enables loading from yaml file."""

    datasets: list[Dataset] = Field(
        help="Name and path to datasets to be processed",
    )

    executor: ExecutorConfig = Field(help="Settings for the executor of the pipeline")

    sent_dedup: bool = Field(False, help="Whether or not to run sentence deduplication")
    dedup_dir: str = Field(help="Directory to save dedup signatures, etc.")
