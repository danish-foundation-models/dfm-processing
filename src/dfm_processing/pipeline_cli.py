"""Module containing methods for the CLI regarding the data pipeline."""

from pathlib import Path

from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.base import PipelineStep
from datatrove.utils.stats import PipelineStats
from datatrove.utils.logging import logger
from distributed import Client, Future
import typer

from dfm_processing.data_pipeline.config import MinHashDeduplication

from .data_pipeline.pipeline import filter_pipeline, build_executor
from .data_pipeline.deduplication import sentence_deduplication, minhash_deduplication
from .data_pipeline.cluster import create_client, submit_job
from .data_pipeline.config import (
    PipelineConfig,
    load_yml_config,
    ClusterConfig,
    ExecutorConfig,
    SentenceDeduplication,
)
from .data_pipeline.utils import print_pipeline

app = typer.Typer()

# TODO: Create a decorator for standard parts of the cli commands (e.g. loading config?s )


@app.command(
    name="filter",
    help="Run the filter pipeline on a defined set of data",
)
def filter_pipe(
    config_file: Path,
):
    """CLI method for running a filtering pipeline on multiple datasets

    Args:
        config_file: Path to a config file.
    """
    config = PipelineConfig(**load_yml_config(config_file))
    executor_config: ExecutorConfig = config.executor

    if not isinstance(config.datasets, list):
        typer.Exit(code=1)

    executors: list[LocalPipelineExecutor] = []

    for dataset in config.datasets:
        steps: list[PipelineStep] = filter_pipeline(dataset=dataset)

        executor: LocalPipelineExecutor = build_executor(
            steps,
            logging_dir=f"{dataset.logging_dir}/filter",
            config=executor_config,
        )

        executors.append(executor)

    if executor_config.debug:
        for executor in executors:
            print_pipeline(executor)

    cluster_config: ClusterConfig = config.cluster
    client: Client = create_client(cluster_config)
    futures: list[Future] = []
    for executor in executors:
        futures.append(submit_job(client, executor.run))

    stats: list[PipelineStats] = client.gather(futures)
    # merged stats
    stats = list(filter(lambda x: x, stats))
    if len(stats) > 0:
        stat: PipelineStats = sum(stats, start=PipelineStats())
        logger.success(stat.get_repr("All tasks"))
    else:
        logger.success("Nothing to do.")


@app.command(
    name="sent_dedup", help="Run sentence deduplication on a defined set of data."
)
def sent_dedup(config_file: Path):
    """CLI method for running a sentence deduplication pipeline on multiple datasets

    Args:
        config_file: Path to a config file.
    """
    config = PipelineConfig(**load_yml_config(config_file))
    executor_config: ExecutorConfig = config.executor
    dedup_config: SentenceDeduplication = config.sentence_deduplication

    if not isinstance(config.datasets, list):
        typer.Exit(code=1)

    executors: list[LocalPipelineExecutor] = []

    dedup_sigs, find_dedups, filter_dedup = sentence_deduplication(
        dedup_dir=dedup_config.dedup_dir,
        data_dir=dedup_config.input_dir,
        output_dir=dedup_config.output_dir,
        exclusion_dir=dedup_config.exclusion_dir,
        n_workers=executor_config.n_tasks,
    )

    # Step 1
    executor: LocalPipelineExecutor = build_executor(
        dedup_sigs,
        logging_dir=f"{dedup_config.logging_dir}/dedup_sigs",
        config=executor_config,
    )

    # Step 2
    executor = build_executor(
        find_dedups,
        logging_dir=f"{dedup_config.logging_dir}/find_dedups",
        config=executor_config,
        depends=executor,
    )
    executor.workers = 1  # NOTE: Not a pretty way of doing this.

    # Step 3
    executor = build_executor(
        filter_dedup,
        logging_dir=f"{dedup_config.logging_dir}/filter_dedups",
        config=executor_config,
        depends=executor,
    )

    executors.append(executor)

    if executor_config.debug:
        for executor in executors:
            print_pipeline(executor)

    cluster_config: ClusterConfig = config.cluster
    client: Client = create_client(cluster_config)
    futures: list[Future] = []
    for executor in executors:
        futures.append(submit_job(client, executor.run))

    stats: list[PipelineStats] = client.gather(futures)
    # merged stats
    stats = list(filter(lambda x: x, stats))
    if len(stats) > 0:
        stat: PipelineStats = sum(stats, start=PipelineStats())
        logger.success(stat.get_repr("All tasks"))
    else:
        logger.success("Nothing to do.")


@app.command(
    name="minhash_dedup", help="Run minhash deduplication on a large set of data."
)
def minhash_dedup(config_file: Path):
    """CLI method for running a MinHash deduplication pipeline across multiple datasets.

    Args:
        config_file: Path to a config file.
    """
    config = PipelineConfig(**load_yml_config(config_file))
    executor_config: ExecutorConfig = config.executor
    dedup_config: MinHashDeduplication = config.minhash_deduplication

    if not isinstance(config.datasets, list):
        typer.Exit(code=1)

    if executor_config.n_tasks % dedup_config.n_buckets != 0:
        logger.error("Number of tasks should be divisible by the number of buckets.")
        typer.Exit(code=1)

    dedup_sigs, dedup_buckets, dedup_cluster, dedup_filter = minhash_deduplication(
        dedup_dir=dedup_config.dedup_dir,
        data_dir=dedup_config.input_dir,
        output_dir=dedup_config.output_dir,
        exclusion_dir=dedup_config.exclusion_dir,
        n_buckets=dedup_config.n_buckets,
    )

    # Step 1
    executor: LocalPipelineExecutor = build_executor(
        dedup_sigs,
        logging_dir=f"{dedup_config.logging_dir}/dedup_sigs",
        config=executor_config,
    )

    # Step 2
    executor = build_executor(
        dedup_buckets,
        logging_dir=f"{dedup_config.logging_dir}/dedup_buckets",
        config=executor_config,
        depends=executor,
    )
    executor.tasks = (
        1  # dedup_config.n_buckets  # NOTE: Not a pretty way of doing this.
    )

    # Step 3
    executor = build_executor(
        dedup_cluster,
        logging_dir=f"{dedup_config.logging_dir}/dedup_cluster",
        config=executor_config,
        depends=executor,
    )
    executor.tasks = 1

    # Step 4
    executor = build_executor(
        dedup_filter,
        logging_dir=f"{dedup_config.logging_dir}/dedup_filter",
        config=executor_config,
        depends=executor,
    )

    if executor_config.debug:
        print_pipeline(executor)

    cluster_config: ClusterConfig = config.cluster
    client: Client = create_client(cluster_config)
    future = submit_job(client, executor.run)

    stats: list[PipelineStats] = client.gather([future])
    # merged stats
    stats = list(filter(lambda x: x, stats))
    if len(stats) > 0:
        stat: PipelineStats = sum(stats, start=PipelineStats())
        logger.success(stat.get_repr("All tasks"))
    else:
        logger.success("Nothing to do.")
