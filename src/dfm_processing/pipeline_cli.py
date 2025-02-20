"""Module containing methods for the CLI regarding the data pipeline."""

from pathlib import Path

from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.base import PipelineStep
from datatrove.utils.stats import PipelineStats
from datatrove.utils.logging import logger
from distributed import Client, Future
import typer

from dfm_processing.data_pipeline.config import ExecutorConfig


from .data_pipeline.pipeline import filter_pipeline, build_executor, sent_dedup
from .data_pipeline.cluster import create_client, submit_job
from .data_pipeline.config import PipelineConfig, load_yml_config, ClusterConfig
from .data_pipeline.utils import print_pipeline


app = typer.Typer()


@app.command(
    name="run",
    help="Run the data processing pipeline on a defined set of data",
)
def run(
    config_file: Path,
):
    """CLI method for running a filtering + sentence deduplication pipeline on multiple datasets

    Args:
        config_file: Path to a config file.
    """
    config = PipelineConfig(**load_yml_config(config_file))
    executor_config: ExecutorConfig = config.executor

    if not isinstance(config.datasets, list):
        typer.Exit(code=1)

    executors: list[LocalPipelineExecutor] = []

    for dataset in config.datasets:
        name = dataset.name
        steps: list[PipelineStep] = filter_pipeline(dataset=dataset)

        dedup_sigs: list[PipelineStep] = []
        find_dedups: list[PipelineStep] = []
        filter_dedup: list[PipelineStep] = []
        if config.sent_dedup:
            dedup_sigs, find_dedups, filter_dedup = sent_dedup(
                dedup_dir=f"{config.dedup_dir}/{name}/",
                filter_dir=f"{dataset.output_dir}/filter_output",
                output_dir=dataset.output_dir,
                exclusion_dir=dataset.exclusion_dir,
                n_workers=executor_config.n_tasks,
            )

        executor: LocalPipelineExecutor = build_executor(
            steps + dedup_sigs,
            logging_dir=f"{dataset.logging_dir}/filter",
            config=executor_config,
        )

        if config.sent_dedup:
            # Step 2
            executor = build_executor(
                find_dedups,
                logging_dir=f"{dataset.logging_dir}/find_dedups",
                config=executor_config,
                depends=executor,
            )
            executor.workers = 1  # NOTE: Not a pretty way of doing this.

            # Step 3
            executor = build_executor(
                filter_dedup,
                logging_dir=f"{dataset.logging_dir}/filter_dedups",
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
