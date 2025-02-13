"""Module containing methods for the CLI regarding the data pipeline."""

from pathlib import Path

from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.base import PipelineStep
import typer


from .data_pipeline.pipeline import filter_pipeline, build_executor, sent_dedup
from .data_pipeline.config import PipelineConfig, load_yml_config


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
    executor_config = config.executor

    if not isinstance(config.datasets, list):
        typer.Exit(code=1)

    executors: list[LocalPipelineExecutor] = []

    for dataset in config.datasets:
        name = dataset.name
        steps: list[PipelineStep] = filter_pipeline(
            input_data=dataset.input_dir,
            output_dir=dataset.output_dir,
            exclusion_dir=dataset.exclusion_dir,
        )

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
            steps + dedup_sigs, logging_dir=dataset.logging_dir, config=executor_config
        )

        if config.sent_dedup:
            # Step 2
            executor = build_executor(
                find_dedups,
                logging_dir=dataset.logging_dir,
                config=executor_config,
                depends=executor,
            )
            executor.workers = 1  # NOTE: Not a pretty way of doing this.

            # Step 3
            executor = build_executor(
                filter_dedup,
                logging_dir=dataset.logging_dir,
                config=executor_config,
                depends=executor,
            )

        executors.append(executor)

    print([executor.pipeline for executor in executors])
