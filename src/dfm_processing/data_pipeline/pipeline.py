"""Module containing methods for the data pipeline."""

from datatrove.pipeline.base import PipelineStep
from datatrove.pipeline.filters import (
    LanguageFilter,
    GopherRepetitionFilter,
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
)
from datatrove.pipeline.readers import JsonlReader
from datatrove.pipeline.formatters import FTFYFormatter
from datatrove.pipeline.tokens import TokensCounter
from datatrove.utils.typeshelper import Languages

from datatrove.executor.local import LocalPipelineExecutor
import nltk

from dfm_processing.data_pipeline.config import ExecutorConfig, Dataset
from dfm_processing.data_pipeline.components.writer import NullableParquetWriter


def filter_pipeline(dataset: Dataset) -> list[PipelineStep]:
    """Method for building up a set of filtering steps for a datatrove pipeline.

    Args:
        input_data: Path to the directory containing the input data
        output_dir: Path to the directory to store the filtered data
        exclusion_dir: Path to the directory to save the excluded data

    Returns:
        A list of pipeline steps to use in a datatrove pipeline
    """
    nltk.download("punkt_tab", quiet=True)
    nltk.download("stopwords", quiet=True)
    nltk.download("punkt", quiet=True)
    if any(
        [
            path == ""
            for path in [dataset.input_dir, dataset.exclusion_dir, dataset.output_dir]
        ]
    ):
        raise ValueError("All input paths must have a value")

    if dataset.glob_pattern == "":
        raise ValueError("Glob pattern cannot be empty.")

    reader = JsonlReader(
        data_folder=dataset.input_dir, glob_pattern=dataset.glob_pattern
    )
    filter_steps = [
        FTFYFormatter(),
        LanguageFilter(
            languages=[
                Languages.danish,
                # Languages.swedish,
                # Languages.norwegian,
                # Languages.norwegian_nynorsk,
                # Languages.english,
            ],
            exclusion_writer=NullableParquetWriter(
                f"{dataset.exclusion_dir}/non_danish_documents"
            ),
            label_only=False,
        ),
        GopherRepetitionFilter(
            language=Languages.danish,
            exclusion_writer=NullableParquetWriter(
                f"{dataset.exclusion_dir}/gopher_repetition"
            ),
        ),
        GopherQualityFilter(
            stop_words=nltk.corpus.stopwords.words("danish"),
            exclusion_writer=NullableParquetWriter(
                f"{dataset.exclusion_dir}/gopher_quality"
            ),
        ),
        C4QualityFilter(
            exclusion_writer=NullableParquetWriter(
                f"{dataset.exclusion_dir}/c4_quality"
            )
        ),
        FineWebQualityFilter(
            exclusion_writer=NullableParquetWriter(
                f"{dataset.exclusion_dir}/fineweb_quality"
            )
        ),
        TokensCounter(),
    ]
    writer = NullableParquetWriter(f"{dataset.output_dir}/filter_output")

    return [reader] + filter_steps + [writer]


def tokenization_pipeline():
    pass


def build_executor(
    pipeline: list[PipelineStep],
    logging_dir: str,
    config: ExecutorConfig,
    depends: LocalPipelineExecutor | None = None,
) -> LocalPipelineExecutor:
    """Helper function to create a pipeline executor

    Args:
        pipeline: The list os pipeline steps to run
        logging_dir: Path to the logging directory
        config: Configurations for the executor
        depends: Another pipeline executor that should run before this new one. Defaults to None.

    Returns:
        Return a local pipeline executor
    """
    executor = LocalPipelineExecutor(
        pipeline=pipeline,
        logging_dir=logging_dir,
        tasks=config.n_tasks,
        workers=config.n_workers,
        depends=depends,
    )
    return executor
