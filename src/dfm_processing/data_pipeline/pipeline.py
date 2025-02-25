"""Module containing methods for the data pipeline."""

from datatrove.pipeline.base import PipelineStep
from datatrove.pipeline.filters import (
    LanguageFilter,
    GopherRepetitionFilter,
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
)
from datatrove.pipeline.dedup import (
    SentenceFindDedups,
    SentenceDedupSignature,
    SentDedupConfig,
    SentenceDedupFilter,
)
from datatrove.pipeline.readers import JsonlReader, ParquetReader
from datatrove.pipeline.writers import ParquetWriter
from datatrove.pipeline.formatters import FTFYFormatter
from datatrove.pipeline.tokens import TokensCounter
from datatrove.utils.typeshelper import Languages

from datatrove.executor.local import LocalPipelineExecutor

from dfm_processing.data_pipeline.config import ExecutorConfig, Dataset


def filter_pipeline(dataset: Dataset) -> list[PipelineStep]:
    """Method for building up a set of filtering steps for a datatrove pipeline.

    Args:
        input_data: Path to the directory containing the input data
        output_dir: Path to the directory to store the filtered data
        exclusion_dir: Path to the directory to save the excluded data

    Returns:
        A list of pipeline steps to use in a datatrove pipeline
    """
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
            exclusion_writer=ParquetWriter(
                f"{dataset.exclusion_dir}/non_danish_documents"
            ),
            label_only=False,
        ),
        GopherRepetitionFilter(
            language=Languages.danish,
            exclusion_writer=ParquetWriter(
                f"{dataset.exclusion_dir}/gopher_repetition"
            ),
        ),
        GopherQualityFilter(
            exclusion_writer=ParquetWriter(f"{dataset.exclusion_dir}/gopher_quality")
        ),
        C4QualityFilter(
            exclusion_writer=ParquetWriter(f"{dataset.exclusion_dir}/c4_quality")
        ),
        FineWebQualityFilter(
            exclusion_writer=ParquetWriter(f"{dataset.exclusion_dir}/fineweb_quality")
        ),
        TokensCounter(),
    ]
    writer = ParquetWriter(f"{dataset.output_dir}/filter_output")

    return [reader] + filter_steps + [writer]


def sent_dedup(
    dedup_dir: str,
    filter_dir: str,
    output_dir: str,
    exclusion_dir: str,
    n_workers: int = 5,
) -> tuple[list[PipelineStep], list[PipelineStep], list[PipelineStep]]:
    """Method for creating the three pipelines that are needed for doing sentence deduplication

    Args:
        dedup_dir: Path to directory to store deduplication artifacts
        filter_dir: Path to directory containing input data
        output_dir: Path to directory to store deduplicated data
        exclusion_dir: Path to directory to store excluded data
        n_workers: Number of finder workers. Defaults to 5.

    Returns:
        tuple[list[PipelineStep], list[PipelineStep], list[PipelineStep]]: The three pipeline for sentence deduplication.
    """
    config = SentDedupConfig(
        n_sentences=3,
        split_sentences=False,  # set to False to split on \n instead of sentences
        only_dedup_in_index=True,
        min_doc_words=50,  # minimum number of words to keep document after dedup
        min_num_sentences=1,  # minimum number of sentences to keep document after dedup
    )
    dedup_sigs = [
        SentenceDedupSignature(
            output_folder=f"{dedup_dir}/sigs",
            config=config,
            finder_workers=n_workers,
            language=Languages.danish,
        ),
    ]

    find_dedups = [
        SentenceFindDedups(
            data_folder=f"{dedup_dir}/sigs",
            output_folder=f"{dedup_dir}/dups",
            config=config,
        )
    ]

    filter_dedup = [
        ParquetReader(data_folder=filter_dir),
        SentenceDedupFilter(
            data_folder=f"{dedup_dir}/dups",
            exclusion_writer=ParquetWriter(f"{exclusion_dir}/sent_dedup/"),
        ),
        ParquetWriter(f"{output_dir}/sent_dedup_output"),
    ]

    return dedup_sigs, find_dedups, filter_dedup


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
