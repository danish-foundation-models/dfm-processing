import pytest

# Import functions to be tested.
from dfm_processing.data_pipeline.pipeline import (
    filter_pipeline,
    build_executor,
)
from dfm_processing.data_pipeline.config import ExecutorConfig, Dataset
from dfm_processing.data_pipeline.deduplication import sentence_deduplication

# Import pipeline step classes for type-checking and attribute inspection.
from datatrove.pipeline.readers.base import BaseDiskReader
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.pipeline.dedup import (
    SentenceFindDedups,
    SentenceDedupSignature,
    SentenceDedupFilter,
    SentDedupConfig,
)
from datatrove.pipeline.base import PipelineStep
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.utils.typeshelper import Languages


# ===========================
# Tests for filter_pipeline
# ===========================


@pytest.fixture
def dataset():
    dataset = {
        "name": "test",
        "input_dir": "/input",
        "output_dir": "/output",
        "exclusion_dir": "/exclude",
        "logging_dir": "/logs",
    }
    return Dataset(**dataset)


def test_filter_pipeline(dataset: Dataset):
    """
    Test that the filter_pipeline function returns the correct sequence of pipeline steps
    with the expected configuration.
    """

    pipeline = filter_pipeline(dataset)
    # Expected 4 steps: [JsonlReader, LanguageFilter, GopherRepetitionFilter, ParquetWriter]
    assert isinstance(pipeline, list)
    assert len(pipeline) >= 3  # Should have atleast a reader, a writer and one filter.

    # Step 1: JsonlReader.
    reader = pipeline[0]
    assert isinstance(reader, BaseDiskReader)
    # Assume the reader stores the input path in an attribute called "data_folder" or "path".
    assert reader.data_folder.path == dataset.input_dir

    assert all([isinstance(step, PipelineStep) for step in pipeline])

    # Step 2: ParquetWriter (for the output).
    writer = pipeline[-1]
    assert isinstance(writer, DiskWriter)
    writer_path = writer.output_folder.path
    assert writer_path == f"{dataset.output_dir}/filter_output"


def test_filter_pipeline_empty_paths(dataset: Dataset):
    """
    Edge case: Test that filter_pipeline handles empty strings for paths.
    """
    dataset.input_dir = ""
    dataset.output_dir = ""
    dataset.exclusion_dir = ""

    with pytest.raises(ValueError):
        filter_pipeline(dataset)


def test_filter_pipeline_empty_glob_pattern(dataset: Dataset):
    """
    Edge case: Test that filter_pipeline handles empty strings for paths.
    """
    dataset.glob_pattern = ""

    with pytest.raises(ValueError):
        filter_pipeline(dataset)


# ===========================
# Tests for sent_dedup
# ===========================


def test_sent_dedup():
    """
    Test that the sent_dedup function creates the three expected pipelines
    (dedup signatures, find dedups, filter dedup) with the correct configuration.
    """
    dedup_dir = "/dedup"
    filter_dir = "/filter"
    output_dir = "/output"
    exclusion_dir = "/exclusion"
    n_workers = 7

    dedup_sigs, find_dedups, filter_dedup = sentence_deduplication(
        filter_dir, dedup_dir, output_dir, exclusion_dir, n_workers
    )

    # Validate dedup_sigs (list of SentenceDedupSignature).
    assert isinstance(dedup_sigs, list)
    assert len(dedup_sigs) == 2
    sig = dedup_sigs[-1]
    assert isinstance(sig, SentenceDedupSignature)
    assert sig.output_folder.path == f"{dedup_dir}/sigs"
    assert sig.finder_workers == n_workers
    assert sig.language == Languages.danish

    # Check the configuration inside the signature.
    config = sig.config
    assert isinstance(config, SentDedupConfig)

    # Validate find_dedups (list of SentenceFindDedups).
    assert isinstance(find_dedups, list)
    assert len(find_dedups) == 1
    find_dup = find_dedups[0]
    assert isinstance(find_dup, SentenceFindDedups)
    assert find_dup.data_folder.path == f"{dedup_dir}/sigs"
    assert find_dup.output_folder.path == f"{dedup_dir}/dups"
    # Ensure the same configuration object is used.
    assert find_dup.config == config

    # Validate filter_dedup (list of 3 steps: ParquetReader, SentenceDedupFilter, ParquetWriter).
    assert isinstance(filter_dedup, list)
    assert len(filter_dedup) == 3

    # Step 1: ParquetReader.
    reader = filter_dedup[0]
    assert isinstance(reader, BaseDiskReader)
    assert reader.data_folder.path == filter_dir

    # Step 2: SentenceDedupFilter.
    dedup_filter = filter_dedup[1]
    assert isinstance(dedup_filter, SentenceDedupFilter)
    assert dedup_filter.data_folder.path == f"{dedup_dir}/dups"
    exclusion_writer = dedup_filter.exclusion_writer
    assert isinstance(exclusion_writer, DiskWriter)
    assert exclusion_writer.output_folder.path == f"{exclusion_dir}/sent_dedup"

    # Step 3: ParquetWriter.
    writer = filter_dedup[2]
    assert isinstance(writer, DiskWriter)
    assert writer.output_folder.path == f"{output_dir}/sent_dedup_output"


def test_sent_dedup_negative_workers():
    """
    Edge case: Test that sent_dedup handles negative n_workers (even if nonsensical)
    by simply passing the value along. Should throw a ValueError.
    """
    dedup_dir = "/dedup"
    filter_dir = "/filter"
    output_dir = "/output"
    exclusion_dir = "/exclusion"
    n_workers = -3  # Negative value

    with pytest.raises(ValueError):
        sentence_deduplication(
            dedup_dir, filter_dir, output_dir, exclusion_dir, n_workers
        )


# ===========================
# Tests for build_executor
# ===========================


def test_build_executor():
    """
    Test that build_executor correctly instantiates a LocalPipelineExecutor
    with the specified pipeline, logging directory, and configuration.
    """
    dummy_pipeline = ["step1", "step2"]
    logging_dir = "/logs"
    config = ExecutorConfig(n_tasks=10, n_workers=5)

    # Test without a dependency executor.
    executor = build_executor(dummy_pipeline, logging_dir, config)
    assert isinstance(executor, LocalPipelineExecutor)
    assert executor.pipeline == dummy_pipeline
    assert executor.logging_dir.path == logging_dir
    assert executor.tasks == config.n_tasks
    assert executor.workers == config.n_workers
    assert executor.depends is None

    # Test with a dependency executor.
    dummy_depends = LocalPipelineExecutor(
        pipeline=["dep_step"], logging_dir="/dep_logs", tasks=1, workers=1, depends=None
    )
    executor_with_dep = build_executor(
        dummy_pipeline, logging_dir, config, depends=dummy_depends
    )
    assert executor_with_dep.depends == dummy_depends


def test_build_executor_empty_pipeline():
    """
    Edge case: Test that build_executor handles an empty pipeline list.
    """
    empty_pipeline: list[PipelineStep] = []
    logging_dir: str = "/logs"
    config = ExecutorConfig(n_tasks=10, n_workers=5)

    executor = build_executor(empty_pipeline, logging_dir, config)
    assert isinstance(executor, LocalPipelineExecutor)
    assert executor.pipeline == empty_pipeline
