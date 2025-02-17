import pytest

# Import functions to be tested.
from dfm_processing.data_pipeline.pipeline import (
    filter_pipeline,
    sent_dedup,
    build_executor,
)
from dfm_processing.data_pipeline.config import ExecutorConfig

# Import pipeline step classes for type-checking and attribute inspection.
from datatrove.pipeline.readers import JsonlReader, ParquetReader
from datatrove.pipeline.writers import ParquetWriter
from datatrove.pipeline.filters import LanguageFilter, GopherRepetitionFilter
from datatrove.pipeline.dedup import (
    SentenceFindDedups,
    SentenceDedupSignature,
    SentenceDedupFilter,
    SentDedupConfig,
)
from datatrove.pipeline.base import PipelineStep
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.utils.typeshelper import Languages
from datatrove.utils.word_tokenizers import WordTokenizer


# ===========================
# Tests for filter_pipeline
# ===========================


def test_filter_pipeline():
    """
    Test that the filter_pipeline function returns the correct sequence of pipeline steps
    with the expected configuration.
    """
    input_data = "/input/path"
    output_dir = "/output/path"
    exclusion_dir = "/exclusion/path"

    pipeline = filter_pipeline(input_data, output_dir, exclusion_dir)
    # Expected 4 steps: [JsonlReader, LanguageFilter, GopherRepetitionFilter, ParquetWriter]
    assert isinstance(pipeline, list)
    assert len(pipeline) == 4

    # Step 1: JsonlReader.
    reader = pipeline[0]
    assert isinstance(reader, JsonlReader)
    # Assume the reader stores the input path in an attribute called "data_folder" or "path".
    assert reader.data_folder.path == input_data

    # Step 2: LanguageFilter.
    lang_filter = pipeline[1]
    assert isinstance(lang_filter, LanguageFilter)
    expected_languages = [
        Languages.danish,
        Languages.swedish,
        Languages.norwegian,
        Languages.norwegian_nynorsk,
        Languages.english,
    ]
    assert lang_filter.languages == expected_languages

    # Verify the exclusion writer in the LanguageFilter.
    exclusion_writer_lang = lang_filter.exclusion_writer
    assert isinstance(exclusion_writer_lang, ParquetWriter)
    writer_path = exclusion_writer_lang.output_folder.path
    assert writer_path == f"{exclusion_dir}/non_danish_documents"

    # Verify label_only is set to False.
    assert lang_filter.label_only is False

    # Step 3: GopherRepetitionFilter.
    gopher_filter = pipeline[2]
    assert isinstance(gopher_filter, GopherRepetitionFilter)
    assert isinstance(gopher_filter.tokenizer, WordTokenizer)
    exclusion_writer_gopher = gopher_filter.exclusion_writer
    assert isinstance(exclusion_writer_gopher, ParquetWriter)
    writer_path = exclusion_writer_gopher.output_folder.path
    assert writer_path == f"{exclusion_dir}/gopher_repetition"

    # Step 4: ParquetWriter (for the output).
    writer = pipeline[3]
    assert isinstance(writer, ParquetWriter)
    writer_path = writer.output_folder.path
    assert writer_path == f"{output_dir}/filter_output"


def test_filter_pipeline_empty_paths():
    """
    Edge case: Test that filter_pipeline handles empty strings for paths.
    """
    input_data = ""
    output_dir = ""
    exclusion_dir = ""

    with pytest.raises(ValueError):
        filter_pipeline(input_data, output_dir, exclusion_dir)


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

    dedup_sigs, find_dedups, filter_dedup = sent_dedup(
        dedup_dir, filter_dir, output_dir, exclusion_dir, n_workers
    )

    # Validate dedup_sigs (list of SentenceDedupSignature).
    assert isinstance(dedup_sigs, list)
    assert len(dedup_sigs) == 1
    sig = dedup_sigs[0]
    assert isinstance(sig, SentenceDedupSignature)
    assert sig.output_folder.path == f"{dedup_dir}/sigs"
    assert sig.finder_workers == n_workers
    assert sig.language == Languages.danish

    # Check the configuration inside the signature.
    config = sig.config
    assert isinstance(config, SentDedupConfig)
    assert config.n_sentences == 3
    assert config.split_sentences is False
    assert config.only_dedup_in_index is True
    assert config.min_doc_words == 50
    assert config.min_num_sentences == 1

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
    assert isinstance(reader, ParquetReader)
    assert reader.data_folder.path == filter_dir

    # Step 2: SentenceDedupFilter.
    dedup_filter = filter_dedup[1]
    assert isinstance(dedup_filter, SentenceDedupFilter)
    assert dedup_filter.data_folder.path == f"{dedup_dir}/dups"
    exclusion_writer = dedup_filter.exclusion_writer
    assert isinstance(exclusion_writer, ParquetWriter)
    assert exclusion_writer.output_folder.path == f"{exclusion_dir}/sent_dedup"

    # Step 3: ParquetWriter.
    writer = filter_dedup[2]
    assert isinstance(writer, ParquetWriter)
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
        sent_dedup(dedup_dir, filter_dir, output_dir, exclusion_dir, n_workers)


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
