"""Module containing methods for building deduplication pipelines."""

from datatrove.pipeline.base import PipelineStep
from datatrove.pipeline.dedup import (
    SentenceFindDedups,
    SentenceDedupSignature,
    SentDedupConfig,
    SentenceDedupFilter,
)
from datatrove.pipeline.dedup import (
    MinhashConfig,
    MinhashDedupSignature,
    MinhashDedupBuckets,
    MinhashDedupCluster,
    MinhashDedupFilter,
)
from datatrove.utils.typeshelper import Languages
from datatrove.utils.hashing import HashConfig

from dfm_processing.data_pipeline.components.writer import (
    JSONParquetWriter,
)
from dfm_processing.data_pipeline.components.reader import (
    JSONParquetReader,
)


def minhash_deduplication(
    data_dir: str,
    dedup_dir: str,
    output_dir: str,
    exclusion_dir: str,
    n_buckets: int = 14,
) -> tuple[
    list[PipelineStep], list[PipelineStep], list[PipelineStep], list[PipelineStep]
]:
    input_reader = JSONParquetReader(data_dir, glob_pattern="**/*.parquet")
    # stage 1 computes minhash signatures for each task (each task gets a set of files)
    # you can also change ngrams or the number of buckets and their size here
    minhash_config = MinhashConfig(
        num_buckets=n_buckets, hash_config=HashConfig(precision=64)
    )  # better precision -> fewer false positives (collisions)
    stage1: list[PipelineStep] = [
        input_reader,
        MinhashDedupSignature(
            output_folder=f"{dedup_dir}/signatures",
            config=minhash_config,
            language=Languages.danish,
        ),
    ]

    # stage 2 finds matches between signatures in each bucket
    stage2: list[PipelineStep] = [
        MinhashDedupBuckets(
            input_folder=f"{dedup_dir}/signatures",
            output_folder=f"{dedup_dir}/buckets",
            config=minhash_config,
        ),
    ]

    # stage 3 creates clusters of duplicates using the results from all buckets
    stage3: list[PipelineStep] = [
        MinhashDedupCluster(
            input_folder=f"{dedup_dir}/buckets",
            output_folder=f"{dedup_dir}/remove_ids",
            config=minhash_config,
        ),
    ]

    # stage 4 reads the original input data and removes all but 1 sample per duplicate cluster
    # the data must match exactly stage 1, so number of tasks and the input source must be the same
    stage4: list[PipelineStep] = [
        input_reader,
        MinhashDedupFilter(
            input_folder=f"{dedup_dir}/remove_ids",
            exclusion_writer=JSONParquetWriter(f"{exclusion_dir}/removed"),
        ),
        JSONParquetWriter(output_folder=f"{output_dir}/deduplicated_output"),
    ]

    return stage1, stage2, stage3, stage4


def sentence_deduplication(
    data_dir: str,
    dedup_dir: str,
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
        split_sentences=True,  # set to False to split on \n instead of sentences
        only_dedup_in_index=True,
        min_doc_words=50,  # minimum number of words to keep document after dedup
        min_num_sentences=1,  # minimum number of sentences to keep document after dedup
    )

    reader = JSONParquetReader(data_folder=data_dir, glob_pattern="**/*.parquet")
    dedup_sigs: list[PipelineStep] = [
        reader,
        SentenceDedupSignature(
            output_folder=f"{dedup_dir}/sigs",
            config=config,
            finder_workers=n_workers,
            language=Languages.danish,
        ),
    ]

    find_dedups: list[PipelineStep] = [
        SentenceFindDedups(
            data_folder=f"{dedup_dir}/sigs",
            output_folder=f"{dedup_dir}/dups",
            config=config,
        )
    ]

    filter_dedup: list[PipelineStep] = [
        reader,
        SentenceDedupFilter(
            data_folder=f"{dedup_dir}/dups",
            config=config,
            exclusion_writer=JSONParquetWriter(f"{exclusion_dir}/sent_dedup/"),
        ),
        JSONParquetWriter(f"{output_dir}/sent_dedup_output"),
    ]

    return dedup_sigs, find_dedups, filter_dedup
