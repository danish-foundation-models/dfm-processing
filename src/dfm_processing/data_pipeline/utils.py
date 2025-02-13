"""Module containing various utils/nice-to-have methods for the data pipeline."""

from datatrove.executor.local import LocalPipelineExecutor


def print_pipeline(executor: LocalPipelineExecutor):
    """Simple method that recursively prints the pipeline of a series of executors.

    Args:
        executor: The executor which pipeline to print
    """
    if executor.depends:
        print_pipeline(executor.depends)

    print(executor.pipeline)
