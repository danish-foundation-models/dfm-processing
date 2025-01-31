"""Module containing methods for the CLI regarding document processing."""

import logging
from pathlib import Path

import typer
from .document_processing.processors import process_files


app = typer.Typer()


@app.command(
    name="process-directory",
    help="Crawl a directory and process files of different types",
)
def crawl_directory(
    top_level_path: Path,
    output_path: Path,
    dsk_client: str,
    output_suffix: str = ".jsonl.gz",
    n_workers: int = 4,
    key_paths: str = "text",
    text_format: str = "txt",
):
    """Process a set of data delivered from a DSK organisation.

    Args:
        top_level_path: Path to a directory with the data delivered by the DSK organization
        output_path: Path to place the processed data
        dsk_client: What DSK organizations pages have been crawled
        output_suffix: What suffix to use. Defaults to ".jsonl.gz".
        n_workers: How many process to run in parallel. Defaults to 4.
        key_paths: If JSON data, what is the path to the text (Can be nested keys represented as a comma separated list).
        text_format: What format is the text, html or plain text.
    """
    files = list(top_level_path.glob("**/*.*"))

    files = list(filter(lambda x: x.is_file(), files))

    if len(files) == 0:
        logging.error("Something went wrong. No files to process")
        raise typer.Exit(code=1)

    process_files(
        files,
        output_path,
        dsk_client,
        output_suffix,
        n_workers,
        text_path=key_paths,
        text_format=text_format,
    )
