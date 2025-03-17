"""Module containing methods for the CLI regarding document processing."""

import logging
from pathlib import Path
import subprocess

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


@app.command(
    name="process-web-crawl",
    help="Process output from a web crawl",
)
def process_web_crawl(
    path_to_crawl_log: Path,
    output_path: Path,
    data_path: Path,
    dsk_client: str,
    output_suffix: str = ".jsonl.gz",
    n_workers: int = 4,
):
    """Process a set of crawled data from a DSK organisation.

    Args:
        path_to_crawl_log: Path to a log file from the crawl
        output_path: Path to place the processed data
        data_path: Path where the crawled data is located
        dsk_client: What DSK organizations pages have been crawled
        output_suffix: What suffix to use. Defaults to ".jsonl.gz".
        n_workers: How many process to run in parallel. Defaults to 4.
    """
    # Define the command as a list of strings
    command = ["grep", "^--", path_to_crawl_log]
    # Run the command and capture the output
    try:
        result = subprocess.run(command, text=True, capture_output=True, check=True)  # type: ignore
        # Filter the third column using Python (equivalent to `awk '{print $3}'`)
        main_folders = {
            line.split()[2].split("/")[2]
            if len(line.split()[2].split("/")) >= 3
            else ""
            for line in result.stdout.splitlines()
        }
        main_folders = set(filter(lambda x: x != "", main_folders))
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with error: {e}")
        raise typer.Exit(code=1)

    files: list[Path] = []
    for main_folder in main_folders:
        if not (data_path / main_folder).exists():
            continue
        files.extend(list((data_path / main_folder).glob("**/*.*")))

    files = list(filter(lambda x: x.is_file(), files))

    if len(files) == 0:
        logging.error("Something went wrong. No files to process")
        raise typer.Exit(code=1)

    process_files(files, output_path, dsk_client, output_suffix, n_workers)
