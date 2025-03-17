"""This module contains tests for the CLI methods pertaining to document processing."""

import subprocess
from pathlib import Path
from subprocess import CalledProcessError, CompletedProcess

import pytest
from typer.testing import CliRunner

# Import the Typer app from your CLI module.
# Adjust the import path if necessary.
from dfm_processing.document_cli import app

# A global list to capture calls to process_files.
process_files_calls = []


def dummy_process_files(
    files, output_path, dsk_client, output_suffix, n_workers, **kwargs
):
    """A dummy replacement for process_files that records its call arguments."""
    process_files_calls.append(
        {
            "files": files,
            "output_path": output_path,
            "dsk_client": dsk_client,
            "output_suffix": output_suffix,
            "n_workers": n_workers,
            "kwargs": kwargs,
        }
    )


runner = CliRunner()


@pytest.fixture(autouse=True)
def clear_process_files_calls():
    """Clear captured calls before each test."""
    global process_files_calls
    process_files_calls.clear()


# ===========================
# Tests for crawl_directory
# ===========================


def test_crawl_directory_valid(tmp_path, monkeypatch):
    """
    Test the 'process-directory' command with valid input:
    - Create an input directory with a file.
    - Monkeypatch process_files to capture the call.
    """
    # Create a temporary input directory with one file.
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    file1 = input_dir / "test.txt"
    file1.write_text("dummy content")

    # Create an output directory.
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Replace process_files in the CLI module with our dummy.
    monkeypatch.setattr(
        "dfm_processing.document_cli.process_files",
        dummy_process_files,
    )

    # Call the command.
    result = runner.invoke(
        app,
        [
            "process-directory",
            str(input_dir),
            str(output_dir),
            "dummy_client",  # dsk_client argument
            "--output-suffix",
            ".jsonl.gz",
            "--n-workers",
            "4",
            "--key-paths",
            "text",
            "--text-format",
            "txt",
        ],
        catch_exceptions=False,
    )

    # Verify successful execution.
    assert result.exit_code == 0, f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    # Ensure process_files was called.
    assert len(process_files_calls) == 1
    call = process_files_calls[0]
    # Verify that the file list includes our file.
    assert any(file1 == f for f in call["files"])
    # Check other parameters.
    assert call["output_path"] == Path(output_dir)
    assert call["dsk_client"] == "dummy_client"
    assert call["output_suffix"] == ".jsonl.gz"
    assert call["n_workers"] == 4
    # Check the extra keyword arguments.
    assert call["kwargs"].get("text_path") == "text"
    assert call["kwargs"].get("text_format") == "txt"


def test_crawl_directory_no_files(tmp_path, monkeypatch):
    """
    Test the 'process-directory' command when no files are found.
    It should log an error and exit with code 1.
    """
    # Create an empty input directory.
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Replace process_files (though it should not be called).
    monkeypatch.setattr(
        "dfm_processing.document_cli.process_files",
        dummy_process_files,
    )

    result = runner.invoke(
        app,
        [
            "process-directory",
            str(input_dir),
            str(output_dir),
            "dummy_client",
        ],
        catch_exceptions=False,
    )
    # Expect exit code 1 because no files were found.
    assert result.exit_code == 1


# ===========================
# Tests for process_web_crawl
# ===========================


def test_process_web_crawl_valid(tmp_path, monkeypatch):
    """
    Test the 'process-web-crawl' command with valid input.
    - Monkeypatch subprocess.run to simulate grep output.
    - Create a data directory with a subfolder (as indicated by the grep output)
      containing at least one file.
    - Replace process_files to capture its call.
    """
    # Create a dummy crawl log file.
    crawl_log = tmp_path / "crawl.log"
    # The dummy grep output line:
    # When split, the third column is "/dummy/data", whose split gives ['', 'dummy', 'data'].
    # The code takes index 2 ("data") as the main folder.
    crawl_log.write_text("-- some /dummy/data\n")

    # Create an output directory.
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    # Create a data directory and the expected subfolder.
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    subfolder = data_dir / "data"
    subfolder.mkdir()
    file_in_subfolder = subfolder / "test.txt"
    file_in_subfolder.write_text("dummy file")

    # Monkeypatch subprocess.run to return a dummy CompletedProcess.
    def dummy_run(command, text, capture_output, check):
        return CompletedProcess(
            args=command, returncode=0, stdout="-- some /dummy/data\n"
        )

    monkeypatch.setattr(subprocess, "run", dummy_run)

    # Replace process_files with our dummy.
    monkeypatch.setattr(
        "dfm_processing.document_cli.process_files",
        dummy_process_files,
    )

    result = runner.invoke(
        app,
        [
            "process-web-crawl",
            str(crawl_log),
            str(output_dir),
            str(data_dir),
            "dummy_client",
            "--output-suffix",
            ".jsonl.gz",
            "--n-workers",
            "4",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, f"STDOUT: {result.stdout}\nSTDERR: {result.stderr}"
    # Verify process_files was called.
    assert len(process_files_calls) == 1
    call = process_files_calls[0]
    # Ensure the file list includes the file from the discovered subfolder.
    assert any(file_in_subfolder == f for f in call["files"])
    assert call["output_path"] == Path(output_dir)
    assert call["dsk_client"] == "dummy_client"
    assert call["output_suffix"] == ".jsonl.gz"
    assert call["n_workers"] == 4


def test_process_web_crawl_subprocess_fail(tmp_path, monkeypatch):
    """
    Test the 'process-web-crawl' command when the grep subprocess fails.
    In this case, the command should exit with code 1.
    """
    crawl_log = tmp_path / "crawl.log"
    crawl_log.write_text("dummy content")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Monkeypatch subprocess.run to simulate failure.
    def dummy_run_fail(command, text, capture_output, check):
        raise CalledProcessError(returncode=1, cmd=command)

    monkeypatch.setattr(subprocess, "run", dummy_run_fail)

    result = runner.invoke(
        app,
        [
            "process-web-crawl",
            str(crawl_log),
            str(output_dir),
            str(data_dir),
            "dummy_client",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 1


def test_process_web_crawl_no_files(tmp_path, monkeypatch):
    """
    Test the 'process-web-crawl' command when the grep output indicates a valid folder,
    but no files are found in that folder. The command should exit with code 1.
    """
    # Create a crawl log file whose dummy output yields main_folder "data".
    crawl_log = tmp_path / "crawl.log"
    crawl_log.write_text("-- some /dummy/data\n")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    # Do NOT create the subfolder "data" inside data_dir, so no files will be found.

    # Monkeypatch subprocess.run to return valid grep output.
    def dummy_run(command, text, capture_output, check):
        return CompletedProcess(
            args=command, returncode=0, stdout="-- some /dummy/data\n"
        )

    monkeypatch.setattr(subprocess, "run", dummy_run)
    # Replace process_files even though it should not be called.
    monkeypatch.setattr(
        "dfm_processing.document_cli.process_files",
        dummy_process_files,
    )

    result = runner.invoke(
        app,
        [
            "process-web-crawl",
            str(crawl_log),
            str(output_dir),
            str(data_dir),
            "dummy_client",
        ],
        catch_exceptions=False,
    )

    # Because no files are found in the expected folder, the command should exit with code 1.
    assert result.exit_code == 1
