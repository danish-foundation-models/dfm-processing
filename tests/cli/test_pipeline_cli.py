"""This module contains test pertaining to the pipeline cli methods."""

from typer.testing import CliRunner
import pytest
from unittest.mock import MagicMock

from datatrove.utils.stats import Stats

# Import the Typer app from your CLI module.
from dfm_processing.pipeline_cli import app

runner = CliRunner()

# Dummy configuration for a valid run.
VALID_DATASET = {
    "name": "test",
    "input_dir": "input",
    "output_dir": "output",
    "exclusion_dir": "exclude",
    "logging_dir": "logs",
}

VALID_EXECUTOR = {"n_workers": 2, "n_tasks": 3}

VALID_CLUSTER = {"type": "distributed"}

VALID_SENT_DEDUP = {
    "input_dir": "output",
    "glob_pattern": "*.parquet",
    "dedup_dir": "dedup",
    "exclusion_dir": "exclusions",
    "output_dir": "sent_dedup",
    "logging_dir": "sent_logs",
}

VALID_MINH_DEDUP = {
    "input_dir": "sent_dedup",
    "glob_pattern": "*.parquet",
    "dedup_dir": "dedup",
    "exclusion_dir": "exclusions",
    "output_dir": "minh_dedup",
    "logging_dir": "minh_logs",
    "n_buckets": 14,
}

VALID_PIPELINE = {
    "datasets": [VALID_DATASET],
    "executor": VALID_EXECUTOR,
    "sentence_deduplication": VALID_SENT_DEDUP,
    "minhash_deduplication": VALID_MINH_DEDUP,
    "cluster": VALID_CLUSTER,
}

# Dummy configuration with an invalid datasets field (not a list).
dummy_config_invalid_datasets = {
    "executor": {"n_tasks": 10, "n_workers": 5, "debug": True},
    "datasets": "not a list",  # Invalid: should be a list.
    "sent_dedup": False,
    "dedup_dir": "/dummy/dedup",
    "cluster": {
        "type": "distributed",
        "scheduler_host": "localhost",
        "scheduler_port": 8786,
        "n_workers": 3,
    },
}


# A dummy PipelineStats class so that the CLI’s sum() call works.
class DummyPipelineStats:
    def __init__(self):
        self.value = 0
        self.stats = [Stats("test")]

    def __add__(self, other):
        return self

    def get_repr(self, title):
        return f"{title}: dummy stats"


@pytest.fixture
def dummy_client():
    """
    Returns a dummy client that simulates the behavior of a distributed Client.
    """
    client = MagicMock()
    dummy_stats = DummyPipelineStats()
    client.gather.return_value = [dummy_stats]
    return client


@pytest.fixture
def dummy_future():
    """
    Returns a dummy future to simulate job submission.
    """
    return MagicMock()


def test_cli_filter_valid(monkeypatch, tmp_path, dummy_client, dummy_future):
    """
    Test the CLI command with a valid configuration.
    We monkeypatch:
      - load_yml_config: returns our dummy_config_valid.
      - create_client: returns a dummy client.
      - submit_job: always returns a dummy future.
      - print_pipeline: replaced with a no-op.
      - logger.success: captured to verify that output is generated.
    """
    # Create a dummy config file (its content is irrelevant because we monkeypatch load_yml_config).
    config_file = tmp_path / "dummy_config.yml"
    config_file.write_text("dummy content")

    # Import the CLI module so that we can patch attributes on it.
    import dfm_processing.pipeline_cli as cli_mod

    # Patch load_yml_config so it returns our dummy configuration.
    monkeypatch.setattr(cli_mod, "load_yml_config", lambda path: VALID_PIPELINE)

    # Patch create_client to return our dummy client.
    monkeypatch.setattr(cli_mod, "create_client", lambda cfg: dummy_client)

    # Patch submit_job so that it returns our dummy future.
    monkeypatch.setattr(cli_mod, "submit_job", lambda client, fn: dummy_future)

    # Patch print_pipeline to be a no-op.
    monkeypatch.setattr(cli_mod, "print_pipeline", lambda executor: None)

    # Patch the logger so we can capture its output.
    dummy_logger_calls = []

    class DummyLogger:
        @staticmethod
        def success(msg):
            dummy_logger_calls.append(msg)

    monkeypatch.setattr(cli_mod, "logger", DummyLogger)

    # Invoke the CLI command with catch_exceptions=False.
    result = runner.invoke(app, ["filter", str(config_file)], catch_exceptions=False)

    # If exit_code is not 0, print details to help with debugging.
    if result.exit_code != 0:
        print("STDOUT:\n", result.stdout)
        # print("STDERR:\n", result.stderr)
        print("Exception:\n", result.exception)

    # Assert the command exited successfully.
    assert result.exit_code == 0, "CLI command failed. See above logs for details."
    # Optionally, check that the logger was called with a message containing "All tasks".
    assert any(
        "All tasks" in msg for msg in dummy_logger_calls
    ), "Expected success message not found."


def test_cli_filter_invalid_datasets(monkeypatch, tmp_path):
    """
    Test the CLI command with an invalid configuration where datasets is not a list.
    The CLI should exit with a non-zero code.
    """
    config_file = tmp_path / "dummy_config_invalid.yml"
    config_file.write_text("dummy content")

    import dfm_processing.pipeline_cli as cli_mod

    # Patch load_yml_config to return the invalid configuration.
    monkeypatch.setattr(
        cli_mod, "load_yml_config", lambda path: dummy_config_invalid_datasets
    )

    # Invoke the CLI command.
    result = runner.invoke(app, ["filter", str(config_file)])
    # The CLI is expected to call typer.Exit (with a non-zero exit code) because datasets is invalid.
    assert (
        result.exit_code != 0
    ), "CLI should have exited with a non-zero code for invalid datasets."
