"""Various pytest fixtures for running the tests."""

import pytest
from loguru import logger


@pytest.fixture(autouse=True)
def capture_loguru_logs(caplog):
    # Redirect loguru logs to pytest's caplog
    handler_id = logger.add(caplog.handler, format="{message}")
    yield
    logger.remove(handler_id)
