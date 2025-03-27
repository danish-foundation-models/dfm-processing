"""Unit tests for the document processors."""

import pytest
import json
from pathlib import Path
from unittest.mock import MagicMock
import gzip

from pytest_mock import MockerFixture

# Import the processing functions
from dfm_processing.document_processing.processors import (
    process_json,
    process_msg,
    process_html,
    process_epub,
    process_txt,
    process_word_old,
    process_file,
    process_files,
)


# Helper function to read gzipped JSONL
def read_gzipped_jsonl(path):
    with gzip.open(path, "rt") as f:
        return [json.loads(line) for line in f]


### Tests for process_json ###
def test_process_json_valid_keypath(tmp_path: Path):
    data = {"text": "Hello world"}
    file_path = tmp_path / "test.json"
    file_path.write_text(json.dumps(data))

    result = process_json(file_path, "test_source", text_path="text")
    assert len(result) == 1  # type: ignore
    jsonl = json.loads(result[0])  # type: ignore
    assert jsonl["text"] == "Hello world"


def test_process_json_html_formatting(tmp_path: Path, mocker: MockerFixture):
    mocker.patch(
        "dfm_processing.document_processing.processors.extract_html_text",
        return_value="Hello World",
    )
    data = [{"doc_1": {"text": "<p>Hello World</p>"}}]
    file_path = tmp_path / "test.json"
    file_path.write_text(json.dumps(data))

    result = process_json(
        file_path, "test_source", text_path="doc_1,text", text_format="html"
    )
    jsonl = json.loads(result[0])  # type: ignore
    assert jsonl["text"] == "Hello World"


def test_process_json_missing_key(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    data = {"other": "value"}
    file_path = tmp_path / "test.json"
    file_path.write_text(json.dumps(data))

    result = process_json(file_path, "test_source", text_path="missing")
    assert len(result) == 0  # type: ignore
    assert "Key 'missing' not found" in caplog.text


def test_process_msg_basic(tmp_path: Path, mocker: MockerFixture):
    mock_msg = MagicMock()
    mock_msg.body = "Hello\n\n\nWorld\n[test]\nhttp://link"
    # Fix: Mock the openMsg imported in YOUR module, not extract_msg's openMsg
    mocker.patch(
        "dfm_processing.document_processing.processors.openMsg",  # Path to YOUR openMsg reference
        return_value=mock_msg,
    )
    mocker.patch(
        "dfm_processing.document_processing.processors.generate_decode_url",
        return_value="decoded_url",
    )

    file_path = tmp_path / "test.msg"
    file_path.touch()

    result = process_msg(file_path, "test_source")
    jsonl = json.loads(result)  # type: ignore
    assert "Hello\nWorld" in jsonl["text"]
    assert "[test]" not in jsonl["text"]
    assert "decoded_url" in jsonl["text"]


### Tests for process_html ###
def test_process_html_extraction(tmp_path: Path):
    # mocker.patch("trafilatura.extract", return_value="Extracted text")
    html_content = "<html><body>Test</body></html>"
    file_path = tmp_path / "test.html"
    file_path.write_text(html_content)

    result = process_html(file_path, "test_source")
    jsonl = json.loads(result)  # type: ignore
    assert jsonl["text"] == "Test"


### Tests for process_epub ###
def test_process_epub_file(tmp_path: Path, mocker: MockerFixture):
    mocker.patch(
        "dfm_processing.document_processing.processors.convert_file",
        return_value="Converted text",
    )
    file_path = tmp_path / "test.epub"
    file_path.touch()

    result = process_epub(file_path, "test_source")
    jsonl = json.loads(result)  # type: ignore
    assert jsonl["text"] == "Converted text"


### Tests for process_txt ###
def test_process_txt_newlines(tmp_path: Path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("Line1\n\n\nLine2")

    result = process_txt(file_path, "test_source")
    jsonl = json.loads(result)  # type: ignore
    assert jsonl["text"] == "Line1\nLine2"


### Tests for process_word_old ###
def test_process_word_old(tmp_path: Path, mocker: MockerFixture):
    mocker.patch(
        "dfm_processing.document_processing.processors.process_doc",
        return_value=b"Extracted text",
    )
    file_path = tmp_path / "test.doc"
    file_path.touch()

    result = process_word_old(file_path, "test_source")
    jsonl = json.loads(result)  # type: ignore
    assert jsonl["text"] == "Extracted text"


### Tests for process_file ###
def test_process_file_dispatch(mocker: MockerFixture):
    mocker.patch(
        "dfm_processing.document_processing.processors.process_document",
        return_value='{"text": "doc"}',
    )
    file = Path("test.pdf")
    result = process_file(file, "source")
    assert json.loads(result)["text"] == "doc"  # type: ignore


def test_process_file_unsupported(caplog: pytest.LogCaptureFixture):
    file = Path("test.unknown")
    result = process_file(file, "source")
    assert result is None
    assert "Unsupported file type" in caplog.text


### Tests for process_files ###
def test_process_files_integration(tmp_path: Path, mocker: MockerFixture):
    mocker.patch(
        "dfm_processing.document_processing.processors.process_file",
        side_effect=lambda f, s, **kw: json.dumps({"text": f.name}),
    )
    files = [Path("file1.txt"), Path("file2.html")]
    output = tmp_path / "output"

    process_files(files, output, "client", n_workers=1)

    output_file = output / "client.jsonl.gz"
    assert output_file.exists()
    data = read_gzipped_jsonl(output_file)
    assert [d["text"] for d in data] == ["file1.txt", "file2.html"]
