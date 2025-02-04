"""Unit tests for the document processors."""

from typing import Callable, Generator
import pytest
import json
from pathlib import Path
from unittest.mock import MagicMock
import gzip
from io import BytesIO
import pandas as pd
import re

from pytest_mock import MockerFixture
from pytest_mock.plugin import _mocker

# Import the processing functions
from dfm_processing.document_processing.processors import (
    process_json,
    process_msg,
    process_html,
    process_epub,
    process_txt,
    process_word_old,
    process_document,
    process_file,
    process_files,
    SCRIPT_TAG,
)
from docling.datamodel.document import TextItem, TableItem
from docling.datamodel.base_models import DocumentStream


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
    assert len(result) == 1
    jsonl = json.loads(result[0])
    assert jsonl["text"] == "Hello world"


def test_process_json_html_formatting(
    tmp_path: Path, mocker: Callable[..., Generator[MockerFixture, None, None]]
):
    # mocker.patch("trafilatura.extract", return_value="formatted text")
    data = {
        "text": '<div class="col_1"><div class="column"><div class="styles_element_summary"><p>Højtkvalificeret international arbejdskraft er afgørende for væksten i de private virksomheder.</p></div></div></div>'
    }
    file_path = tmp_path / "test.json"
    file_path.write_text(json.dumps(data))

    result = process_json(
        file_path, "test_source", text_path="text", text_format="html"
    )
    jsonl = json.loads(result[0])
    assert jsonl["text"] == "content"


def test_process_json_missing_key(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    data = {"other": "value"}
    file_path = tmp_path / "test.json"
    file_path.write_text(json.dumps(data))

    result = process_json(file_path, "test_source", text_path="missing")
    assert len(result) == 0
    assert "Key 'missing' not found" in caplog.text


### Tests for process_msg ###
def test_process_msg_basic(
    mocker: Callable[..., Generator[MockerFixture, None, None]], tmp_path: Path
):
    mock_msg = MagicMock()
    mock_msg.body = "Hello\n   World\n[test]\nhttp://link"
    mocker.patch("extract_msg.openMsg", return_value=mock_msg)
    mocker.patch(
        "dfm_processing.document_processing.utils.generate_decode_url",
        return_value="decoded_url",
    )

    file_path = tmp_path / "test.msg"
    file_path.touch()

    result = process_msg(file_path, "test_source")
    jsonl = json.loads(result)
    assert "Hello\nWorld" in jsonl["text"]
    assert "[test]" not in jsonl["text"]
    assert "decoded_url" in jsonl["text"]


### Tests for process_html ###
def test_process_html_extraction(
    tmp_path: Path, mocker: Callable[..., Generator[MockerFixture, None, None]]
):
    # mocker.patch("trafilatura.extract", return_value="Extracted text")
    html_content = "<html><body>Test</body></html>"
    file_path = tmp_path / "test.html"
    file_path.write_text(html_content)

    result = process_html(file_path, "test_source")
    jsonl = json.loads(result)
    assert jsonl["text"] == "Test"


### Tests for process_epub ###
def test_process_epub_file(
    tmp_path: Path, mocker: Callable[..., Generator[MockerFixture, None, None]]
):
    mocker.patch("pypandoc.convert_file", return_value="Converted text")
    file_path = tmp_path / "test.epub"
    file_path.touch()

    result = process_epub(file_path, "test_source")
    jsonl = json.loads(result)
    assert jsonl["text"] == "Converted text"


### Tests for process_txt ###
def test_process_txt_newlines(tmp_path: Path):
    file_path = tmp_path / "test.txt"
    file_path.write_text("Line1\n   \nLine2")

    result = process_txt(file_path, "test_source")
    jsonl = json.loads(result)
    assert jsonl["text"] == "Line1\nLine2"


### Tests for process_word_old ###
def test_process_word_old(
    mocker: Callable[..., Generator[MockerFixture, None, None]], tmp_path: Path
):
    mocker.patch("textract.parsers.process", return_value=b"Extracted text")
    file_path = tmp_path / "test.doc"
    file_path.touch()

    result = process_word_old(file_path, "test_source")
    jsonl = json.loads(result)
    assert jsonl["text"] == "Extracted text"


### Tests for process_document ###
def test_process_document_text_and_tables(
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    mock_text_item = TextItem(text="Text1")
    mock_table_item = TableItem()
    mock_table_item.export_to_dataframe = MagicMock(
        return_value=pd.DataFrame({"A": [1]})
    )
    mocker.patch(
        "dfm_processing.document_processing.utils.find_near_duplicates", return_value=[]
    )
    mocker.patch(
        "dfm_processing.document_processing.utils.remove_newlines",
        side_effect=lambda x: x,
    )

    mock_result = MagicMock()
    mock_result.document.iterate_items.return_value = [
        (mock_text_item, 0),
        (mock_table_item, 1),
    ]
    mock_converter = MagicMock()
    mock_converter.convert.return_value = mock_result
    mocker.patch(
        "dfm_processing.document_processing.utils.build_metadata", return_value={}
    )

    result = process_document(Path("test.pdf"), "source", converter=mock_converter)
    jsonl = json.loads(result)
    assert "Text1" in jsonl["text"]
    assert "|" in jsonl["text"]  # Check for markdown table


### Tests for process_file ###
def test_process_file_dispatch(
    mocker: Callable[..., Generator[MockerFixture, None, None]],
):
    mocker.patch(
        "dfm_processing.document_processing.processors.process_document",
        return_value='{"text": "doc"}',
    )
    file = Path("test.pdf")
    result = process_file(file, "source")
    assert json.loads(result)["text"] == "doc"


def test_process_file_unsupported(caplog: pytest.LogCaptureFixture):
    file = Path("test.unknown")
    result = process_file(file, "source")
    assert result is None
    assert "Unsupported file type" in caplog.text


### Tests for process_files ###
def test_process_files_integration(
    tmp_path: Path, mocker: Callable[..., Generator[MockerFixture, None, None]]
):
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
