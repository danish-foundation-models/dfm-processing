"""Unit tests for document processor utils."""

from docling.document_converter import DocumentConverter
from pathlib import Path


from dfm_processing.document_processing.utils import (
    JSONL,
    build_document_converter,
    create_JSONL,
    build_metadata,
    generate_decode_url,
    make_unique,
)


def test_build_document_converter():
    doc_converter: DocumentConverter = build_document_converter()
    assert isinstance(doc_converter, DocumentConverter)


def test_create_JSONL():
    jsonl: JSONL = create_JSONL(
        text="Hello world",
        source="Test",
        metadata={"type": "test", "filename": "test.pdf"},
    )

    assert isinstance(jsonl, JSONL)
    assert jsonl.text == "Hello world"
    assert jsonl.source == "Test"
    assert jsonl.metadata == {"type": "test", "filename": "test.pdf"}
    assert jsonl.id == "Test-test.pdf"


def test_build_metadata_path(tmp_path: Path):
    text = "Hello World"
    file_path = tmp_path / "file.txt"
    file_path.write_text(text)

    metadata = build_metadata(file_path)

    assert metadata["filename"] == "file.txt"
    assert metadata["filetype"] == ".txt"
    assert metadata["filesize"] != 0
    assert metadata["page_count"] == 0
    assert metadata["file_path"] == str(file_path)


def test_make_unique_new_entry():
    column = "Test"
    column_counts = {"New": 1}
    new_column = make_unique(column_name=column, column_counts=column_counts)

    assert new_column == column
    assert column_counts == {"New": 1, "Test": 0}


def test_make_unique_seen_entry():
    column = "Test"
    column_counts = {"Test": 1}
    new_column = make_unique(column_name=column, column_counts=column_counts)

    assert new_column == "Test_2"
    assert column_counts == {"Test": 2}


def test_valid_url_with_url_param():
    link = "https://safelink.example.com?foo=bar&url=http%3A%2F%2Fexample.com"
    assert generate_decode_url(link) == "http://example.com"


def test_no_query_params():
    link = "https://safelink.example.com"
    assert generate_decode_url(link) is None


def test_single_param_no_ampersand():
    link = "https://safelink.example.com?url=http%3A%2F%2Fexample.com"
    assert generate_decode_url(link) is None


def test_invalid_param_format():
    link = "https://safelink.example.com?invalidparam&url=http%3A%2F%2Fexample.com"
    assert generate_decode_url(link) is None
