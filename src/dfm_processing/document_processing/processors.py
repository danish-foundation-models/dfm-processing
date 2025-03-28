"""This module contains processing methods for extracting text from various documents."""

import gzip
import io
import json
import re
from dataclasses import asdict
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any, Callable

from docling.datamodel.base_models import DocumentStream
from docling.datamodel.document import TableItem, TextItem
from extract_msg import openMsg
from joblib import Parallel, delayed
from loguru import logger
from pandas import Index
from pypandoc import convert_file, convert_text
from textract.parsers import process as process_doc
from tqdm import tqdm
from trafilatura import extract as extract_html_text

from .utils import (
    build_document_converter,
    build_metadata,
    create_JSONL,
    find_near_duplicates,
    generate_decode_url,
    make_unique,
    remove_newlines,
)

if TYPE_CHECKING:
    import pandas as pd

SCRIPT_TAG = "<script></script>"


def process_json(
    file_path: Path | IO[bytes], source: str, **kwargs
) -> list[str] | str | None:
    """
    Extracts and processes text from a JSON file based on a given path of keys and formatter.

    Args:
        file_path (Path): Path to the JSON file.
        source (str): Source description for logging purposes.
        **kwargs: Additional arguments:
            - text_path (str): Comma-separated keys to traverse the JSON structure.
            - text_format (str): Format of the text ('txt' or 'html').
            - formatter (Callable): Custom function to format the extracted text.

    Returns:
        list[str]: A list of formatted text strings extracted from the JSON file.
    """

    def extract_text(data: Any, keys: list[str]) -> list[str]:
        """
        Recursively traverses the JSON structure to extract text at the given keys.

        Args:
            data (Any): The current level of the JSON object.
            keys (list[str]): List of keys to traverse.

        Returns:
            list[str]: A list of extracted text strings.
        """
        if not keys:
            # If keys are exhausted, handle the data
            if isinstance(data, str):
                return [data]
            if isinstance(data, list):
                # Collect strings from list elements
                texts = []
                for item in data:
                    texts.extend(extract_text(item, keys))
                return texts
            if isinstance(data, dict):
                return [json.dumps(data)]  # Convert objects to string representation

            return [str(data)]  # Fallback for other data types

        key = keys[0]
        remaining_keys = keys[1:]

        if isinstance(data, dict) and key in data:
            return extract_text(data[key], remaining_keys)
        if isinstance(data, list):
            # If data is a list, attempt to extract text from each item
            texts = []
            for item in data:
                texts.extend(extract_text(item, keys))
            return texts

        logger.warning(f"Key '{key}' not found in JSON document.")
        return []

    # Supported formatters (default options)
    def default_formatter(text: str) -> str:
        return text

    # Setup
    key_path: list[str] = kwargs.get("text_path", "text").split(",")  # type: ignore
    text_format: str = kwargs.get("text_format", "txt")

    # Choose the appropriate formatter
    formatters = {
        "txt": default_formatter,
        "html": lambda text: extract_html_text(text + SCRIPT_TAG),
    }
    formatter: Callable[[str], str] = formatters.get(
        text_format,
        default_formatter,
    )

    if text_format not in formatters:
        logger.warning(
            f"Text format '{text_format}' is not supported. Defaulting to plain text.",
        )

    # Load and process the JSON file
    try:
        with file_path.open("r", encoding="utf-8") as f:  # type: ignore
            document = json.load(f)

        extracted_texts = extract_text(document, key_path)
        extracted_texts = [formatter(text) for text in extracted_texts]
        metadata = build_metadata(file_path)
        formatted_texts = [
            json.dumps(
                asdict(create_JSONL(text, source, metadata)),
                ensure_ascii=False,
            )
            for text in extracted_texts
        ]
        return formatted_texts
    except Exception as e:
        logger.error(
            f"Error processing JSON file '{file_path}' from source '{source}': {e}",
        )
        return None


def process_msg(
    file_path: Path | IO[bytes], source: str, **kwargs
) -> list[str] | str | None:  # noqa: ARG001
    """Read a single MSG file and build a JSONL object. Uses Trafilatura for the extraction.

    Args:
        file_path: Path to the HTML file
        source: What is the data source (most likely DSK client)
        **kwargs: Additional arguments

    Returns:
        str: JSONL line with the file content
    """

    def replace_url(match: re.Match) -> str:
        url = match.group(0)
        decoded_url = generate_decode_url(url)
        if decoded_url:
            return decoded_url
        return url

    text = openMsg(file_path).body
    text = re.sub(r"(\n)+", "\n", text)
    text = re.sub(r"\[.+?\]", "", text)
    text = text.replace("\r", "")
    text = re.sub(r"https?:\/\/[^>]+", replace_url, text)
    metadata = build_metadata(file_path)
    return json.dumps(asdict(create_JSONL(text, source, metadata)), ensure_ascii=False)


def process_html(
    file_path: Path | IO[bytes],
    source: str,
    **kwargs,  # noqa: ARG001
) -> list[str] | str | None:
    """Read a single HTML file and build a JSONL object. Uses Trafilatura for the extraction.

    Args:
        file_path: Path to the HTML file
        source: What is the data source (most likely DSK client)
        **kwargs: Additional arguments

    Returns:
        str: JSONL line with the file content
    """
    try:
        file_content = (
            file_path.read_text()
            if isinstance(file_path, Path)
            else file_path.read().decode()
        )
    except UnicodeDecodeError:
        logger.error(f"Unable to read {file_path}")
        return None
    text = extract_html_text(file_content)
    if not text:
        return None
    text = re.sub(r"(\n)+", "\n", text)
    metadata = build_metadata(file_path)
    return json.dumps(asdict(create_JSONL(text, source, metadata)), ensure_ascii=False)


def process_epub(
    file_path: Path | IO[bytes],
    source: str,
    **kwargs,  # noqa: ARG001
) -> list[str] | str | None:
    """Read a single EPUB file and build a JSONL object. Uses Pypandoc for the extraction.

    Args:
        file_path: Path to the EPUB file
        source: What is the data source (most likely DSK client)
        **kwargs: Additional arguments

    Returns:
        str: JSONL line with the file content
    """
    if isinstance(file_path, Path):
        text = convert_file(file_path, to="plain", format="epub")
    else:
        try:
            text = convert_text(file_path.read().decode(), to="plain", format="epub")
        except UnicodeDecodeError:
            logger.error(f"Unable to read {file_path}")
            return None
    text = re.sub(r"(\n)+", "\n", text)
    metadata = build_metadata(file_path)
    return json.dumps(asdict(create_JSONL(text, source, metadata)), ensure_ascii=False)


def process_txt(
    file_path: Path | IO[bytes],
    source: str,
    **kwargs,  # noqa: ARG001
) -> list[str] | str | None:
    """Read a single TXT file and build a JSONL object

    Args:
        file_path: Path to the TXT file
        source: What is the data source (most likely DSK client)
        **kwargs: Additional arguments

    Returns:
        str: JSONL line with the file content
    """
    text = (
        file_path.read_text()
        if isinstance(file_path, Path)
        else file_path.read().decode()
    )
    text = re.sub(r"(\n)+", "\n", text)
    metadata = build_metadata(file_path)
    return json.dumps(asdict(create_JSONL(text, source, metadata)), ensure_ascii=False)


def process_word_old(
    file_path: Path | IO[bytes],
    source: str,
    **kwargs,  # noqa: ARG001
) -> list[str] | str | None:
    """Process an older version word document, specifically a .doc file.

    Args:
        file_path: Path to the .doc file
        source: What is the data source (most likely DSK client)
        **kwargs: Additional arguments

    Returns:
        str: JSONL line with the file content
    """
    file_bytes: bytes = process_doc(file_path)
    text: str = file_bytes.decode("utf-8")
    text = re.sub(r"(\n)+", "\n", text)
    metadata = build_metadata(file_path)
    return json.dumps(asdict(create_JSONL(text, source, metadata)), ensure_ascii=False)


def process_document(
    file_path: Path | IO[bytes],
    source: str,
    **kwargs,
) -> list[str] | str | None:
    """
    Process a single file, converting it into a JSONL string.

    Args:
        file: Path to the input file.
        source: Source identifier for the data.
        **kwargs: Extra arguments

    Returns:
        str | None: JSONL string if the file is processed successfully, else None.
    """
    doc_converter = kwargs.get("converter", build_document_converter())
    try:
        input_ = (
            file_path
            if isinstance(file_path, Path)
            else DocumentStream(
                name=file_path.name, stream=io.BytesIO(file_path.read())
            )
        )
        result = doc_converter.convert(input_)

        metadata = build_metadata(result.input)

        file_content = ""
        # Iterate the elements in reading order, including hierarchy level
        for item, _ in result.document.iterate_items():
            if isinstance(item, TextItem):
                if item.text.strip():
                    file_content += item.text
            elif isinstance(item, TableItem):
                table_df: pd.DataFrame = item.export_to_dataframe()
                dups = find_near_duplicates(table_df, 0.8)
                column_counts: dict[str, int] = {}
                table_df.columns = [
                    make_unique(col, column_counts) for col in table_df.columns
                ]
                columns_to_keep: Index[str] = table_df.columns.copy(deep=True)  # type: ignore
                for pair in dups:
                    if pair[1] in columns_to_keep:
                        columns_to_keep = columns_to_keep.drop(pair[1])

                df_cleaned = table_df[list(columns_to_keep)]
                df_cleaned = remove_newlines(df_cleaned)
                file_content += df_cleaned.to_markdown(index=False, tablefmt="github")
            file_content += "\n\n"

        # Create and return JSONL entry
        return json.dumps(
            asdict(create_JSONL(file_content, source, metadata)),
            ensure_ascii=False,
        )
    except Exception as e:
        logger.error(f"Failed to process file {file_path}: {e}")
        return None


def process_file(
    file_path: Path | IO[bytes], source: str, **kwargs: dict
) -> str | list[str] | None:
    """Generic method for processing a file. Will find the file type and use the right processing method.

    Args:
        file: Path to the file to process
        source: What DSK client have delivered the file
        **kwargs: Extra arguments

    Returns:
        str | None: Returns a JSONL line if the file type is supported, else None.
    """
    suffix = (
        file_path.suffix
        if isinstance(file_path, Path)
        else "." + file_path.name.split(".")[-1]
    )
    method = {
        ".pdf": process_document,
        ".html": process_html,
        ".docx": process_document,
        ".epub": process_epub,
        ".txt": process_txt,
        ".pptx": process_document,
        ".md": process_txt,
        ".msg": process_msg,
        ".json": process_json,
        ".doc": process_word_old,
    }.get(suffix)

    if not method:
        logger.warning(f"Unsupported file type: {suffix} - for file: {file_path!s}")
        return None

    return method(file_path, source, **kwargs)


def process_files(
    files: list[Path],
    output_path: Path,
    dsk_client: str,
    output_suffix: str = ".jsonl.gz",
    n_workers: int = 4,
    **kwargs,
):
    save_file = output_path
    if "".join(output_path.suffixes) != ".jsonl.gz":
        save_file = output_path / (dsk_client + output_suffix)

    converter = build_document_converter()
    parallel = Parallel(n_jobs=n_workers, return_as="generator_unordered")
    save_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(save_file, mode="wb") as out_file:
        # with (output_path / output_name).open("w+") as out_file:
        for doc in parallel(
            delayed(process_file)(
                file,
                dsk_client,
                converter=converter,
                **kwargs,
            )
            for file in tqdm(files)
        ):
            if doc is None:
                continue
            if isinstance(doc, str):
                out_file.write(f"{doc}\n".encode())
            if isinstance(doc, list):
                [out_file.write(f"{d}\n".encode()) for d in doc]
