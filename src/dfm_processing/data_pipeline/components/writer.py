from collections import Counter, defaultdict
from typing import IO, Callable, Literal

from datatrove.io import DataFolderLike
from datatrove.pipeline.writers.disk_base import DiskWriter


class NullableParquetWriter(DiskWriter):
    default_output_filename: str = "${rank}.parquet"
    name = "📒 Parquet (Nullable)"
    _requires_dependencies = ["pyarrow"]

    def __init__(
        self,
        output_folder: DataFolderLike,
        output_filename: str | None = None,
        compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] | None = None,
        adapter: Callable | None = None,
        batch_size: int = 1000,
        expand_metadata: bool = False,
        max_file_size: int = 5 * 2**30,  # 5GB
    ):
        # Validate the compression setting
        if compression not in {"snappy", "gzip", "brotli", "lz4", "zstd", None}:
            raise ValueError(
                "Invalid compression type. Allowed types are 'snappy', 'gzip', 'brotli', 'lz4', 'zstd', or None."
            )

        super().__init__(
            output_folder,
            output_filename,
            compression=None,  # Ensure superclass initializes without compression
            adapter=adapter,
            mode="wb",
            expand_metadata=expand_metadata,
            max_file_size=max_file_size,
        )
        self._writers: dict = {}
        self._batches: defaultdict = defaultdict(list)
        self._file_counter: Counter = Counter()
        self.compression: Literal["snappy", "gzip", "brotli", "lz4", "zstd"] | None = (
            compression
        )
        self.batch_size: int = batch_size
        self._file_handlers: dict = {}

    def _on_file_switch(self, original_name, old_filename, new_filename):
        """
            Called when we are switching file from "old_filename" to "new_filename" (original_name is the filename
            without 000_, 001_, etc)
        Args:
            original_name: name without file counter
            old_filename: old full filename
            new_filename: new full filename
        """
        self._writers.pop(original_name).close()
        super()._on_file_switch(original_name, old_filename, new_filename)

    def _write_batch(self, filename):
        if not self._batches[filename]:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        def make_nullable_type(typ):
            """
            Recursively rebuilds a PyArrow type so that for list and struct types
            all inner elements/fields are marked as nullable. For list types, if the
            inner type is inferred as null, default it to pa.string(), which is often
            the expected type.
            """
            if pa.types.is_list(typ):
                inner_field = typ.value_field
                # If the inner type is null (which can happen if the list only contains nulls),
                # assume it should be a string (adjust this if you expect another type).
                if pa.types.is_null(inner_field.type):
                    new_inner_field = pa.field(
                        inner_field.name, pa.string(), nullable=True
                    )
                else:
                    new_inner_field = pa.field(
                        inner_field.name,
                        make_nullable_type(inner_field.type),
                        nullable=True,
                    )
                return pa.list_(new_inner_field)
            elif pa.types.is_struct(typ):
                # For struct types, rebuild each child field recursively.
                new_fields = [
                    pa.field(field.name, make_nullable_type(field.type), nullable=True)
                    for field in typ
                ]
                return pa.struct(new_fields)
            else:
                # For other types, return as is.
                return typ

        def make_nullable_schema(schema):
            """
            Takes an existing schema and returns a new one with all fields (and
            their nested elements, if applicable) marked as nullable.
            """
            return pa.schema(
                [
                    pa.field(field.name, make_nullable_type(field.type), nullable=True)
                    for field in schema
                ]
            )

        # Prepare the record batch from your documents.
        batch = pa.RecordBatch.from_pylist(self._batches.pop(filename))

        if filename not in self._writers:
            # Infer the initial schema from the first batch.
            initial_schema = batch.schema
            # Build a schema that marks all fields (including nested ones) as nullable.
            nullable_schema = make_nullable_schema(initial_schema)
            self._writers[filename] = pq.ParquetWriter(
                self._file_handlers[filename],
                schema=nullable_schema,
                compression=self.compression,
            )
        else:
            # For subsequent batches, cast the batch to the writer's schema.
            # This ensures that even if Arrow infers a different inner type (like null for an empty list),
            # the batch is converted to match the existing file schema.
            batch = batch.cast(self._writers[filename].schema)

        # Write the batch using the ParquetWriter.
        self._writers[filename].write_batch(batch)

    def _write(self, document: dict, file_handler: IO, filename: str):
        if filename not in self._file_handlers:
            self._file_handlers[filename] = file_handler

        self._batches[filename].append(document)
        if len(self._batches[filename]) == self.batch_size:
            self._write_batch(filename)

    def close(self):
        for filename in list(self._batches.keys()):
            self._write_batch(filename)
        for writer in self._writers.values():
            writer.close()
        self._batches.clear()
        self._writers.clear()
        super().close()
