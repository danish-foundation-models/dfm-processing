"""Main entry point for the dfm-processing CLI"""

import typer

from .document_cli import app as document_app
from .pipeline_cli import app as pipeline_app

app = typer.Typer(name="DFM Processing CLI")

app.add_typer(document_app, name="document")
app.add_typer(pipeline_app, name="pipeline")
