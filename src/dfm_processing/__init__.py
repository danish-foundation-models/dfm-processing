"""Main entry point for the dfm-processing CLI"""

import typer

from .document_cli import app as document_app

app = typer.Typer(name="DFM Processing CLI")

app.add_typer(document_app, name="document")
