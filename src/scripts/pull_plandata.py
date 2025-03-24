# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "geopandas",
#     "owslib",
#     "typer",
#     "pyarrow",
#     "fiona",
#     "loguru",
#     "requests",
#     "tqdm",
# ]
# ///
from glob import glob
from pathlib import Path
import time
from owslib.feature.wfs110 import WebFeatureService_1_1_0
from owslib.wfs import WebFeatureService, Authentication
import geopandas as gpd
import pandas as pd
import io
from loguru import logger
import warnings
from urllib3.exceptions import InsecureRequestWarning
import requests

from tqdm import tqdm
from typer import Typer

app = Typer(name="Pull Plandata.dk", no_args_is_help=True)

# Set the base URL for the WFS service.
# Note: Remove the query parameters (e.g. request=getcapabilities) since OWSLib will handle that.
URL = "https://geoserver.plandata.dk/geoserver/wfs"

warnings.filterwarnings("ignore", category=InsecureRequestWarning)


def get_pdf(url: str):
    response = requests.get(url, verify=False, timeout=5)

    # Check if the GET request was successful
    if response.status_code == 200:
        # Check Content-Type header for PDF
        content_type = response.headers.get("Content-Type", "").lower()
        if "application/pdf" in content_type or response.content.startswith(b"%PDF"):
            return response.content
        else:
            return None
    else:
        print(
            f"Failed to retrieve the file: {url}. Status code: {response.status_code}"
        )
        return None


@app.command(name="pull-layers", no_args_is_help=True)
def pull_layers(output_path: Path):
    # Create a WebFeatureService object. You can change the version (e.g., '2.0.0') if needed.
    wfs: WebFeatureService_1_1_0 = WebFeatureService(
        url=URL, version="1.1.0", auth=Authentication(verify=False)
    )
    gpd.options.io_engine = "fiona"
    # Print available layers (the keys are the fully qualified layer names).
    print("Available layers:")
    for layer_name, layer_content in list(wfs.contents.items()):
        print("\nDownloading features from layer:", layer_name)
        print(layer_content)
        save_path = output_path / f"{layer_name.replace(':', '_')}.parquet"
        if save_path.exists():
            continue

        try:
            # Issue a GetFeature request. Here we request the output as GeoJSON.
            response = wfs.getfeature(typename=layer_name, outputFormat="json")
            # Read the response into a GeoDataFrame. (If using GML, you might need to use GML-specific parsing.)
            gdf = gpd.read_file(io.BytesIO(response.read()), ignore_geometry=True)
            gdf.to_parquet(save_path)
        except Exception as e:
            logger.error(e)
        time.sleep(2)


@app.command(name="pull-docs", no_args_is_help=True)
def pull_docs(input_dir: Path, output_dir: Path, filetype: str = "parquet"):
    input_files = glob(str(input_dir / f"*.{filetype}"))
    links: set[str] = set()
    for input_file in input_files:
        gdf = pd.read_parquet(input_file)
        if "kp_kpt_doklink" in gdf.columns:
            links.update(gdf["kp_kpt_doklink"].unique().tolist())
        if "doklink" in gdf.columns:
            links.update(gdf["doklink"].unique().tolist())
        if "link" in gdf.columns:
            links.update(gdf["link"].unique().tolist())

    print(f"Found a total of {len(links)} links")

    for link in tqdm(links):
        # Run request
        if len(link) == 0 or not link.startswith("http"):
            continue
        file_path = output_dir / link.split("/")[-1]
        if file_path.exists():
            continue
        content = get_pdf(link)
        if content:
            with file_path.open(mode="wb") as pdf:
                pdf.write(content)
        time.sleep(2)


if __name__ == "__main__":
    app()
