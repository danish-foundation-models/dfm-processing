# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "typer",
#     "loguru",
#     "requests",
#     "tqdm",
#     "ijson",
#     "tenacity",
#     "aiohttp",
# ]
# ///
from pathlib import Path
import time
import requests
import json
from datetime import datetime, timedelta
from typer import Typer
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential
import asyncio
import aiohttp
import ijson


APP = Typer(name="Pull CVR")

QUERY = {
    "query": {
        "bool": {
            "must": [
                {"term": {"dokumenter.dokumentMimeType": "application"}},
                {"term": {"dokumenter.dokumentMimeType": "pdf"}},
                {
                    "range": {
                        "offentliggoerelsesTidspunkt": {
                            "gt": "2000-01-01T00:00:00.001Z",
                            "lt": "2025-01-02T00:00:00.001Z",
                        }
                    }
                },
            ],
            "must_not": [],
            "should": [],
        }
    },
    "size": 3000,
}


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


def daterange(start_date: datetime, end_date: datetime):
    days = int((end_date - start_date).days)
    for n in range(days * 2):
        yield start_date + timedelta(hours=n * 12)


@APP.command(name="list")
def pull_list(path: Path):
    url = "http://distribution.virk.dk/offentliggoerelser/_search"
    all_results: list = []

    start_date = datetime(2015, 1, 2)
    end_date = datetime.today()
    previous_date = datetime(2015, 1, 1)

    for idx, day in enumerate(daterange(start_date, end_date)):
        QUERY["query"]["bool"]["must"][2]["range"]["offentliggoerelsesTidspunkt"][  # type: ignore
            "gt"
        ] = previous_date.isoformat(timespec="milliseconds") + "Z"
        QUERY["query"]["bool"]["must"][2]["range"]["offentliggoerelsesTidspunkt"][  # type: ignore
            "lt"
        ] = day.isoformat(timespec="milliseconds") + "Z"

        response = requests.get(
            url, json=QUERY, headers={"Content-Type": "application/json"}
        )

        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            break

        data: dict = response.json()
        hits = data.get("hits", {}).get("hits", [])

        all_results.extend(hits)

        # Print total results fetched
        print(f"Total documents retrieved ({day}): {len(all_results)}")

        previous_date = day

        if idx % 100 == 0:
            with path.open(mode="w") as output:
                json.dump(all_results, output, ensure_ascii=False)

        time.sleep(1)


# --- Function to fetch PDFs with retry logic ---
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
async def fetch_pdf(session, url, output_dir):
    file_path = output_dir / url.split("/")[-1]

    if file_path.exists():  # Avoid redundant downloads
        return

    try:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                file_path.write_bytes(content)
            else:
                print(f"Failed to download {url}: {response.status}")
    except Exception as e:
        print(f"Error fetching {url}: {e}")


# --- Process a single report ---
async def process_report(report, session, output_dir):
    pdf_links = [
        doc["dokumentUrl"]
        for doc in report["_source"]["dokumenter"]
        if doc["dokumentMimeType"] == "application/pdf"
    ]

    # Concurrently fetch all PDFs for this report
    await asyncio.gather(*(fetch_pdf(session, url, output_dir) for url in pdf_links))


# --- Main processing function ---
async def process_documents(doc_list: Path, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        with doc_list.open("rb") as f:
            reports = ijson.items(f, "item")
            tasks = []

            for idx, report in tqdm(enumerate(reports), desc="Processing Reports"):
                tasks.append(process_report(report, session, output_dir))

                if len(tasks) >= 10:  # Batch processing to prevent overload
                    await asyncio.gather(*tasks)
                    tasks.clear()

    # Run any remaining tasks
    if tasks:
        await asyncio.gather(*tasks)


@APP.command(name="document")
def pull_documents(doc_list: Path, output: Path):
    if output.is_file():
        raise ValueError("Output should be path to an output directory, not a file.")

    asyncio.run(process_documents(doc_list, output))


if __name__ == "__main__":
    APP()
