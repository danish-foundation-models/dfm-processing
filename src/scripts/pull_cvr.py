import sys
import time
import requests
import json
from datetime import datetime, timedelta


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


def daterange(start_date: datetime, end_date: datetime):
    days = int((end_date - start_date).days)
    for n in range(days * 2):
        yield start_date + timedelta(hours=n * 12)


url = "http://distribution.virk.dk/offentliggoerelser/_search"
from_offset = 0
all_results: list = []

path = sys.argv[1]

start_date = datetime(2015, 1, 2)
end_date = datetime.today()
previous_date = datetime(2015, 1, 1)

for idx, day in enumerate(daterange(start_date, end_date)):
    QUERY["query"]["bool"]["must"][2]["range"]["offentliggoerelsesTidspunkt"]["gt"] = (  # type: ignore
        previous_date.isoformat(timespec="milliseconds") + "Z"
    )
    QUERY["query"]["bool"]["must"][2]["range"]["offentliggoerelsesTidspunkt"]["lt"] = (  # type: ignore
        day.isoformat(timespec="milliseconds") + "Z"
    )

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

    if idx % 10 == 0:
        with open(path, "w") as output:
            json.dump(all_results, output, ensure_ascii=False)

    time.sleep(1)
