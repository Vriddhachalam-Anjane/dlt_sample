import dlt
from dlt.sources.helpers import requests
from requests.auth import HTTPBasicAuth
from typing import Any, Optional

@dlt.resource(
    primary_key="id",
    table_name=lambda i: i["type"],
    write_disposition="append",
)
def repo_events(last_created_at=dlt.sources.incremental("created_at"),github_token: Optional[str] = dlt.secrets.value):
    url = "https://api.github.com/repos/Vriddhachalam-Anjane/dbt_self_for_django/events?per_page=100"


    while True:
        response = requests.get(url, auth=HTTPBasicAuth('username', github_token))


        # response = requests.get(url)
        response.raise_for_status()
        yield response.json()


        # Stop requesting pages if the last element was already older than
        # the initial value.
        # Note: incremental will skip those items anyway, we just do not
        # want to use the API limits.
        if last_created_at.start_out_of_range:
            break


        # Get the next page.
        if "next" not in response.links:
            break
        url = response.links["next"]["url"]



pipeline = dlt.pipeline(
    pipeline_name="github_events",
    destination="duckdb",
    dataset_name="github_events_data",
)
load_info = pipeline.run(repo_events)
row_counts = pipeline.last_trace.last_normalize_info


print(row_counts)
print("------")
print(load_info)