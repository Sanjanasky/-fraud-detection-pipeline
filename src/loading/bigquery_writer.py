from google.cloud import bigquery
import json

def write_flagged_to_bq(row_dict: dict, project: str, dataset: str):
    """
    Called per flagged transaction after LLM adds explanation.
    """
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.flagged_with_explanation"

    errors = client.insert_rows_json(table_id, [row_dict])
    if errors:
        print(f"BigQuery insert error: {errors}")
    else:
        print(f"Wrote flagged txn {row_dict['transaction_id']} to BQ")