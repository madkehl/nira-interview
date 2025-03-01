import pandas as pd
from dagster import op
from typing import List
_RAW_CSV_TO_READ = "pipeline/interview_job/raw/raw_buses.csv"
# _RAW_CSV_TO_READ = "pipeline/interview_job/raw/new_raw_buses.csv"


@op
def raw_buses_to_run(ran_prev: List[str]):
    all_buses = pd.read_csv(
        _RAW_CSV_TO_READ,
        index_col=None,
        encoding="utf-8",
    )

    to_run = all_buses[~all_buses['bus_number'].isin(ran_prev)].reset_index(drop=True)
    if to_run.empty:
        return None
    else:
        return to_run
