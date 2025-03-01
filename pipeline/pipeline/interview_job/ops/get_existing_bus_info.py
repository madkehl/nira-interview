from dagster import get_dagster_logger, op
import pandas as pd
import os
from typing import List

_OUTPUT_CSV_TO_READ = "pipeline/interview_job/output/output.csv"


absolute_path = os.path.abspath(_OUTPUT_CSV_TO_READ)

@op
def get_existing_bus_info() -> List[str]:

    return list(pd.read_csv(
        absolute_path,
        index_col=None,
        encoding="utf-8",
    )['bus_number'])

