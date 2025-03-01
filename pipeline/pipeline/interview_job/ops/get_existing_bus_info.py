from dagster import op
import pandas as pd
import os
from typing import List

_OUTPUT_CSV_TO_READ = "pipeline/interview_job/output/output.csv"
absolute_output_path = os.path.abspath(_OUTPUT_CSV_TO_READ)


@op
def get_existing_bus_info() -> List[str]:
    # check for output file
    if os.path.exists(absolute_output_path):
        # if output file, return the bus_number column
        return list(pd.read_csv(
            absolute_output_path,
            index_col=None,
            encoding="utf-8",
        )['bus_number'])
    else:
        return []
