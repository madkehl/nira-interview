from dagster import op
import pandas as pd
import os
from typing import List

@op
def get_existing_bus_info(filepath: str) -> List[str]:
    # check for output file
    if os.path.exists(filepath):
        # if output file, return the bus_number column
        return list(pd.read_csv(
            filepath,
            index_col=None,
            encoding="utf-8",
        )['bus_number'])
    else:
        return []
