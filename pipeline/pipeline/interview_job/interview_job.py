from dagster import job

from pipeline.interview_job.ops.get_existing_bus_info import get_existing_bus_info
from pipeline.interview_job.ops.add_gw_available_column import add_gw_available_column
from pipeline.interview_job.ops.get_mw_available_for_each_bus_very_slow import (
    get_mw_available_for_each_bus_very_slow,
)
from pipeline.interview_job.ops.raw_buses_to_run import raw_buses_to_run
from pipeline.interview_job.output.output_interview_job import output_interview_job
import pandas as pd

@job
def interview_job():
    # check if exists first
    existing_buses = get_existing_bus_info()

    df_raw_buses_to_run = raw_buses_to_run(ran_prev=existing_buses)

    df_buses_with_calculation = get_mw_available_for_each_bus_very_slow(
        df_raw_buses_to_run
    )

    df_buses_with_calculation_transformed = add_gw_available_column(
        df_buses_with_calculation
    )

    output_interview_job(df_buses_with_calculation_transformed)
