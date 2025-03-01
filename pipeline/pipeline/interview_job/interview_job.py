from dagster import job

from pipeline.interview_job.ops.get_existing_bus_info import get_existing_bus_info
from pipeline.interview_job.ops.add_gw_available_column import add_gw_available_column
from pipeline.interview_job.ops.get_mw_available_for_each_bus_very_slow import (
    get_mw_available_for_each_bus_very_slow,
)
from pipeline.interview_job.ops.raw_buses_to_run import raw_buses_to_run
from pipeline.interview_job.output.output_interview_job import output_interview_job

@job
def interview_job():
    existing_buses = get_existing_bus_info()

    df_raw_buses_to_run_dyn = raw_buses_to_run(ran_prev=existing_buses)

    df_buses_with_calculation = df_raw_buses_to_run_dyn.map(get_mw_available_for_each_bus_very_slow)
    df_buses_with_calculation_transformed = df_buses_with_calculation.map(add_gw_available_column)

    df_buses_with_calculation_transformed.map(output_interview_job)
