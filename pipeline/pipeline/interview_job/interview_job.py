from dagster import job
import os

from pipeline.interview_job.ops.get_existing_bus_info import get_existing_bus_info
from pipeline.interview_job.ops.add_gw_available_column import add_gw_available_column
from pipeline.interview_job.ops.get_mw_available_for_each_bus_very_slow import (
    get_mw_available_for_each_bus_very_slow,
)
from pipeline.interview_job.ops.raw_buses_to_run import raw_buses_to_run
from pipeline.interview_job.output.output_interview_job import output_interview_job


_OUTPUT_CSV_TO_READ = "pipeline/interview_job/output/output.csv"
absolute_output_path = os.path.abspath(_OUTPUT_CSV_TO_READ)


@job
def interview_job():
    existing_buses = get_existing_bus_info(filepath=absolute_output_path)

    df_raw_buses_to_run = raw_buses_to_run(ran_prev=existing_buses)

    df_buses_with_calculation = get_mw_available_for_each_bus_very_slow(
        df_raw_buses_to_run
    )

    df_buses_with_calculation_transformed = add_gw_available_column(
        df_buses_with_calculation
    )

    output_interview_job(df_buses_with_calculation_transformed)
