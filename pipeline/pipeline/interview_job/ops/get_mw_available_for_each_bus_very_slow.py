from time import sleep

from dagster import get_dagster_logger, op

_DUMMY_BUS_NUMBER_TO_MW_AVAILABLE = {
    "10a": 100_000,
    "11a": 90_000,
    "12a": 110_000,
    "13a": 140_000,
    "14a": 160_000,
}


@op
def get_mw_available_for_each_bus_very_slow(df_bus_numbers):

    df_bus_numbers["mw_available"] = df_bus_numbers.apply(
        lambda row: _run_slow_calculation_for_bus(row["bus_number"]), axis=1
    )

    return df_bus_numbers


def _run_slow_calculation_for_bus(bus_number):
    get_dagster_logger().info(f"Starting calculation for bus {bus_number}")
    sleep(300)
    get_dagster_logger().info(f"Finished calculation for bus {bus_number}")

    return _DUMMY_BUS_NUMBER_TO_MW_AVAILABLE[bus_number]
