from dagster import op, get_dagster_logger


@op
def add_gw_available_column(df_buses_with_mw_available):
    if df_buses_with_mw_available is None:
        get_dagster_logger().info("Input is None, skipping this step.")
        return None
    else:
        df_buses_with_mw_available["gw_available"] = df_buses_with_mw_available.apply(
            lambda row: int(row["mw_available"] / 1_000), axis=1
        )
        return df_buses_with_mw_available
