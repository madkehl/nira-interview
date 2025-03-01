from dagster import op


@op
def add_gw_available_column(df_buses_with_mw_available):

    df_buses_with_mw_available["gw_available"] = df_buses_with_mw_available.apply(
        lambda row: int(row["mw_available"] / 1_000), axis=1
    )
    return df_buses_with_mw_available
