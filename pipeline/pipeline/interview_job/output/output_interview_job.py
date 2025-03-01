from dagster import op
import os

@op
def output_interview_job(context, df_with_all_columns):
    # there should never be two csvs in the output folder
    if df_with_all_columns is not None:
        output_file_path = f"pipeline/interview_job/output/output.csv"
        if os.path.isfile(output_file_path):
            df_with_all_columns.to_csv(
                output_file_path,
                mode='a',
                header=False,
                index=False,
                line_terminator="\n",
                encoding="utf-8"
            )
        else:
            df_with_all_columns.to_csv(
                output_file_path, index=False, line_terminator="\n", encoding="utf-8"
            )
