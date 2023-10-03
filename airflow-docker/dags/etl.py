from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta, date
import time
from botocore.exceptions import ClientError
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
import logging


def download_transform_and_store(url: str, s3_url: str) -> None:
    """
    This function will perform the simple data transformation such as removing data that
    do not have any coordinates. But also this will send data into the s3 bucket after such
    transformation
    """

    df = pd.read_csv(url)
    length_original_df = len(df)
    logging.info(
        f"The data was downloaded and converted to a dataframe of {length_original_df} rows"
    )

    # Delete all rows that do not have both coordinates pairs
    # df.index[df['BoolCol'] == True].tolist()
    df.drop(
        df[
            df["X"].isnull()
            & df["Y"].isnull()
            & df["LONGITUDE"].isnull()
            & df["LATITUDE"].isnull()
        ].index,
        inplace=True,
    )

    length_transformed_df = len(df)
    logging.info(
        f"{length_original_df-length_transformed_df} rows have been deleted from the dataframe because they do not have both XY and lat/long coordinates"
    )

    # # convert the X & Y coordinates into Longitude and latitude  and vice versa when one set is not available
    # # Take columns names as a list
    # new_cols = []
    # for col in df.columns:
    #     col = (
    #         col.lower()
    #         .replace(" ", "-")
    #         .replace("/", "")
    #         .replace("--", "-")
    #         .replace("--", "-")
    #         .replace(".", "")
    #     )
    #     new_cols.append(col)

    # df.set_axis(new_cols, axis=1)

    # logging.info("Columns have been renamed")

    df.to_parquet(s3_url, compression="gzip")


def S3_to_redshift_glue_job_triger(job_name: str, **kwargs) -> None:
    session = AwsGenericHook(aws_conn_id="aws_s3_conn")
    boto3_session = session.get_session(region_name="us-east-2")

    # trigger the job
    client = boto3_session.client("glue")
    client.start_job_run(JobName=job_name)


def grab_glue_job_id(job_name: str, **kwargs) -> str:
    time.sleep(10)
    session = AwsGenericHook(aws_conn_id="aws_s3_conn")
    boto3_session = session.get_session(region_name="us-east-2")
    glue_client = boto3_session.client("glue")
    jobs_running = glue_client.get_job_runs(JobName=job_name)
    job_run_id = jobs_running["JobRuns"][0]["Id"]
    return job_run_id


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 1),
    "email": ["nick@domain.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}

date_downloaded = date.today().strftime("%d-%m-%Y")
with DAG(
    "Fetch_and_load",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=False,
) as dag:
    fetch_data = PythonOperator(
        task_id="download_transform_store",
        python_callable=download_transform_and_store,
        op_kwargs={
            "url": "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/4ef82789-e038-44ef-a478-a8f3590c3eb1/resource/7fa98ab2-7412-43cd-9270-cb44dd75b573/download/Apartment%20Building%20Evaluations%202023%20-%20current.csv",
            "s3_url": f"s3://ndangalasi-lake/raw_data/{date_downloaded}-parquet.gzip",
        },
    )

    glue_job_trigger = PythonOperator(
        task_id="trigger_glue_job",
        python_callable=S3_to_redshift_glue_job_triger,
        op_kwargs={"job_name": "etl_trial_0"},
    )

    grab_glue_job_id = PythonOperator(
        task_id="grab_job_id",
        python_callable=grab_glue_job_id,
        op_kwargs={"job_name": "etl_trial_0"},
    )
    is_glue_job_still_running = GlueJobSensor(
        task_id="is_glue_job_finished",
        job_name="etl_trial_0",
        run_id='{{task_instance.xcom_pull("grab_job_id")}}',
        verbose=True,
        aws_conn_id="aws_s3_conn",
        poke_interval=60,
        timeout=3600,
    )
    [fetch_data >> glue_job_trigger >> grab_glue_job_id >> is_glue_job_still_running]
