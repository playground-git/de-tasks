import logging as log
import os
import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# random.seed(42)


# NOTE:  I started with these configs for faster development; was too lazy to use connections, variables, etc.
PROVIDERS = {
    "provider_1": {
        "connection_id": "provider_1_conn",
        "data_types": ["type_1", "type_2"],
    },
    "provider_2": {
        "connection_id": "provider_2_conn",
        "data_types": ["type_1"],
    },
    # TODO: add more providers (up to 15)
}

CONSUMERS = {
    "consumer_1": {
        "connection_id": "consumer_1_conn",
    },
    "consumer_2": {
        "connection_id": "consumer_2_conn",
    },
    "consumer_3": {
        "connection_id": "consumer_3_conn",
    },
}

PROVIDER_CONSUMER_MAPPING = {
    "provider_1": {
        "type_1": ["consumer_1", "consumer_3"],
        "type_2": ["consumer_1", "consumer_2"],
        # TODO: add more types (up to 10)
    },
    "provider_2": {
        "type_1": ["consumer_1", "consumer_2"],
        # TODO: add more types (up to 10)
    },
    # TODO: add more mappings
}

# It's used for testing when tasks fails
FAILURE_PROBABILITIES = {
    "extract": 0.1,  # chance of extract failure
    "transform": 0.15,  # chance of transform failure
    "load": 0.2,  # chance of load failure
}


def maybe_fail(operation, provider_id, data_type, consumer_id=None):
    failure_chance = FAILURE_PROBABILITIES.get(operation, 0)

    if random.random() < failure_chance:
        error_msg = f"{operation.capitalize()} operation failed"
        if consumer_id:
            error_msg += f" for {provider_id}/{data_type} to {consumer_id}"
        else:
            error_msg += f" for {provider_id}/{data_type}"

        raise AirflowFailException(error_msg)


# Helper functions for ETL


def extract(provider_id, data_type):
    # NOTE: artificial failure
    maybe_fail("extract", provider_id, data_type)

    # NOTE: not actual "extract", but we need some test data =)
    columns = ["id", "value", "timestamp"]
    data = []

    for i in range(100):
        data.append([i, random.random() * 100, datetime.now().isoformat()])

    df = pd.DataFrame(data, columns=columns)

    os.makedirs(f"/tmp/airflow_data/raw/{provider_id}/{data_type}", exist_ok=True)
    file_path = f"/tmp/airflow_data/raw/{provider_id}/{data_type}/data.csv"
    df.to_csv(file_path, index=False)

    return {
        "file_path": file_path,
        "rows": len(df),
        "provider_id": provider_id,
        "data_type": data_type,
    }


def transform(extract_result, consumer_id):
    provider_id = extract_result["provider_id"]
    data_type = extract_result["data_type"]

    # NOTE: artificial failure
    maybe_fail("transform", provider_id, data_type, consumer_id)

    df = pd.read_csv(extract_result["file_path"])

    # NOTE: heavy transformation step
    df["transformed_value"] = df["value"] * random.randint(1, 42)

    # some consumer specific column
    df[f"for_{consumer_id}"] = True

    os.makedirs(
        f"/tmp/airflow_data/transformed/{provider_id}/{data_type}/{consumer_id}",
        exist_ok=True,
    )
    file_path = f"/tmp/airflow_data/transformed/{provider_id}/{data_type}/{consumer_id}/data.csv"
    df.to_csv(file_path, index=False)

    return {
        "file_path": file_path,
        "rows": len(df),
        "provider_id": provider_id,
        "data_type": data_type,
        "consumer_id": consumer_id,
    }


def load(transform_result):
    provider_id = transform_result["provider_id"]
    data_type = transform_result["data_type"]
    consumer_id = transform_result["consumer_id"]

    # NOTE: artificial failure
    maybe_fail("load", provider_id, data_type, consumer_id)

    df = pd.read_csv(transform_result["file_path"])

    # NOTE: here we kinda load our transformed data to the consumer
    # But actually we just log the data =)
    log.info(
        f"Loaded {len(df)} rows of {data_type} data from {provider_id} to {consumer_id}"
    )
    log.info(f"Sample data: {df.head().to_dict()}")

    return {
        "rows_loaded": len(df),
        "provider_id": provider_id,
        "data_type": data_type,
        "consumer_id": consumer_id,
        "status": "success",
    }


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "etl_dag",
    default_args=default_args,
    description="ETL dag",
    schedule_interval="0 18 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "solidgate"],
) as dag:

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)

    # create task groups for each provider
    for provider_id, provider_config in PROVIDERS.items():

        @task_group(group_id=f"process_{provider_id}")
        def process_provider(provider_id: str, provider_config: dict):
            provider_start = EmptyOperator(task_id=f"start_{provider_id}")

            provider_end = EmptyOperator(
                task_id=f"end_{provider_id}", trigger_rule=TriggerRule.ALL_SUCCESS
            )

            # processing step
            for data_type in provider_config["data_types"]:
                if data_type not in PROVIDER_CONSUMER_MAPPING.get(provider_id, {}):
                    continue

                @task(task_id=f"extract_{provider_id}_{data_type}", retries=0)
                def extract_data(
                    provider_id: str, data_type: str, connection_id: str
                ) -> dict:
                    # NOTE: in real life scenario, we'd use some connection_id and extract data from the povider
                    # But for the purpose of simplicity, we just mock it

                    result = extract(provider_id, data_type)
                    log.info(
                        f"Extracted {result['rows']} rows of {data_type} data from {provider_id}"
                    )

                    return result

                extract_task = extract_data(
                    provider_id=provider_id,
                    data_type=data_type,
                    connection_id=provider_config["connection_id"],
                )

                consumers = PROVIDER_CONSUMER_MAPPING[provider_id][data_type]

                for consumer_id in consumers:

                    @task(
                        task_id=f"transform_{provider_id}_{data_type}_{consumer_id}",
                        retries=0,
                    )
                    def transform_data(extract_result: dict, consumer_id: str) -> dict:
                        provider_id = extract_result["provider_id"]
                        data_type = extract_result["data_type"]

                        # NOTE: same here, just using mock function
                        result = transform(extract_result, consumer_id)
                        log.info(
                            f"Transformed {result['rows']} rows of {data_type} data from {provider_id} for {consumer_id}"
                        )

                        return result

                    @task(
                        task_id=f"load_{provider_id}_{data_type}_{consumer_id}",
                        retries=0,
                    )
                    def load_data(transform_result: dict, connection_id: str) -> dict:
                        # NOTE: and of course same here, just using mock function =)
                        result = load(transform_result)

                        return result

                    # directly pass the extract_task XCom to create transform task
                    transform_task = transform_data(
                        extract_result=extract_task, consumer_id=consumer_id
                    )

                    # directly pass the transform_task XCom to create load task
                    load_task = load_data(
                        transform_result=transform_task,
                        connection_id=CONSUMERS[consumer_id]["connection_id"],
                    )

                    extract_task >> transform_task >> load_task >> provider_end

                provider_start >> extract_task

            return provider_start, provider_end

        # create provider task group
        provider_start, provider_end = process_provider(
            provider_id=provider_id, provider_config=provider_config
        )

        start >> provider_start
        provider_end >> end
