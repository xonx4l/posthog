import dlt
from typing import Dict, List
from django.conf import settings
from .stripe import stripe_source
from dataclasses import dataclass
from posthog.warehouse.models import ExternalDataSource
import s3fs


@dataclass
class PipelineInputs:
    dataset_name: str
    job_type: str
    team_id: int


def create_pipeline(inputs: PipelineInputs):
    return dlt.pipeline(
        pipeline_name=f"{inputs.job_type}_pipeline",
        destination="filesystem",
        dataset_name=inputs.dataset_name,
        credentials={
            "aws_access_key_id": settings.AIRBYTE_BUCKET_KEY,
            "aws_secret_access_key": settings.AIRBYTE_BUCKET_SECRET,
        },
    )


@dataclass
class SourceColumnType:
    name: str
    data_type: str
    nullable: bool


@dataclass
class SourceSchema:
    resource: str
    name: str
    columns: Dict[str, SourceColumnType]
    write_disposition: str


@dataclass
class StripeJobInputs(PipelineInputs):
    stripe_secret_key: str


def run_stripe_pipeline(inputs: StripeJobInputs) -> List[SourceSchema]:
    pipeline = create_pipeline(inputs)

    # TODO: decouple API calls so they can be incrementally read and sync_rows updated
    source = stripe_source(
        stripe_secret_key=inputs.stripe_secret_key,
        endpoints=(
            "Subscription",
            "Account",
            "Customer",
            "Product",
            "Price",
        ),
    )
    pipeline.run(source, loader_file_format="parquet")
    return get_schema(pipeline)


def get_schema(pipeline: dlt.pipeline) -> List[SourceSchema]:
    schema = pipeline.default_schema
    data_tables = schema.data_tables()
    schemas = []

    for resource in data_tables:
        columns = {}
        for column_name, column_details in resource["columns"].items():
            columns[column_name] = SourceColumnType(
                name=column_details["name"],
                data_type=column_details["data_type"],
                nullable=column_details["nullable"],
            )

        resource_schema = SourceSchema(
            resource=resource["resource"],
            name=resource["name"],
            columns=columns,
            write_disposition=resource["write_disposition"],
        )
        schemas.append(resource_schema)

    return schemas


PIPELINE_TYPE_INPUTS_MAPPING = {"Stripe": StripeJobInputs}
PIPELINE_TYPE_MAPPING = {"Stripe": run_stripe_pipeline}


def get_s3fs():
    return s3fs.S3FileSystem(key=settings.AIRBYTE_BUCKET_KEY, secret=settings.AIRBYTE_BUCKET_SECRET)


def move_draft_to_production(team_id: int, external_data_source_id: str):
    model = ExternalDataSource.objects.get(team_id=team_id, id=external_data_source_id)
    bucket_name = settings.BUCKET_URL
    s3 = get_s3fs()
    s3.copy(
        f"{bucket_name}/{model.draft_folder_path}", f"{bucket_name}/{model.draft_folder_path}_success", recursive=True
    )
    try:
        s3.delete(f"{bucket_name}/{model.folder_path}", recursive=True)
    except:
        pass
    s3.copy(f"{bucket_name}/{model.draft_folder_path}_success", f"{bucket_name}/{model.folder_path}", recursive=True)
    s3.delete(f"{bucket_name}/{model.draft_folder_path}_success", recursive=True)
    s3.delete(f"{bucket_name}/{model.draft_folder_path}", recursive=True)
