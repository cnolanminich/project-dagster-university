from dagster import AssetKey, AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator, DbtArtifacts

import os
import json

from .constants import DBT_DIRECTORY
from ..resources import clone_dbt_resource
from ..partitions import daily_partition

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):

    @classmethod
    def get_asset_key(cls, dbt_resource_props):
        type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        if type == "source":
            return AssetKey(f"taxi_{name}")
        else:
            return super().get_asset_key(dbt_resource_props)
        
    @classmethod
    def get_metadata(cls, dbt_node_info):
        return {
            "columns": dbt_node_info["columns"],
            "sources": dbt_node_info["sources"],
            "description": dbt_node_info["description"],
        }

    @classmethod
    def get_group_name(cls, dbt_resource_props):
        return dbt_resource_props["fqn"][1]
        
dbt_artifacts = DbtArtifacts(
    project_dir=DBT_DIRECTORY,
    prepare_command=["--quiet",
                     "parse"],
)
DBT_MANIFEST = dbt_artifacts.manifest_path
DBT_MANIFEST_FOLDER = os.path.dirname(dbt_artifacts.manifest_path)

dbt_artifacts_clone = DbtArtifacts(
    project_dir=DBT_DIRECTORY,
    prepare_command=["--quiet",
                     "parse",
                     "--target",
                     "clone_test"],
)
DBT_MANIFEST_CLONE = dbt_artifacts_clone.manifest_path
DBT_MANIFEST_CLONE_PATH = os.path.dirname(dbt_artifacts.manifest_path)

INCREMENTAL_SELECTOR = "config.materialized:incremental"


@dbt_assets(
    manifest=DBT_MANIFEST_CLONE,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    exclude=INCREMENTAL_SELECTOR
)
def dbt_analytics(context: AssetExecutionContext, dbt_clone: DbtCliResource, dbt: DbtCliResource):
    #dbt.cli(args=['run-operation','set_up_macro'], context=context)
    dbt_clone.cli(args=['clone',"--state",DBT_MANIFEST_FOLDER], context=context)
    dbt_build_invocation = dbt_clone.cli(["build"], context=context)
    dbt.cli(args=['clone', "--state",DBT_MANIFEST_CLONE_PATH, "--full-refresh"], context=context)
    yield from dbt_build_invocation.stream()

    dbt_clone.cli(args=['run-operation','drop_target_schema_if_exists'])
    run_results_json = dbt_build_invocation.get_artifact("run_results.json")
    for result in run_results_json["results"]:
        context.log.debug(result["compiled_code"])


@dbt_assets(
    manifest=DBT_MANIFEST,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select=INCREMENTAL_SELECTOR,
    partitions_def=daily_partition,
)
def incremental_dbt_models(
    context: AssetExecutionContext,
    dbt: DbtCliResource
):
    time_window = context.partition_time_window

    dbt_vars = {
        "min_date": time_window.start.isoformat(),
        "max_date": time_window.end.isoformat()
    }
    
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()