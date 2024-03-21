{% macro drop_target_schema_if_exists() %}
  {% set target_schema = api.Relation.create(schema=target.schema) %}
  {{ drop_schema("clone_test") }}
  {{ log("Dropped schema " ~ target.schema, info = true) }}
{% endmacro %}