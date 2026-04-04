/*
  Override dbt's default schema generation.

  Default dbt behaviour:  target.schema + "_" + custom_schema  →  mart_mart
  Our override:           use custom_schema directly            →  mart

  This ensures models with +schema: mart materialize in the
  ClickHouse `mart` database, not `mart_mart`.
*/
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
