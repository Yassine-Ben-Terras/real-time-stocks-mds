{#
    Custom schema naming.

    dbt's default concatenates the target schema and the model's custom
    schema (DEV_silver). In CI every pull request gets its own throwaway
    schema, so we want the layer suffix there too, but in prod we want clean
    layer names (SILVER, GOLD) that a BI tool can point at without churn.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim | upper }}

    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim | upper }}

    {%- endif -%}
{%- endmacro %}
