{#
    Called by CI after a pull request build to drop the throwaway schemas,
    so the CI database does not accumulate one schema per pull request.
#}
{% macro drop_pr_schemas() %}
    {% if target.name != 'ci' %}
        {{ log("drop_pr_schemas only runs against the ci target. Skipping.", info=True) }}
        {{ return('') }}
    {% endif %}

    {% set find_schemas %}
        select schema_name
        from {{ target.database }}.information_schema.schemata
        where schema_name ilike '{{ target.schema }}%'
    {% endset %}

    {% set results = run_query(find_schemas) %}

    {% if execute %}
        {% for schema in results.columns[0].values() %}
            {{ log("dropping " ~ target.database ~ "." ~ schema, info=True) }}
            {% do run_query("drop schema if exists " ~ target.database ~ "." ~ schema ~ " cascade") %}
        {% endfor %}
    {% endif %}
{% endmacro %}
