{#
    Slim CI helper: cap the rows scanned when building against the CI target,
    so a pull request costs seconds of warehouse time instead of minutes.
    Usage:  select * from {{ ref('x') }} {{ limit_in_ci(50000) }}
#}
{% macro limit_in_ci(row_limit=50000) -%}
    {%- if target.name == 'ci' -%}
        limit {{ row_limit }}
    {%- endif -%}
{%- endmacro %}
