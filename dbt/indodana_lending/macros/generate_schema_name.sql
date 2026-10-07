{#
    Default dbt behaviour prefixes custom schemas with the target schema,
    which would land models in DBT_SILVER / DBT_GOLD. This project wants
    the literal BRONZE / SILVER / GOLD layout, so the custom schema is
    used verbatim.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
