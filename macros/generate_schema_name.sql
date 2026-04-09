{#
    Override dbt's default generate_schema_name behavior.

    Default behavior concatenates target schema with custom schema:
        target.schema=PUBLIC + custom_schema=GOLD  →  PUBLIC_GOLD

    We want the custom schema to be used as-is (GOLD, BRONZE, SILVER, etc.)
    to match the CLAUDE.md spec and the manually-created Snowflake schemas.

    Models with no custom schema fall back to target.schema (PUBLIC).
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
