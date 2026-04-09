{#
    Override dbt's default generate_schema_name behavior.

    DEFAULT dbt behavior concatenates target schema with custom schema:
        target.schema=PUBLIC + custom_schema=GOLD  →  PUBLIC_GOLD

    THIS OVERRIDE uses the custom schema as-is:
        custom_schema=GOLD  →  GOLD

    Models with no custom schema fall back to target.schema (PUBLIC).

    Includes a log() call so you can verify the macro is being invoked
    by running: dbt compile --select bridge_return_products
    The log lines appear in stdout/dbt.log during compilation.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ log("[generate_schema_name] custom=None -> using target.schema=" ~ target.schema, info=True) }}
        {{ target.schema }}
    {%- else -%}
        {{ log("[generate_schema_name] custom=" ~ custom_schema_name ~ " -> returning as-is", info=True) }}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
