{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override the default behaviour which concatenates
        target.schema + custom_schema_name (e.g. "gold_silver").
        Return the custom schema as-is when set, otherwise fall
        back to the profile's default schema ("gold").
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
