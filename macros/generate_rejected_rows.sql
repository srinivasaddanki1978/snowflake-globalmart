{% macro generate_rejected_rows(source_ref, source_table, record_key, affected_field, issue_type, fail_flag) %}
SELECT
    '{{ source_table }}' AS source_table,
    {{ record_key }} AS record_key,
    '{{ affected_field }}' AS affected_field,
    '{{ issue_type }}' AS issue_type,
    OBJECT_CONSTRUCT(*)::VARCHAR AS record_data,
    _source_file,
    CURRENT_TIMESTAMP() AS rejected_at,
    _load_timestamp
FROM {{ ref(source_ref) }}
WHERE {{ fail_flag }}
{% endmacro %}
