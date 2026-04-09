{% test in_range(model, column_name, min_value=none, max_value=none, inclusive=true) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (
    {% if min_value is not none %}
      {% if inclusive %}
        {{ column_name }} < {{ min_value }}
      {% else %}
        {{ column_name }} <= {{ min_value }}
      {% endif %}
      {% if max_value is not none %}OR{% endif %}
    {% endif %}
    {% if max_value is not none %}
      {% if inclusive %}
        {{ column_name }} > {{ max_value }}
      {% else %}
        {{ column_name }} >= {{ max_value }}
      {% endif %}
    {% endif %}
  )

{% endtest %}
