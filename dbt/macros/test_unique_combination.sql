{% test unique_combination(model, combination) %}
{#
    Test that the combination of columns is unique (composite primary key).
    Usage in YAML:
        tests:
          - unique_combination:
              arguments:
                combination: ['col_a', 'col_b']
#}

SELECT
    {{ combination | join(', ') }}
FROM {{ model }}
GROUP BY {{ combination | join(', ') }}
HAVING COUNT(*) > 1

{% endtest %}
