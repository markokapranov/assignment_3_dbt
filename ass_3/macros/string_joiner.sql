{% macro string_joiner(col1, col2) %}
  CONCAT(CONCAT({{col1}},' '), {{  col2}})
{% endmacro %}