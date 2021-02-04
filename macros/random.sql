{% macro random_int(len) -%}
  {% for n in range(len) %}{{ [0,1,2,3,4,5,6,7,8,9]|random }}{% endfor %}
{%- endmacro %}