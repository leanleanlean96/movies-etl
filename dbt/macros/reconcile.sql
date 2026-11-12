{% macro reconcile_sql(target, incoming, columns, partition_keys) %}
  {% set cols = get_quoted_csv(columns) %}
  {% set keys = get_quoted_csv(partition_keys) %}
  create temporary table _etl_changed on commit drop as
  select distinct {{ keys }} from (
      (select {{ cols }} from {{ target }} except select {{ cols }} from {{ incoming }})
      union all
      (select {{ cols }} from {{ incoming }} except select {{ cols }} from {{ target }})
  ) differences;
  delete from {{ target }} as old using _etl_changed as changed
  where {% for key in partition_keys %}old.{{ adapter.quote(key) }} is not distinct from changed.{{ adapter.quote(key) }}{% if not loop.last %} and {% endif %}{% endfor %};
  insert into {{ target }} ({{ cols }})
  select {% for col in columns %}new.{{ adapter.quote(col) }}{% if not loop.last %}, {% endif %}{% endfor %}
  from {{ incoming }} as new
  where exists (select 1 from _etl_changed as changed where
  {% for key in partition_keys %}new.{{ adapter.quote(key) }} is not distinct from changed.{{ adapter.quote(key) }}{% if not loop.last %} and {% endif %}{% endfor %});
  drop table _etl_changed;
{% endmacro %}

{% macro get_incremental_reconcile_sql(arg_dict) %}
{{ return(reconcile_sql(arg_dict['target_relation'], arg_dict['temp_relation'],
    arg_dict['dest_columns'] | map(attribute='name') | list,
    config.get('partition_keys', ['movie_id']))) }}
{% endmacro %}
