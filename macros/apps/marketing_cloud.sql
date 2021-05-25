{% macro marketing_cloud(load_task_name,job_id) -%}
    {%- set operation = config.get('operation') -%}

    {# -- First, check and see if an incremental load is running with no data - we'll skip this entirely #}
    {% set skip_load = false %}
    {% if var('full-refresh-marketing-cloud')==false %}
        {% set query_text %}
        with load_source as (
            {{ sql }}
        )
        select count(*) from load_source
        {% endset %}
        {% set load_source_count = run_query(query_text) %}
        {% set load_count = load_source_count.columns[0].values()[0] | int %}
        {% if load_count == 0 %}
            {% set skip_load = True %}
        {% endif %}
    {% endif %}

    {% if skip_load==false %}
        {# -- We have data to load #}

        {% if operation=='data_extension_upload' %}
            {{ omnata_push.marketing_cloud_data_extension_upload(job_id,load_task_name) }}
        {% endif %}

    {% else %}
        {% call statement('main') -%}
            select 1 where 0=1
        {%- endcall %}
        {{ adapter.commit() }}
    {% endif %}

{%- endmacro %}