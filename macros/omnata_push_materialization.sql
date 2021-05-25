{% materialization omnata_push, default %}
    {{ run_hooks(pre_hooks) }}

    {%- set load_task_name = this.name -%}
    {%- set job_id = invocation_id -%}
    {%- set app = config.get('app') -%}
    {%- set temp_table = 'temp_'+omnata_push.random_int(10) -%}

    {% if app=='marketing_cloud' %}
        {{ omnata_push.marketing_cloud(load_task_name,job_id) }}
    {% elif app=='salesforce' %}
        {{ omnata_push.salesforce(load_task_name,job_id) }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown Omnata Push app: " ~ app) }}         
    {% endif %}

    {{ run_hooks(post_hooks) }}

    {{ return({'relations': []}) }}
{% endmaterialization %}