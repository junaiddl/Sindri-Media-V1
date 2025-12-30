{% set events = var('event_action') %}
{% set stringid = var('string_id') %}

{% if events == 'Newsletter' and stringid == false %}
    {{dynamic_linequery_month_dbt_newsletter()}}
{% elif events == 'Next Click' and stringid == false %}
    {{dynamic_linequery_month_dbt_nextclick()}}
{% elif events == 'Subscription' and stringid == false %}
    {{dynamic_linequery_month_dbt_subscription()}}
{% elif events == 'Sales' and stringid == false %}
    {{dynamic_linequery_month_dbt_sales()}}
{% elif events == 'Next Click' and stringid == true %}
    {{string_dynamic_linequery_month_dbt_nextclick()}}
{% endif %}