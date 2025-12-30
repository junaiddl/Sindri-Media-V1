{% set event = var('event_action') %}
{% set stringid = var('string_id') %}


{% if event== "Next Click" and stringid == false %}
    {{ bigview_traffic_week_dbt()}}
{% elif event== "Newsletter" and stringid == false %}
    {{ newsletter_bigview_traffic_week_dbt()}}

{% elif event== "Subscription" and stringid == false %}
    {{ subscription_bigview_traffic_week_dbt()}}

{% elif event== "Sales" and stringid == false %}
    {{ sales_bigview_traffic_week_dbt() }}
{% elif event== "Next Click" and stringid == true %}
    {{ string_bigview_traffic_week_dbt() }}
{% endif %}

