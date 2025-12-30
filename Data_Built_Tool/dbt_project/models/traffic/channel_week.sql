{% set events = var('event_action') %}
{% set stringid = var('string_id') %}
{% if events == 'Newsletter' and stringid == false %}
    {{channel_week_dbt_newsletter()}}
{% elif events == 'Next Click' and stringid == false %}
    {{channel_week_dbt_nextclick()}}
{% elif events == 'Subscription' and stringid == false %}
    {{channel_week_dbt_subscription()}}
{% elif events == 'Sales' and stringid == false %}
    {{channel_week_dbt_sales()}}
{% elif events == 'Next Click' and stringid == true %}
    {{string_channel_week_dbt_nextclick()}}
{% endif %}