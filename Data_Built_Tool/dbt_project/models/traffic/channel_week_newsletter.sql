{% set events = var('event_action') %}
{% set stringid = var('string_id') %}
{% if events == 'Newsletter' and stringid == false %}
    {{channel_week_dbt_newsletter_news()}}
{% elif events == 'Next Click' and stringid == false %}
    {{channel_week_dbt_nextclick_news()}}
{% elif events == 'Subscription' and stringid == false %}
    {{channel_week_dbt_subscription_news()}}
{% elif events == 'Sales' and stringid == false %}
    {{channel_week_dbt_sales_news()}}
{% elif events == 'Next Click' and stringid == true %}
    {{string_channel_week_dbt_nextclick_news()}}
{% endif %}