{% set event = var('event_action') %}
{% set stringid = var('string_id') %}

{% if event == "Next Click" and stringid == false %}
    {{ next_click_performance_articles2_tables_ytd_dbt() }}
{% elif event == 'Subscription' and stringid == false %}
    {{ subscription_performance_articles2_tables_ytd_dbt() }}
{% elif event == 'Newsletter' and stringid == false %}
    {{ newsletter_performance_articles2_tables_ytd_dbt() }}
{% elif event == 'Sales' and stringid == false %}
    {{ sales_performance_articles2_tables_ytd_dbt() }}
{% elif event == "Next Click" and stringid == true %}
    {{ string_next_click_performance_articles2_tables_ytd_dbt() }}
{% endif %}