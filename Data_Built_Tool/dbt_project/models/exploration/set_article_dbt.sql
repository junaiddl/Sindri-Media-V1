{% set event = var('event_action') %}
{% set stringid = var('string_id') %}


{% if event == "Next Click" and stringid == false %}
    {{ next_click_set_article_dbt() }}
{% elif event == 'Subscription' and stringid == false %}
    {{ subscription_set_article_dbt() }}
{% elif event == 'Newsletter' and stringid == false %}
    {{ newsletter_set_article_dbt() }}
{% elif event == 'Sales' and stringid == false %}
    {{ sales_set_article_dbt() }}
{% elif event == "Next Click" and stringid == true %}
    {{ string_next_click_set_article_dbt() }}
{% endif %}