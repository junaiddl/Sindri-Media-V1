{% set event = var('event_action')%}


{% if event== "Next Click" %}
    {{ ugerapport_table2_dbt_next_click()}}
    

    {% elif event== "Newsletter" %}
    {{ ugerapport_table2_dbt_newsletter()}}
    

    -- {% elif event== "Subscription" %}
    -- {{ subscription_bigview_header_week_dbt()}}

    -- {% elif event== "Sales" %}
    -- {{ sales_bigview_header_week_dbt()}}
{% endif %}