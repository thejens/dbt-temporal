-- Intermediate: trip duration bucketing with dynamic Jinja.
-- Demonstrates: set, for loop, loop.last, ephemeral materialization.

{% set duration_buckets = [
    (0, 5, 'under_5_min'),
    (5, 15, '5_to_15_min'),
    (15, 30, '15_to_30_min'),
    (30, 60, '30_to_60_min'),
    (60, 1440, 'over_60_min')
] %}

select
    trip_id,
    start_station_id,
    end_station_id,
    duration_minutes,
    subscriber_type,
    start_time,
    case
        {% for low, high, label in duration_buckets %}
        when duration_minutes >= {{ low }} and duration_minutes < {{ high }}
            then '{{ label }}'
        {% endfor %}
        else 'unknown'
    end as duration_bucket
from {{ ref('stg_trips') }}
