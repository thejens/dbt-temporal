-- Subscriber analysis with dynamic column pivoting.
-- Demonstrates: Jinja set, for loop, loop.last, countif(), string filters,
--   ref() to ephemeral intermediate, ref() to seed.
{{ config(
    tags=['reporting', 'trips']
) }}

{% set subscriber_types = ['Annual Member', 'Single Trip', 'Walk Up'] %}

select
    d.start_station_id as station_id,
    d.duration_bucket,
    count(*) as total_trips,
    {% for sub_type in subscriber_types %}
    countif(d.subscriber_type = '{{ sub_type }}')
        as {{ sub_type | lower | replace(' ', '_') }}_trips
        {{ "," if not loop.last }}
    {% endfor %}
from {{ ref('int_trip_durations') }} d
group by d.start_station_id, d.duration_bucket
