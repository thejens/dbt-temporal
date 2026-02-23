{% docs station_status %}
The operational status of a bikeshare station:

- **active**: Station is currently operational and accepting bikes
- **closed**: Station has been permanently or temporarily closed
- **moved**: Station has been relocated to a new position
- **ACL**: Station operates under access control restrictions
{% enddocs %}

{% docs trip_duration_minutes %}
The total duration of the trip in minutes, measured from bike checkout to return.

Trips exceeding 24 hours (1440 minutes) are excluded as they likely
indicate lost or stolen bikes rather than legitimate usage.
{% enddocs %}

{% docs audit_invocation_id %}
The unique identifier for the dbt invocation that created or last updated this row.
Useful for debugging data lineage and identifying which run produced specific records.
{% enddocs %}

{% docs audit_run_started_at %}
The UTC timestamp when the dbt run that produced this row was initiated.
{% enddocs %}
