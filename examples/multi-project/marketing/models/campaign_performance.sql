-- Campaign-level performance summary
select
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.budget,
    sum(i.clicks) as total_clicks,
    sum(i.conversions) as total_conversions,
    sum(i.spend) as total_spend,
    case
        when sum(i.clicks) > 0
        then round(sum(i.conversions) * 1.0 / sum(i.clicks), 4)
        else 0
    end as conversion_rate
from {{ ref('stg_campaigns') }} c
left join {{ ref('stg_impressions') }} i on c.campaign_id = i.campaign_id
group by c.campaign_id, c.campaign_name, c.channel, c.budget
