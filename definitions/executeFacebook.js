// a function to create a query to get keyword data
function createFacebookFileQuery(schemas, site) {
  return `
	
with fb_ads_base as (
    -- joins facebook tables together for main kpis
  SELECT distinct 
      campaigns."account_id" AS account_id,
      campaigns."id" AS campaign_id,
      campaigns."name" AS campaign_name,
	  ads_insights.adset_id as adgroup_id,
	  ads_insights.adset_name as adgroup_name,
	  ads_insights.ad_id,
	  ads_insights.ad_name ,
	  ads_insights.date_start as date,
 	  --adcreative.id as creative_id,
	-- adcreative.name as creative_name,
      	  case 
					when campaigns."account_id" = '1402060363429896' then 'drugwatch.com'
					when campaigns."account_id" = '110731655701215' then 'asbestos.com'
					when campaigns."account_id" = '1376949775890008' then 'pleuralmesothelioma.com'
					when campaigns."account_id" = '124326801252369' then 'mesotheliomaprognosis.com'
						when campaigns."account_id" = '1389534567937754' then 'annuity.org'
					 else null end as account_name,
	  ads_insights.impressions as impressions,
	ads_insights.clicks as clicks,
	  ads_insights.spend as cost,
		 ads_insights.reach,
		 ads_insights.frequency,
	 ads_insights.inline_link_clicks as link_clicks



FROM ${schemas.facebook}."campaigns"
inner join ${schemas.facebook}."ads_insights" on campaigns.id = ads_insights.campaign_id 
--inner join ${schemas.facebook}."ads" on ads_insights.ad_id = ads.id
--inner join ${schemas.facebook}."adcreative" on ads.creative__id = adcreative.id
inner join ${schemas.facebook}."adsets" on ads_insights.adset_id = adsets.id

 -- Only include this line if running the Drugwatch query 
 ${typeof schemas.facebook !== 'lt__drugwatchcom_facebook' ? '' : `
	where ads_insights.adset_name ILIKE '%hernia%mesh%' `}
-- End Drugwatch-only line
),
status_setup_ads as (
select distinct
ads.id,
max(_sdc_received_at) as _sdc_received_at
from ${schemas.facebook}."ads"
group by 1
),
ads_status as (
select distinct
status_setup_ads.id,
ads.effective_status as ad_status
from status_setup_ads
inner join ${schemas.facebook}."ads" on status_setup_ads.id = ads.id and status_setup_ads._sdc_received_at = ads._sdc_received_at
),
status_setup_adsets as (
select distinct
adsets.id,
max(_sdc_received_at) as _sdc_received_at
from ${schemas.facebook}."adsets"
group by 1
),
adsets_status as (
select distinct
status_setup_adsets.id,
adsets.effective_status as adgroup_status
from status_setup_adsets
inner join ${schemas.facebook}."adsets" on status_setup_adsets.id = adsets.id and status_setup_adsets._sdc_received_at = adsets._sdc_received_at
),
status_setup_campaigns as (
select distinct
campaigns.id,
max(_sdc_received_at) as _sdc_received_at
from ${schemas.facebook}."campaigns"
group by 1
),
campaigns_status as (
select distinct
status_setup_campaigns.id,
campaigns.effective_status as campaign_status
from status_setup_campaigns
inner join ${schemas.facebook}."campaigns" on status_setup_campaigns.id = campaigns.id and status_setup_campaigns._sdc_received_at = campaigns._sdc_received_at
),
stats_base as (
    SELECT "Ads Insights Actions"."action_type" AS "at",
    "Ads Insights Actions"._sdc_source_key_ad_id as ad_id,
    "Ads Insights Actions"._sdc_source_key_adset_id as adset_id,
    "Ads Insights Actions"._sdc_source_key_campaign_id as campaign_id,
		"Ads Insights Actions"._sdc_source_key_date_start as date_start,
       SUM("Ads Insights Actions"."value") AS "val"
FROM ${schemas.facebook}."ads_insights__actions" AS "Ads Insights Actions"
group by 1,2,3,4,5
order by 6 desc
),
reach_base as (
	select distinct
ads_insights.ad_id,
max(_sdc_received_at) as _sdc_received_at
	from ${schemas.facebook}."ads_insights"
	group by 1
	),
campaign_stats as (
select distinct
stats_base.campaign_id,
stats_base.date_start,
sum(case when stats_base.at ='post_reaction' then stats_base.val else 0 end) as post_reactions_campaign,
sum(case when stats_base.at = 'link_click'then stats_base.val else 0 end) as link_clicks_campaign,
sum(case when stats_base.at = 'like'then stats_base.val else 0 end) as likes_campaign
from stats_base
group by 1,2
),
adset_stats as (
select distinct
stats_base.campaign_id,
stats_base.adset_id,
stats_base.date_start,
sum(case when stats_base.at ='post_reaction' then stats_base.val else 0 end) as post_reactions_adset,
sum(case when stats_base.at = 'link_click'then stats_base.val else 0 end) as link_clicks_adset,
sum(case when stats_base.at = 'like'then stats_base.val else 0 end) as likes_adset
from stats_base
group by 1,2,3
),
ad_stats as (
select distinct
stats_base.campaign_id,
stats_base.adset_id,
stats_base.ad_id,
stats_base.date_start,
sum(case when stats_base.at ='post_reaction' then stats_base.val else 0 end) as post_reactions_ad,
sum(case when stats_base.at = 'link_click'then stats_base.val else 0 end) as link_clicks_ad,
sum(case when stats_base.at = 'like'then stats_base.val else 0 end) as likes_ad
from stats_base

group by 1,2,3,4
  ),
fb_summed_data as(
  -- sums facebook data in preparation for join to salesforce data
select distinct 
A.date,
A.account_name,
A.account_id,
A.campaign_id,
A.campaign_name,
A.adgroup_id,
A.adgroup_name,
A.ad_id,
A.ad_name,
D.campaign_status,
C.adgroup_status,
B.ad_status,
sum(A.impressions) as impressions,
sum(A.clicks) as clicks,
sum(A.cost) as cost,
sum(A.link_clicks) as link_clicks,
sum(E.post_reactions_ad) as post_reactions_ad,
sum(E.link_clicks_ad) as link_clicks_ad,
sum(E.likes_ad) as likes_ad
/*sum(F.post_reactions_adset) as post_reactions_adgroup,
sum(F.link_clicks_adset) as link_clicks_adgroup,
sum(F.likes_adset) as likes_adgroup,
sum(G.post_reactions_campaign) as post_reactions_campaign,
sum(G.link_clicks_campaign) as link_clicks_campaign,
sum(G.likes_campaign) as likes_campaign*/

  

from fb_ads_base as A
  left join ads_status as B on A.ad_id = B.id
	left join adsets_status as C on A.adgroup_id = C.id
	left join campaigns_status as D on A.campaign_id = D.id
	left join ad_stats as E on A.ad_id = E.ad_id and A.adgroup_id = E.adset_id and A.campaign_id = E.campaign_id and A.date::date = E.date_start::date
	/*left join adset_stats as F on A.adgroup_id = F.adset_id and A.campaign_id = F.campaign_id and A.date::date = F.date_start::date
	left join campaign_stats as G on A.campaign_id = G.campaign_id and A.date::date = G.date_start::date*/

group by 1,2,3,4,5,6,7,8,9,10,11,12
)
select 
account_name,
account_id,
campaign_id,
campaign_name,
adgroup_id,
adgroup_name,
ad_id,
ad_name,
campaign_status,
adgroup_status,
ad_status,
sum(impressions) as impressions,
-- not using plain clicks
-- sum(clicks) as clicks,
sum(cost) as cost,
sum(link_clicks) as link_clicks,
sum(post_reactions_ad) as post_reactions_ad,
sum(likes_ad) as likes_ad
-- same as link_clicks 
--sum(link_clicks_ad) as link_clicks_ad,
--adgroup and campaign metrics don't seem to work 
-- sum(post_reactions_adgroup) as post_reactions_adgroup,
--sum(link_clicks_adgroup) as link_clicks_adgroup,
--sum(likes_adgroup) as likes_adgroup,
--sum(post_reactions_campaign) as post_reactions_campaign,
--sum(link_clicks_campaign) as link_clicks_campaign,
--sum(likes_campaign) as likes_campaign
from fb_summed_data
where date::date between '2020-02-01' AND '2020-02-29'
group by 1,2,3,4,5,6,7,8,9,10,11
order by 12 desc

`;
}

// a function to create an creative file table given some
// sites config parameters
function createFacebookTable(item) {
  publish(`paidpal_facebook_${item.name}`).query(
    createFacebookFileQuery(
      item.schemas,
      item.site)
  );
}

vars.config.filter(table => !!table.schemas.facebook).forEach(createFacebookTable);
