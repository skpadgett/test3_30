// a function to create a query to get keyword data
function createKeywordsFileQuery(schemas, site) {
  return `
-- This query is designed to remove AdWords Dispay campaign performance for PaidPal. The Display metrics will be calculated separately for use elsewhere and joined to the results of the below query for use outside of PaidPal.
WITH adw_base_1 AS (
SELECT DISTINCT 
    adgroup, 
    day, 
    MAX(_sdc_report_datetime) AS _sdc_report_datetime
FROM 
    ${schemas.google}.KEYWORDS_PERFORMANCE_REPORT
-- ONLY USE DATE RANGE ONCE WE DUMP ALL THE DATA INITIALLY
-- TODO: what should this do?
-- WHERE day::date BETWEEN \'".$this->start."\' and \'".$this->now."\'
 --  WHERE day::date > ${utils.refreshrange("day")}
-- ONLY USE DATE RANGE ONCE WE DUMP ALL THE DATA INITIALLY
GROUP BY 1,2
),

-- Get most adwords keyword data (excluding impressions)
adwords_keyword AS (
SELECT DISTINCT
    B.account,
    B.customerid, 
    case when B.account is not null then 'AdWords' else null end as platform,
    B.campaignid, 
    B.adgroupid, 
    B.keywordid, 
    A.adgroup, 
    B.campaign,
    B.keyword,
    SUM(B.clicks) AS clicks,
    SUM(B.cost/1000000.00) AS cost,
    B.adgroupstate,
    B.campaignstate,
    B.keywordstate,
    B.matchtype, 
    A.day,
    A._sdc_report_datetime,
    -- For some reason, asbestos calls the maxcpc column maxcpc_bigint
    -- 3/17/2021 Not anymore after migration to BigQuery
  --  MAX(B.maxcpc${(site == 'asbestos.com' || site == 'pleuralmesothelioma.com' ? '__bigint' : '')}/1000000.00) AS maxcpc
       MAX(B.maxcpc)/1000000.00 AS maxcpc
FROM adw_base_1 AS A
INNER JOIN ${schemas.google}.KEYWORDS_PERFORMANCE_REPORT AS B ON A.adgroup = B.adgroup AND A._sdc_report_datetime = B._sdc_report_datetime AND A.day = B.day
WHERE LOWER(B.campaign) NOT LIKE '%display%'
GROUP BY 
    B.account,
    B.customerid,
    B.campaignid,
    B.adgroupid,
    B.keywordid, 
    A.adgroup,
    B.campaign, 
    B.keyword,
    B.adgroupstate,
    B.campaignstate,
    B.keywordstate,
    B.matchtype,
    A.day,
    A._sdc_report_datetime
),

-- Get headline impressions
adwords_headline_impressions AS (
    SELECT DISTINCT
        B.customerid,
        B.campaignid,
        B.adgroupid,
        B.keywordid,
        A.day,
        SUM(B.impressions) AS impressions,
        A._sdc_report_datetime
    FROM adw_base_1 AS A
    INNER JOIN ${schemas.google}.KEYWORDS_PERFORMANCE_REPORT AS B ON A.adgroup = B.adgroup AND A._sdc_report_datetime = B._sdc_report_datetime AND A.day = B.day
    WHERE B.clicktype = 'Headline'
    GROUP BY 
        B.customerid,
        B.campaignid,
        B.adgroupid,
        B.keywordid,
        A.day,
        A._sdc_report_datetime
),

-- Get phone impressions
adwords_phone_impressions AS (
    SELECT DISTINCT
        B.customerid,
        B.campaignid,
        B.adgroupid,
        B.keywordid,
        A.day,
        SUM(B.impressions) AS impressions,
        A._sdc_report_datetime
    FROM adw_base_1 AS A
    INNER JOIN ${schemas.google}.KEYWORDS_PERFORMANCE_REPORT AS B ON A.adgroup = B.adgroup AND A._sdc_report_datetime = B._sdc_report_datetime AND A.day = B.day
    WHERE B.clicktype = 'Phone calls'
    GROUP BY 
        B.customerid,
        B.campaignid,
        B.adgroupid,
        B.keywordid, 
        A.day,
        A._sdc_report_datetime
),
           -- get daily campaign budget
           budget_base as (
  select distinct
  CampaignPerformanceReport.campaignid AS campaignid,
  max(CampaignPerformanceReport._sdc_report_datetime) as _sdc_report_datetime
  from ${schemas.google}.CAMPAIGN_PERFORMANCE_REPORT AS CampaignPerformanceReport
  group by 1 
  ),
budget as (
SELECT distinct
budget_base.campaignid AS campaignid,
       CampaignPerformanceReport.campaign AS campaign,
       CampaignPerformanceReport.budgetid AS budgetid,
       CampaignPerformanceReport.hasrecommendedbudget AS has_recommended_budget,
       CampaignPerformanceReport.budgetperiod AS budget_period,
     CampaignPerformanceReport.budget/1000000 AS budget
     
FROM budget_base
inner join ${schemas.google}.CAMPAIGN_PERFORMANCE_REPORT AS CampaignPerformanceReport on budget_base.campaignid = CampaignPerformanceReport.campaignid and budget_base._sdc_report_datetime = CampaignPerformanceReport._sdc_report_datetime

),

-- Aggregate all adwords data together
final_adwords AS (
    SELECT DISTINCT
        A.account,
        A.customerid,
        A.platform,
        A.campaignid,
        A.adgroupid,
        A.keywordid,
        A.adgroup,
        A.campaign,
        A.keyword,
        A.clicks,
        A.cost,
        A.adgroupstate,
        A.campaignstate,
        A.keywordstate,
        A.matchtype,
        A.day,
        COALESCE(A.maxcpc, 0) AS maxcpc,
        COALESCE(B.impressions, C.impressions) AS impressions,
        D.budget
    FROM adwords_keyword as A
    LEFT JOIN adwords_headline_impressions AS B 
    ON A.campaignid = B.campaignid AND 
        A.adgroupid = B.adgroupid AND 
        A.keywordid = B.keywordid AND 
        A.day = B.day
    LEFT JOIN adwords_phone_impressions AS C 
    ON A.campaignid = C.campaignid AND 
    A.adgroupid = C.adgroupid AND 
    A.keywordid = C.keywordid AND 
    A.day = C.day
    LEFT JOIN budget as D 
                ON A.campaignid = D.campaignid
),

-- Only include bing if it is defined in schema
${typeof schemas.bing == 'undefined' ? '' : `
-- Base bing data with most up to date reporting
bing_base_1 AS (
    SELECT DISTINCT 
        timeperiod, 
        MAX(_sdc_report_datetime) AS _sdc_report_datetime
    FROM ${schemas.bing}.keyword_performance_report
    -- ONLY USE DATE RANGE ONCE WE DUMP ALL THE DATA INITIALLY
    -- TODO: What to do here?
    -- WHERE timeperiod::date BETWEEN \\\''.$this->start.'\\\' and \\\''.$this->now.'\\\'
  WHERE (cast(timeperiod as date) BETWEEN DATE_ADD (CURRENT_DATE(), INTERVAL -30 DAY) and CURRENT_DATE())
    -- ONLY USE DATE RANGE ONCE WE DUMP ALL THE DATA INITIALLY
    GROUP BY 1
),

-- Get all bing keyword data
bing_keyword AS (
SELECT 
    B.accountname,
    B.accountid,
    CASE when B.accountid is not null then 'Bing' else null end AS platform,
    B.campaignid,
    B.adgroupid,
    B.keywordid,
    B.campaignname,
    B.adgroupname,
    B.keyword,
    B.impressions AS Impressions,
    B.clicks AS Clicks,
    B.spend AS Cost,
    B.adgroupstatus,
    B.campaignstatus,
    B.keywordstatus,
    A.timeperiod,
    A._sdc_report_datetime,
    C.dailybudget as budget
FROM bing_base_1 AS A
INNER JOIN ${schemas.bing}.keyword_performance_report AS B ON A._sdc_report_datetime = B._sdc_report_datetime AND A.timeperiod = B.timeperiod 
INNER JOIN ${schemas.bing}.campaigns as C 
  ON B.campaignid = C.id
),

-- Need to get latest keyword to check match type
latest_keyword as 
(
SELECT DISTINCT 
    campaignid,
    adgroupid,
    keywordid,
    max(timeperiod) as timeperiod
FROM ${schemas.bing}.keyword_performance_report
WHERE bidmatchtype != ''
group by 1,2,3
),

-- Need to get correct match type from Bing table 
bing_match_type AS (
SELECT DISTINCT
    A.campaignid,
    A.adgroupid,
    A.keywordid,
    A.bidmatchtype
FROM ${schemas.bing}.keyword_performance_report as A
INNER JOIN latest_keyword as C on A.campaignid = C.campaignid and A.adgroupid = C.adgroupid and A.keywordid = C.keywordid and A.timeperiod = C.timeperiod
WHERE bidmatchtype != ''
),

-- Final Bing data
final_bing AS (
SELECT
    A.accountname,
    A.accountid,
    A.platform,
    A.campaignid,
    A.adgroupid,
    A.keywordid,
    A.campaignname,
    A.adgroupname,
    A.keyword,
    A.Impressions,
    A.Clicks,
    A.Cost,
    A.adgroupstatus,
    A.campaignstatus,
    A.keywordstatus,
    A.timeperiod,
    A._sdc_report_datetime,
    B.bidmatchtype,
    A.budget
FROM bing_keyword as A
LEFT JOIN bing_match_type as B ON A.keywordid = B.keywordid
),`}

-- End bing insertion (ternary)
-- Join Adwords and Bing data (if both exist)
joined_data as (
-- Adwords final selection
SELECT 
    account,
    customerid AS account_id,
    platform,
    campaignid AS campaign_id, 
    adgroupid AS adgroup_id,
    keywordid AS keyword_id, 
    adgroup, 
    campaign, 
    keyword, 
    COALESCE(impressions, 0) AS impressions, 
    clicks,
    cost, 
    adgroupstate AS adgroup_state, 
    campaignstate AS campaign_state, 
    keywordstate AS keyword_state, 
    matchtype AS match_type, 
    day AS date, 
    maxcpc AS max_cpc,
    budget
FROM final_adwords
-- Only include bing if it is defined in schema
${typeof schemas.bing == 'undefined' ? '' : `
-- Append adwords final selection with Bing final selection 
UNION ALL
SELECT 
    accountname,
    accountid AS account_id, 
    platform, 
    campaignid AS campaign_id, 
    adgroupid AS adgroup_id, 
    keywordid AS keyword_id,
    adgroupname, 
    campaignname, 
    keyword, 
    impressions,
    clicks,
    cost,
    adgroupstatus, 
    campaignstatus, 
    keywordstatus, 
    bidmatchtype, 
    timeperiod, 
    0 AS max_cpc,
    budget
FROM final_bing`}
-- End bing insertion (ternary)
)
-- Aggregate final data
select
account_id,
campaign_id, 
adgroup_id,
keyword_id, 
cast(date as date), 
platform,
account,
adgroup, 
campaign, 
keyword,
sum(impressions) AS impressions, 
sum(clicks) as clicks,
sum(cost) as cost,
adgroup_state, 
campaign_state, 
keyword_state,  
match_type,
max(max_cpc) AS max_cpc,
budget
from joined_data
group by 1,2,3,4,5,6,7,8,9,10,14,15,16,17,19


`;
}

// a function to create an keyword file table given some
// sites config parameters
function createKeywordTable(item) {
  publish(`paidpal_ppc_keywords_${item.name}`).query(
      createKeywordsFileQuery(
        item.schemas,
        item.site)
  );
}

vars.config.filter(table => !!table.schemas.google).forEach(createKeywordTable);

/*  // a function to create an keyword file operation given some
// sites config parameters
function createKeywordFileOperation(item) {
    let table_name = `keywords_${item.name}`;
  operate(`keywords_${item.name}_unload`).queries(
    ctx => utils.unloadToS3(`select * from ${ctx.ref(table_name)}`)
  );
}

vars.config.forEach(createKeywordFileOperation);*/