// a function to create a query to get keyword data
function createCreativesFileQuery(schemas, site) {
  return `
WITH adw_base_1 AS (
                SELECT DISTINCT 
                    adgroup, 
                    day, 
                    MAX(_sdc_report_datetime) AS _sdc_report_datetime
                FROM ${schemas.google}.ad_performance_report
                -- ONLY USE DATE RANGE ONCE WE DUMP ALL THE DATA INITIALLY
                -- TODO: fix
                -- WHERE day::date BETWEEN \'".$this->start."\' and \'".$this->now."\'
                --WHERE day::date > ${utils.refreshrange("day")}
                -- ONLY USE DATE RANGE ONCE WE DUMP ALL THE DATA INITIALLY
                GROUP BY 1,2
                
            ),
            -- Get most adwords keyword data (excluding impressions)
            adwords_ad AS (
                SELECT DISTINCT
                    B.account,  
                    B.customerid,
                    CASE when B.account is not null then \'AdWords\' else null end AS platform,
                    B.campaignid,
                    B.adgroupid,
                    B.adid,
                    A.adgroup,
                    B.campaign,
                    SUM(B.clicks) AS clicks,
                    SUM(B.cost/1000000.00) AS cost,
                    B.finalurl,
                    B.headline1,
                    B.headline2,
                    -- Meso Prog and Annuity do not have the headline3 column...
                    ${(site == 'mesotheliomaprognosis.com' || site == 'annuity.org') ? 'null AS expandedtextadheadline3,' : 'B.expandedtextadheadline3,'}
                    -- Annuity sites do not have the same description format
                    ${(site == 'annuity.org' || site == 'structuredsettlements.com') ? 'TRIM(COALESCE(B.description, \'\') || \' \' || COALESCE(B.descriptionline1, \'\') || \' \' || COALESCE(B.descriptionline2, \'\'))' : 'TRIM(COALESCE(B.description, \'\') || \' \' || COALESCE(B.expandedtextaddescription2, \'\'))'} AS description,
                    B.adgroupstate,
                    B.campaignstate,
                    B.adstate,
                    A.day,
                    A._sdc_report_datetime
                FROM adw_base_1 AS A
                INNER JOIN ${schemas.google}.ad_performance_report AS B 
                ON A.adgroup = B.adgroup AND 
                A._sdc_report_datetime = B._sdc_report_datetime AND 
                A.day = B.day
                WHERE B.campaign NOT ILIKE \'%Display%\'
                GROUP BY 
                    B.account,
                    B.customerid,
                    B.campaignid,
                    B.adgroupid,
                    B.adid, 
                    A.adgroup,
                    B.campaign, 
                    B.finalurl,
                    B.headline1,
                    B.headline2,
                    expandedtextadheadline3,
                    B.description,
                    -- Annuity sites do not have the same description format
                    ${(site == 'annuity.org' || site == 'structuredsettlements.com') ?
                        'B.descriptionline1, B.descriptionline2,' :
                        'B.expandedtextaddescription2,'
                    }
                    B.adgroupstate,
                    B.campaignstate,
                    B.adstate,
                    A.day,
                    A._sdc_report_datetime
            ),
            -- Get headline impressions
            adwords_headline_impressions AS (
                SELECT DISTINCT
                    B.customerid,
                    B.campaignid,
                    B.adgroupid,
                    B.adid,
                    A.day,
                    SUM(B.impressions) AS impressions,
                    A._sdc_report_datetime
                FROM adw_base_1 AS A
                INNER JOIN ${schemas.google}.ad_performance_report AS B 
                ON A.adgroup = B.adgroup AND 
                    A._sdc_report_datetime = B._sdc_report_datetime AND 
                    A.day = B.day
                WHERE B.clicktype = 'Headline'
                GROUP BY 
                    B.customerid,
                    B.campaignid,
                    B.adgroupid,
                    B.adid, 
                    A.day,
                    A._sdc_report_datetime
            ),
            -- Get phone impressions
           adwords_phone_impressions AS (
                SELECT DISTINCT
                    B.customerid,
                    B.campaignid,
                    B.adgroupid,
                    B.adid,
                    A.day,
                    SUM(B.impressions) AS impressions,
                    A._sdc_report_datetime
                FROM adw_base_1 AS A
                INNER JOIN ${schemas.google}.ad_performance_report AS B 
                ON A.adgroup = B.adgroup AND 
                    A._sdc_report_datetime = B._sdc_report_datetime AND 
                    A.day = B.day
                WHERE B.clicktype = 'Phone calls'
                GROUP BY 
                    B.customerid,
                    B.campaignid,
                    B.adgroupid,
                    B.adid, 
                    A.day,
                    A._sdc_report_datetime
           ),
           -- get daily campaign budget
           budget_base as (
  select distinct
  "Campaign Performance Report"."campaignid" AS "Campaignid",
  max("Campaign Performance Report"._sdc_report_datetime) as _sdc_report_datetime
  from ${schemas.google}."campaign_performance_report" AS "Campaign Performance Report"
  group by 1 
  ),
budget as (
SELECT distinct
budget_base.campaignid AS campaignid,
       "Campaign Performance Report"."campaign" AS campaign,
       "Campaign Performance Report"."budgetid" AS budgetid,
       "Campaign Performance Report"."hasrecommendedbudget" AS has_recommended_budget,
       "Campaign Performance Report"."budgetperiod" AS budget_period,
     "Campaign Performance Report"."budget"/1000000 AS budget
     
FROM budget_base
inner join ${schemas.google}."campaign_performance_report" AS "Campaign Performance Report" on budget_base.campaignid = "Campaign Performance Report".campaignid and budget_base._sdc_report_datetime = "Campaign Performance Report"._sdc_report_datetime

),
            -- Aggregate all adwords data together
            final_adwords AS (
                SELECT DISTINCT
                    A.account,
                    A.customerid,
                    A.platform,
                    A.campaignid,
                    A.adgroupid,
                    A.adid,
                    A.adgroup,
                    A.campaign,
                    A.clicks,
                    A.cost,
                    A.finalurl,
                    A.headline1,
                    A.headline2,
                    A.expandedtextadheadline3,
                    A.description,
                    A.adgroupstate,
                    A.campaignstate,
                    A.adstate,
                    A.day,
                    COALESCE(B.impressions, C.impressions) AS impressions,
                    D.budget
                FROM adwords_ad AS A
                LEFT JOIN adwords_headline_impressions AS B 
                ON A.campaignid = B.campaignid 
                AND A.adgroupid = B.adgroupid 
                AND A.adid = B.adid 
                AND A.day = B.day
                LEFT JOIN adwords_phone_impressions AS C 
                ON A.campaignid = C.campaignid 
                AND A.adgroupid = C.adgroupid 
                AND A.adid = C.adid 
                AND A.day = C.day
                LEFT JOIN budget as D 
                ON A.campaignid = D.campaignid
            ),
            -- Only include bing if it is defined in schema
            ${typeof schemas.bing == 'undefined' ? '' : `
            -- Base bing data with most up to date reporting 
            bing_base_1 AS (
                SELECT DISTINCT 
                    timeperiod,
                    max(_sdc_report_datetime) AS _sdc_report_datetime
                FROM ${schemas.bing}.ad_performance_report
                -- ONLY USE DATE RANGE ONCE WE DUMP ALL THE DATA INITIALLY
               -- TODO: fix
                -- WHERE timeperiod::date BETWEEN \\\''.$this->start.'\\\' and \\\''.$this->now.'\\\'
               -- WHERE timeperiod::date > ${utils.refreshrange("timeperiod")}
                -- ONLY USE DATE RANGE ONCE WE DUMP ALL THE DATA INITIALLY
                GROUP BY 1
           ),
            -- Get all bing keyword data
            bing_ad AS (
                SELECT 
                    B.accountname,
                    B.accountid,
                    CASE when B.accountid is not null then 'Bing' else null end AS platform,
                    B.campaignid,
                    B.adgroupid,
                    B.adid,
                    B.campaignname,
                    B.adgroupname,
                    B.impressions AS Impressions,
                    B.clicks as Clicks,
                    B.spend as Cost,
                    B.finalurl,
                    B.titlepart1,
                    B.titlepart2,
                    B.addescription,  
                    B.adgroupstatus,
                    B.campaignstatus,
                    B.adstatus,  
                    A.timeperiod,
                    A._sdc_report_datetime,
                    C.dailybudget as budget
                FROM bing_base_1 as A
                INNER JOIN ${schemas.bing}.ad_performance_report AS B 
                ON A._sdc_report_datetime = B._sdc_report_datetime 
                AND A.timeperiod = B.timeperiod 
                INNER JOIN ${schemas.bing}.campaigns as C 
                ON B.campaignid = C.id
            ),
            
            `}
            -- End bing insertion (ternary)
            joined_data as (
                -- Adwords final selection
                SELECT 
                    account,  
                    customerid AS account_id,
                    platform,
                    campaignid AS campaign_id,
                    adgroupid AS adgroup_id,
                    adid AS creative_id,
                    adgroup,
                    campaign,
                    COALESCE(impressions, 0) AS impressions, 
                    clicks,
                    cost,
                    finalurl AS url,
                    headline1,
                    headline2,
                    expandedtextadheadline3 AS headline3,
                    description,
                    adgroupstate AS adgroup_state,
                    campaignstate AS campaign_state,
                    adstate AS creative_state,
                    day AS date,
                    budget
                FROM final_adwords
                -- Only include bing if it is defined in schema
                ${typeof schemas.bing == 'undefined' ? '' : `
                -- Append adwords final selection with bing final selection
                UNION ALL
                SELECT
                    accountname,
                    accountid,
                    platform,
                    campaignid,
                    adgroupid,
                    adid,
                    adgroupname,
                    campaignname,
                    impressions,
                    clicks,
                    cost,
                    finalurl,
                    titlepart1,
                    titlepart2,
                    null as headline3,
                    addescription,  
                    adgroupstatus,
                    campaignstatus,
                    adstatus, 
                    timeperiod,
                    budget
                FROM bing_ad
                `}
                -- End bing insertion (ternary)
            )
            -- Aggregate final data
            select
            account_id,
            campaign_id,
            adgroup_id,
            creative_id,
            date::date,
            platform,
            account,
            adgroup,
            campaign,
            sum(impressions) as impressions, 
            sum(clicks) as clicks,
            sum(cost) as cost,
            url,
            headline1,
            headline2,
            headline3,
            description,
            adgroup_state,
            campaign_state,
            creative_state,
            budget
            from joined_data
           
            group by 1,2,3,4,5,6,7,8,9,13,14,15,16,17,18,19,20,21
           
`;
}

// a function to create an creative file table given some
// sites config parameters
function createCreativeTable(item) {
  publish(`paidpal_ppc_creatives_${item.name}`).query(
      createCreativesFileQuery(
        item.schemas,
        item.site)
  );
}

vars.config.filter(table => !!table.schemas.google).forEach(createCreativeTable);

/* // a function to create an creative file operation given some
// sites config parameters
function createCreativeFileOperation(item) {
    let table_name = `creatives_${item.name}`;
  operate(`creatives_${item.name}_unload`).queries(
    ctx => utils.unloadToS3(`select * from ${ctx.ref(table_name)}`)
  );
} 

vars.config.forEach(createCreativeFileOperation);
*/