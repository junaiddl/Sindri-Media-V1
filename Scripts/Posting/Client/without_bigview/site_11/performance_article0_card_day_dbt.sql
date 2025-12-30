


    

  
  
  
  


  CREATE PROCEDURE `performance_articles0_card_day_dbt_11`()
  BEGIN

  WITH last_7days_post AS (
      SELECT s.siteid as siteid,ID, s.date as Date
      FROM prod.site_archive_post s
      where s.date  between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) 
      
        AND (
  NOT 
  (
      tags REGEXP "(^|[ ,])(billedkunst|børnehaveklassen|dsa|engelsk|håndværk og design|idræt|kulturfag|lærersenior|lærerstuderende|madkundskab|musik|naturfag|plc|ppr|praktik|sosu|tyskfransk|uu)([ ,]|$)"
  )
  OR 
  (
      tags REGEXP "(^|[ ,])(billedkunst|børnehaveklassen|dsa|engelsk|håndværk og design|idræt|kulturfag|lærersenior|lærerstuderende|madkundskab|musik|naturfag|plc|ppr|praktik|sosu|tyskfransk|uu)([ ,]|$)"
      AND tags REGEXP "(^|[ ,])(dansk|it|matematik|specialpædagogik|Skolepolitik|DLF|Skoleledelse|Psykisk arbejdsmiljø|Forskning)([,]|$)"
  )
)

      
      and s.siteid = 11
  ),
  last_7_before_days_post AS (
      SELECT p.siteid as siteid,ID, p.date as Date
      FROM prod.site_archive_post p
    where p.date  between DATE_SUB(NOW(), INTERVAL 15 DAY) and DATE_SUB(NOW(), INTERVAL  8 DAY) 
    
        AND (
  NOT 
  (
      tags REGEXP "(^|[ ,])(billedkunst|børnehaveklassen|dsa|engelsk|håndværk og design|idræt|kulturfag|lærersenior|lærerstuderende|madkundskab|musik|naturfag|plc|ppr|praktik|sosu|tyskfransk|uu)([ ,]|$)"
  )
  OR 
  (
      tags REGEXP "(^|[ ,])(billedkunst|børnehaveklassen|dsa|engelsk|håndværk og design|idræt|kulturfag|lærersenior|lærerstuderende|madkundskab|musik|naturfag|plc|ppr|praktik|sosu|tyskfransk|uu)([ ,]|$)"
      AND tags REGEXP "(^|[ ,])(dansk|it|matematik|specialpædagogik|Skolepolitik|DLF|Skoleledelse|Psykisk arbejdsmiljø|Forskning)([,]|$)"
  )
)

      
      and p.siteid = 11
  ),
  last_7days_hits as(
  select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(hits),0) as hits,e.Event_Action from  last_7days_post ldp
  left join prod.events e
  on e.postid=ldp.id   and e.siteid = ldp.siteid
  and e.date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY)  and e.siteid = 11
  and e.Event_Action = 'Next Click'
  group by 1,2,3,5
  ),
  last_7days_before_hits as(
  select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(hits),0) as hits,e.Event_Action from  last_7_before_days_post ldp
  left join prod.events e
  on e.postid=ldp.id 
  and e.date between DATE_SUB(NOW(), INTERVAL 15 DAY) and DATE_SUB(NOW(), INTERVAL  8 DAY)  and e.siteid = 11
  and e.Event_Action = 'Next Click'
  group by 1,2,3,5
  ),
  last_7days_pageview as(
  select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(unique_pageviews),0) as pageviews from  last_7days_post ldp
  left join prod.pages e
  on e.postid=ldp.id and e.siteid = ldp.siteid
  and e.date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY)  and e.siteid = 11
  group by 1,2,3
  ),
  missing_post_in_pages as(
  select * from last_7days_pageview
  union
  select ldp.siteid, ldp.id as postid ,ldp.date as publish_date,0  as pageviews from last_7days_post ldp
  left join last_7days_pageview lp on lp.postid = ldp.id
  where lp.siteid is null
  ),
  last_7days_pageview_before as(
  select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(unique_pageviews),0) as pageviews  from  last_7_before_days_post ldp
  left join prod.pages e
  on e.postid=ldp.id  
  and e.date between DATE_SUB(NOW(), INTERVAL 15 DAY) and DATE_SUB(NOW(), INTERVAL  8 DAY)  and e.siteid = 11
  group by 1,2,3
  ),
  before_missing_post_in_pages as(
  select * from last_7days_pageview_before
  union
  select ldp.siteid, ldp.id as postid ,ldp.date as publish_date,0  as pageviews from last_7_before_days_post ldp
  left join last_7days_pageview_before lp on lp.postid = ldp.id
  where lp.siteid is null
  ),
  last_7days_hits_pages as(
  select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.event_action from last_7days_hits l
  left join missing_post_in_pages p on l.postid=p.postId   and l.siteid = p.siteid
  where  l.siteid = 11 
  union 
  select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,l.event_action from last_7days_hits l
  right join missing_post_in_pages p on l.postid=p.postId   and l.siteid = p.siteid
  where p.siteid = 11 
  ),
  last_7days_hits_pages_before as(
  select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.event_action from last_7days_before_hits l
  left join before_missing_post_in_pages p on l.postid=p.postId   and l.siteid = p.siteid
  where  l.siteid = 11 
  union 
  select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,l.event_action from last_7days_before_hits l
  right join before_missing_post_in_pages p on l.postid=p.postId   and l.siteid = p.siteid
  where  p.siteid = 11 
  ),
  last_7days_hits_pages_goal as (
  select l.*,g.min_pageviews,g.Min_CTA,
  case when coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,0)*100),0)<coalesce(g.Min_CTA,0)  and coalesce(l.pageviews,0)<coalesce(g.Min_pageviews,0) then 1 else 0 end as gt_goal  from last_7days_hits_pages l
  join  prod.goals g on g.date=l.publish_date and l.siteid = g.site_id
  where g.Site_ID  =11
  ),
  last_7days_hits_pages_goal_before as (
  select l.*,g.min_pageviews,g.Min_CTA,
  case when coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,0)*100),0)<coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)<coalesce(g.Min_pageviews,0) then 1 else 0 end as gt_goal 
  from last_7days_hits_pages_before l
  join  prod.goals g on g.date=l.publish_date and l.siteid = g.site_id
  where g.Site_ID  =11
  ),
  last_goal_achived_public_article as(
  select siteid,count(*) as total_publish_article,sum(gt_goal) as  value
  from last_7days_hits_pages_goal
  where   siteid = 11
  group by 1
  ),
  last_goal_achived_public_article_before as(
  select siteid,count(*) as total_publish_article,sum(gt_goal) as  value
  from last_7days_hits_pages_goal_before
  where siteid = 11
  group by 1
  )
  select 
  JSON_OBJECT(
            'data', JSON_OBJECT(
          'label', 'Artikler under på begge mål',
                'hint', 'Artikler publiceret seneste 7 dage, der er under på begge mål ',
                'value', lg.value,
                'change', coalesce(round(((lg.value-pg.value)/pg.value)*100,2),0),
                "progressCurrent", 0,
          "progressTotal", 0
            ),
            'site', lg.siteid
        ) AS json_output
  from last_goal_achived_public_article lg
  left join last_goal_achived_public_article_before pg on lg.siteid = pg.siteid;


  END


