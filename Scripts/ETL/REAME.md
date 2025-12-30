Comparison Query:

-- Compare Events Table
with table1 as (select ROW*NUMBER() OVER (ORDER BY postid, siteid, Date, Event_Category, Event_Action, event_name,hits,batch_id) AS number, a.* from prod.events a where a.siteid = 4 and a.date = '2024-11-07' ORDER BY number),
table2 as (select ROW*NUMBER() OVER (ORDER BY postid, siteid, Date, Event_Category, Event_Action, event_name,hits,batch_id) AS number, a.* from prod.events_v2 a where a.siteid = 4 and a.date = '2024-11-07'ORDER BY number)

select \* from table1 t1 INNER JOIN table2 t2 ON t1.number = t2.number
WHERE t1.postid != t2.postid OR t1.siteid != t2.siteid OR t1.Date != t2.Date OR t1.Event_Category != t2.Event_Category
OR t1.Event_Action != t2.Event_Action OR t1.event_name != t2.event_name OR t1.hits != t2.hits;

-- Compare Pages Table
with table1 as (select ROW*NUMBER() OVER (ORDER BY id, siteid, Date, postid, unique_pageviews, URL) AS number, a.* from prod.pages a where a.siteid = 4 and a.date = '2024-11-07' ORDER BY number),
table2 as (select ROW*NUMBER() OVER (ORDER BY id, siteid, Date, postid, unique_pageviews, URL) AS number, a.* from prod.pages_v2 a where a.siteid = 4 and a.date = '2024-11-07'ORDER BY number)

select \* from table1 t1 INNER JOIN table2 t2 ON t1.number = t2.number
WHERE t1.postid != t2.postid OR t1.siteid != t2.siteid OR t1.Date != t2.Date OR t1.unique_pageviews != t2.unique_pageviews
OR t1.URL != t2.URL;

-- Compare Daily Totals
with table1 as (select ROW*NUMBER() OVER (ORDER BY id, siteid, Date, Visits, unique_pageviews) AS number, a.* from prod.daily*totals a where a.siteid = 4 and a.date = '2024-11-07' ORDER BY number),
table2 as (select ROW_NUMBER() OVER (ORDER BY id, siteid, Date, Visits, unique_pageviews) AS number, a.* from prod.daily_totals_v2 a where a.siteid = 4 and a.date = '2024-11-07'ORDER BY number)

select \* from table1 t1 INNER JOIN table2 t2 ON t1.number = t2.number
WHERE t1.siteid != t2.siteid OR t1.Date != t2.Date OR t1.unique_pageviews != t2.unique_pageviews
OR t1.Visits != t2.Visits;
