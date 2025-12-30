


    























CREATE DEFINER=`Site`@`%` PROCEDURE `Traffic_channel_forside_ytd_dbt_16`(
    IN site INT,
    IN channel_label VARCHAR(100),
    card_label VARCHAR(100),
    card_hint VARCHAR(100),
    chartbar_label VARCHAR(100),
    chartbar_hint VARCHAR(100),
    chartbar_name VARCHAR(100),
    chartline_label VARCHAR(100),
    chartline_hint VARCHAR(100)
)
BEGIN
    SET SESSION group_concat_max_len = 100000;

    SET @sql_query = CONCAT('
        WITH 
        card_visits AS (
			SELECT
        		16 AS siteid,			
				''Forside'' as realreferrer,
				COALESCE(SUM(Hits),0) AS visits
			FROM prod.events
			where siteid = 16 and event_action = ''Frontpage''
			AND date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1)
                AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
			GROUP BY 1,2
		),

		card_visits_last_year AS (
			SELECT
				16 AS siteid,
				''Forside'' as realreferrer,
				COALESCE(SUM(Hits),0) AS last_visits
			FROM prod.events
			where siteid = 16 and event_action = ''Frontpage''
			AND date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()) - 1, 1)
					AND MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) - INTERVAL 1 DAY
			GROUP BY 1,2
		 ),
		 final_card as(
			SELECT
			''Kliks på forsiden'' AS label,
			''Antal kliks på forsiden i perioden samt udvikling ift. forrige sammenlignelige periode'' AS hint,
			COALESCE(cur_v.siteid, 16) AS siteid,
			COALESCE(cur_v.realreferrer, ''Forside'') AS realreferrer,
			COALESCE(cur_v.visits, 0) AS value,
			COALESCE(ROUND(CASE WHEN last_v.last_visits = 0 THEN 0 ELSE ((cur_v.visits - last_v.last_visits) / last_v.last_visits * 100) END, 2), 0) AS chang,
			COALESCE(cur_v.visits, 0) AS progresscurrent,
			COALESCE(cur_v.visits, 0) AS total_value
			FROM (SELECT 16 AS siteid, ''Forside'' AS realreferrer, 0 AS visits) dummy
			LEFT JOIN card_visits cur_v ON dummy.siteid = cur_v.siteid AND dummy.realreferrer = cur_v.realreferrer
			LEFT JOIN card_visits_last_year last_v ON dummy.siteid = last_v.siteid AND dummy.realreferrer = last_v.realreferrer
			GROUP BY 1,2,3,4,5,6
		),

       
       
		channels_table_real AS (
			SELECT
				siteid,
				postid AS postid,
				''Forside'' as realreferrer,
				SUM(Hits) AS visits
			FROM prod.events
			WHERE
				siteid = 16 and event_action = ''Frontpage'' and
				date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) 
            AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                and postid is not null
			GROUP BY 1, 2, 3
	    ),
		site_j AS (
			SELECT
				r.siteid,
				r.postid as ID,
				s.Title,
				s.Date,
				date(s.Modified) as updated,
				s.Link,
				userneeds
 AS userneeds, 
        		CASE
  WHEN tags LIKE ''%Løn%'' THEN REPLACE(tags, ''Løn'', ''Løn og konkrete fordele'')
  WHEN tags LIKE ''%Arbejdsmiljø%'' THEN REPLACE(tags, ''Arbejdsmiljø'', ''Arbejdsmiljø'')
  WHEN tags LIKE ''%Arbejdsliv%'' THEN REPLACE(tags, ''Arbejdsliv'', ''Det moderne arbejdsliv'')
  WHEN tags LIKE ''%Udannelse%'' THEN REPLACE(tags, ''Udannelse'', ''Uddannelse'')
  WHEN tags LIKE ''%Grøn_omstilling%'' THEN REPLACE(tags, ''Grøn_omstilling'', ''Grøn omstilling'')
  WHEN tags LIKE ''%Formanden%'' THEN REPLACE(tags, ''Formanden'', ''Formanden har ordet'')
  ELSE ''Others''
END
 AS tags,
        		Categories
 AS Categories,
                
				sum(r.visits) as visits
			FROM channels_table_real r
			LEFT JOIN prod.site_archive_post s ON  r.postid = s.id  and r.siteid = s.siteid
			WHERE s.siteid = 16 and r.realreferrer = ''Forside''
            group by 1,2,3,4,5,6, 7,  8,  9
		),
        
        table_data AS (
			select 
					ifnull(JSON_ARRAYAGG( JSON_OBJECT(
					   ''id'', ID,
						  ''article'', coalesce(Title," "),
						   ''date'', coalesce(date," "),
						   ''updated'',coalesce( updated," "),
						   ''url'', coalesce(Link," "),
                            ''category'', coalesce(userneeds, " "),
''tags'', coalesce(Tags," "),
''sektion'', coalesce(Categories, " "),

						   ''brugerbehov'', coalesce(visits,0)
					   ) ), ''[{"id": "0", "date": " ", "clicks": " " , "article": " ", "sektion": " ","BRUGERBEHOV": " ", "brugerbehov": 0}]'' )
                       as data
					   from site_j where visits >= 5
        ),
        
        DayNameTranslations_chartbar AS (
			SELECT
				''Monday'' AS english_day, ''Mandag'' AS danish_day, 1 AS day_order
			UNION ALL SELECT ''Tuesday'', ''Tirsdag'', 2
			UNION ALL SELECT ''Wednesday'', ''Onsdag'', 3
			UNION ALL SELECT ''Thursday'', ''Torsdag'', 4
			UNION ALL SELECT ''Friday'', ''Fredag'', 5
			UNION ALL SELECT ''Saturday'', ''Lørdag'', 6
			UNION ALL SELECT ''Sunday'', ''Søndag'', 7
		),
		AllDays AS (
			SELECT english_day, danish_day, day_order
			FROM DayNameTranslations_chartbar
		),
		
       visits_per_dayname AS (
			SELECT
				ad.english_day,
				DATE(tc.`date`) AS last_week_dates,
				ad.danish_day AS categories,
				COALESCE(COUNT(tc.`date`), 0) as visits,
				ad.day_order
			FROM AllDays ad
			LEFT JOIN prod.traffic_channels tc ON ad.english_day = DAYNAME(tc.`date`)
				AND date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) 
            AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
				AND siteid = 16 and realreferrer = ',channel_label,'
			GROUP BY 1,2,3,5
			ORDER BY ad.day_order 
		),
		total_visits_per_day AS
		(
			SELECT
				categories,
				SUM(visits) AS total_per_dayname
			FROM visits_per_dayname vpd
			GROUP BY categories
		),
		total_visits AS
		(
			SELECT
				SUM(total_per_dayname) AS total_visits
			FROM total_visits_per_day
		),
		percentile_chartbar AS (
			SELECT
				categories,
				COALESCE(total_per_dayname,0) AS total_per_dayname,
				COALESCE((ROUND((total_per_dayname / (SELECT total_visits FROM total_visits) * 100), 2)),0) AS series
			FROM total_visits_per_day
		),

		final_chartbar AS (
			Select
			''Ugerytme'' as label,
			''andel besøg'' as name,
            ''Fordeling af besøg på ugedage for den valgte trafikkanal'' as hint,
			CONCAT(''['', GROUP_CONCAT(''"'', categories, ''"'') , '']'') AS categories,
			CONCAT(''['', GROUP_CONCAT(series) , '']'') AS series
			from percentile_chartbar
			group by 1,2
		),
        
        AllHours AS (
			SELECT
				series,
				LPAD(h, 2, ''0'') AS hour
			FROM (
				SELECT ''Hverdag'' AS series, h FROM (SELECT 0 AS h UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23) hours
				UNION
				SELECT ''Weekend'' AS series, h FROM (SELECT 0 AS h UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10 UNION SELECT 11 UNION SELECT 12 UNION SELECT 13 UNION SELECT 14 UNION SELECT 15 UNION SELECT 16 UNION SELECT 17 UNION SELECT 18 UNION SELECT 19 UNION SELECT 20 UNION SELECT 21 UNION SELECT 22 UNION SELECT 23) hours
			) all_hours
		),
		LastYearVisits AS (
			SELECT
				siteid,
				CASE
					WHEN DAYNAME(`date`) IN (''Monday'', ''Tuesday'', ''Wednesday'', ''Thursday'', ''Friday'') THEN ''Hverdag''
					WHEN DAYNAME(`date`) IN (''Saturday'', ''Sunday'') THEN ''Weekend''
				END AS series,
				LPAD(`hour`, 2, ''0'') AS hour,
				RealReferrer,
				COALESCE(COUNT(*), 0) AS visits
			FROM prod.traffic_channels
			WHERE date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) 
            AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
				AND siteid = 16 and RealReferrer = ',channel_label,'
			GROUP BY series, hour, RealReferrer,siteid
		),

		Result AS (
			SELECT
				COALESCE(lastyear.siteid,16) as siteid,
				ah.series,
				ah.hour as categories,
				COALESCE(lastyear.RealReferrer, ''Direct'') AS RealReferrer,
				COALESCE(Visits, 0) AS Visits
			FROM AllHours ah
			LEFT JOIN LastYearVisits lastyear ON ah.series = lastyear.series AND ah.hour = lastyear.hour
			ORDER BY ah.series, ah.hour, RealReferrer
		),

		total_sum AS (
			select
			siteid,
			series,
			sum(Visits) as total_visits
			from Result
			group by 1,2
		),

		percentile as 
		(
			select 
				r.siteid , 
				r.series, 
				r.categories , 
				coalesce((round(((visits/total_visits) *100),2)),0) as percentile
			from Result r
			join total_sum f on r.siteid = f.siteid and r.series = f.series
		)
		,
		 weekend AS (
			select siteid,
			series,
			categories,
			percentile as weekend_data
			from percentile
			where series = ''Weekend''
		),
		weekday as(
			select siteid,
			series,
			categories,
			percentile as weekday_data
			from percentile
			where series = ''Hverdag''
			order by categories
		),
		weekday_percentile as(
			select
				siteid,
				group_concat(categories order by categories ) as categories,
				group_concat(weekday_data order by categories) as weekdaydata
			from  weekday
			where series = ''Hverdag''
			group by siteid
		),
		weekend_percentile AS (
			select siteid, 
				GROUP_CONCAT(CONCAT(''"'', categories, ''"'') ORDER BY categories) AS categories,
				group_concat(weekend_data order by categories) as weekenddata
			from  weekend
			where series = ''Weekend''
			group by siteid
		),
		final_chartline as (
			select
				''Døgnrytme'' as label,
				''Fordeling af besøg på timer i døgnet for hhv. hverdage og weekend for den valgte trafikkanal'' as hint,
                ''"Hverdag","Weekend"'' as series,
				weekend.categories,
                CONCAT(''['', (weekend.weekenddata) , '']'') as weekenddata,
                CONCAT(''['', (weekday.weekdaydata) , '']'') as weekdaydata
				from weekend_percentile weekend
				left join weekday_percentile weekday on weekday.siteid = weekend.siteid
                group by 1,2,3,4,5,6
		)

		SELECT
			CONCAT(
		   ''{"label":"'' ''Forside''  ''",'', 
				''"data": {'',
					''"card1": { '',
						   ''"label":"'', c1.label,
						   ''","hint":"'', c1.hint,''",''
						   ''"change":'', c1.chang,
						   '',"value":'', c1.value,
					 ''},'',
					  ''"entrypages":{'',''"columns":[{"field": "id","label": "ID"},{"field": "article","label": "ARTIKEL"},{"field": "category","label": "BRUGERBEHOV"},{"field": "tags","label": "FOKUSOMRÅDER", "hidden" : true},{"field": "date","label": "DATO"},{"field": "brugerbehov","label": "Besøg"}]
,'',''"rows":'',
							   tble.data
				   ,''},''
				   
				''"chartbar":{'',
						''"label":"'', chartbar.label,''",'',
					    ''"hint":"'', chartbar.hint,''",'',
						''"categories": '', chartbar.categories, '','',
						''"series":[ {'',
							''"name":"'', chartbar.name,''",'' 
							''"data": '', chartbar.series, '''',
					''}]'',
				''},'',
				''"chartline":{'',
					''"label":"'', cl.label,''",'',
					''"hint":"'', cl.hint,''",'',
					''"categories": ['', cl.categories, ''],'',
					''"series": ['', cl.series, ''],'',
					''"data": ['', cl.weekdaydata, '','', cl.weekenddata, '']'',
				 ''}''
			 ''}}''
			) AS data_j
		FROM final_card c1
        CROSS JOIN table_data tble
        CROSS JOIN final_chartbar chartbar
        CROSS JOIN final_chartline cl;
    
	');

    PREPARE dynamic_query FROM @sql_query;
    EXECUTE dynamic_query;
    DEALLOCATE PREPARE dynamic_query;
END

