{% macro channel_month_dbt_newsletter() %}

{% set siteid = var('site') %}

{% set dropdown_val = var('Tendency').dropdown_values %}
{% set dropdown_val_m = var('Tendency').dropdown_values_grouping %}

{% set dropdown_mapping = var('Tendency').dropdown_mapping %}
{% set dropdown_titles = var('Tendency').dropdown_titles %}


{% set default_columns = var('Traffic').table_grouping.default_columns %}
{% set grouping_status = var('Traffic').table_grouping.grouping_status %}
{% set grouping_columns = var('Traffic').table_grouping.grouping_columns %}
{% set table_json = var('Traffic').table_grouping.table_json %}
{% set total_columns = var('Traffic').table_grouping.total_columns %}


{% set dropdown_count = var('Tendency').dropdown_mapping %}
{% set entry_pages_json = var('Traffic').entry_pages_json %}
{% set table_modification = var('table_modification').table_modification_traffic %}




CREATE PROCEDURE `Traffic_channel_month_dbt_{{siteid}}`(
    IN site INT,
    IN channel_label VARCHAR(200),
    card_label VARCHAR(100),
    card_hint VARCHAR(100),
    chartbar_label VARCHAR(100),
    chartbar_name VARCHAR(100),
    chartbar_hint VARCHAR(100),
    chartline_label VARCHAR(100),
    chartline_hint VARCHAR(100)
)
BEGIN
    SET SESSION group_concat_max_len = 100000;

    SET @sql_query = CONCAT('
        WITH 
        referrer AS (
			SELECT realreferrer,referrer_name
			FROM pre_stage.ref_value 
			WHERE realreferrer = ',channel_label,'
			AND siteid = {{siteid}}
		),
        card_visits AS (
            SELECT
				tc.siteid,
				rc.realreferrer,
				COALESCE(COUNT(tc.realreferrer), 0) AS visits
			FROM referrer rc
			LEFT JOIN prod.traffic_channels tc ON
				(rc.realreferrer = tc.realreferrer OR rc.referrer_name = tc.realreferrer)
				AND tc.siteid = {{siteid}}
				AND tc.date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
			GROUP BY 1,2
        ),
        card_visits_last_month AS (
            SELECT
				tc.siteid,
				rc.realreferrer,
				COALESCE(COUNT(tc.realreferrer), 0) AS last_visits
			FROM referrer rc
			LEFT JOIN prod.traffic_channels tc ON
				(rc.realreferrer = tc.realreferrer OR rc.referrer_name = tc.realreferrer)
				AND tc.siteid = {{siteid}}
				AND tc.date BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY)
			GROUP BY 1,2
         ),
         final_card as(
           SELECT
              ''Besøg'' as label,
              ''Antal besøg fra valgte trafikkanal samt udvikling ift. forrige sammenlignelige periode'' as hint,
              cur_v.siteid,
              cur_v.realreferrer,
              COALESCE(cur_v.visits,0) AS value,
              COALESCE(ROUND(((cur_v.visits - last_v.last_visits) / last_v.last_visits * 100), 2), 0) AS chang,
              COALESCE(cur_v.visits,0) AS progresscurrent,
              COALESCE(cur_v.visits,0) AS total_value
          FROM card_visits cur_v
          left JOIN card_visits_last_month last_v on cur_v.siteid  = last_v.siteid and cur_v.realreferrer = last_v.realreferrer
          group by 1,2,3,4,6,5
        ),
		channels_table_real AS (
			SELECT
				siteid,
				postid AS postid,
				Date,
				realreferrer,
				COUNT(*) AS visits
			FROM prod.traffic_channels 
			WHERE
				siteid = {{siteid}} and
				realreferrer = ',channel_label,' and postid is not  null and
				date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
			GROUP BY 1, 2, 3, 4
		),
		site_j AS (
			SELECT
				r.siteid,
				r.postid as ID,
				s.Title,
				s.Date,
				date(s.Modified) as updated,
				s.Link,
				{{ table_modification['userneeds'] }} AS userneeds, 
        		{{ table_modification['tags'] }} AS tags,
        		{{ table_modification['Categories'] }} AS Categories,
                {% if grouping_status %}
                    {% for i in range(grouping_columns | length) %}
                        CASE
                            {%- for j in range(dropdown_val[dropdown_count[grouping_columns[i]]] | length) %}
                            WHEN s.{{ dropdown_count[grouping_columns[i]] }} REGEXP ".*{{dropdown_val[dropdown_count[grouping_columns[i]]][j]}}.*" THEN "{{ dropdown_val_m[dropdown_count[grouping_columns[i]]][j] }}"
                            {%- endfor %}
                        END AS {{grouping_columns[i]}},
                    {% endfor %}
                {% endif %}
				sum(r.visits) as visits
			FROM channels_table_real r
			LEFT JOIN prod.site_archive_post s ON  r.postid = s.id  and r.siteid = s.siteid
			WHERE s.siteid = {{siteid}} and r.realreferrer = ',channel_label,'
			AND visits >= 5
            group by 1,2,3,4,5,6,7
		),
        table_data AS (
			select 
				ifnull(JSON_ARRAYAGG( JSON_OBJECT(
				   ''id'', ID,
					  ''article'', coalesce(Title," "),
					   ''date'', coalesce(date," "),
					   ''updated'',coalesce( updated," "),
					   ''url'', coalesce(Link," "),
                       {{table_json}}
					   ''brugerbehov'', coalesce(visits,0)
				   ) ), ''[{"id": "{{siteid}}", "date": " ", "clicks": " " , "article": " ", "sektion": " ","BRUGERBEHOV": " ", "brugerbehov": 0}]'' )
                as data
				from site_j
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
			  DATE(tc.`date`) AS last_month_dates,
			  ad.danish_day AS categories,
			  dayname(date) AS day_of_month,
			  realreferrer,
			  COALESCE(Count(tc.date),0) as visits
			FROM AllDays ad
			JOIN prod.traffic_channels tc ON ad.english_day = DAYNAME(tc.`date`)
			  AND date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
			  AND siteid = {{siteid}}
			  AND realreferrer = ',channel_label,'
			GROUP BY 1,2,3,4,5
		),
		total_visits_per_day AS (
			SELECT
				dnt.danish_day AS categories,
				SUM(visits) AS visits_per_dayname
			FROM visits_per_dayname vpd
			RIGHT JOIN DayNameTranslations_chartbar dnt ON vpd.day_of_month = dnt.english_day
			GROUP BY 1
		),
        total_visits AS (
			SELECT
				SUM(visits_per_dayname) AS total_visits
			FROM total_visits_per_day
		),
		percentile_chartbar AS (
			SELECT
				categories,
				COALESCE(visits_per_dayname,0) AS visits_per_dayname,
				COALESCE((ROUND((visits_per_dayname / (SELECT total_visits FROM total_visits) * 100), 2)),0) AS series
			FROM total_visits_per_day
		),
		final_chartbar AS (
			SELECT
				''andel besøg'' as name,
				''Ugerytme'' AS label,
				''Fordeling af besøg på ugedage for den valgte trafikkanal'' AS hint,
				CONCAT(''['', GROUP_CONCAT(''"'', categories, ''"'') , '']'') AS categories,
				CONCAT(''['', GROUP_CONCAT(series) , '']'') AS series
			FROM percentile_chartbar
			GROUP BY 1, 2
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
		LastWeekVisits AS (
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
			WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
				AND siteid = {{siteid}}
				AND RealReferrer IN (',channel_label,')
			GROUP BY series, hour, RealReferrer,siteid
		),
		Result AS (
			SELECT
				COALESCE(lastweek.siteid,{{siteid}}) as siteid,
				ah.series,
				ah.hour as categories,
				COALESCE(lastweek.RealReferrer, ',channel_label,') AS RealReferrer,
				COALESCE(Visits, 0) AS Visits
			FROM AllHours ah
			LEFT JOIN LastWeekVisits lastweek ON ah.series = lastweek.series AND ah.hour = lastweek.hour
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
		percentile as (
			select 
				r.siteid , 
				r.series, 
				r.categories , 
				coalesce((round(((visits/total_visits) *100),2)),0) as percentile
			from Result r
			join total_sum f on r.siteid = f.siteid and r.series = f.series
		),
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
		   ''{"label":"'' ',channel_label,'  ''",'', 
				''"data": {'',
					''"card1": { '',
						   ''"label":"'', c1.label,
						   ''","hint":"'', c1.hint,''",''
						   ''"change":'', c1.chang,
						   '',"value":'', c1.value,
					 ''},'',
					  ''"entrypages":{'',''"columns":{{entry_pages_json}},'',''"rows":'',
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
{% endmacro %}