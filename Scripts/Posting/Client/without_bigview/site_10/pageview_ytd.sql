


    

















CREATE PROCEDURE `Traffic_pageview_ytd_dbt_10`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement_first  VARCHAR(2000),
    IN domain VARCHAR(200)
)
BEGIN
    SET SESSION group_concat_max_len = 1000000;

    SET @referrers_sql = CONCAT(
        'SELECT DISTINCT realreferrer FROM pre_stage.ref_value
        WHERE realreferrer IN ( ''Forside'',''Search'', ''Facebook'',''X'',''Newsletter'' ) and siteid = 10'
    );
    
        SET @categories_sql_first = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Nyheder'',''Leder'',''Opinion'',''Analyse'',''Reportage'' ) and siteid = 10'
        );
    
        SET @categories_sql_second = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'' ) and siteid = 10'
        );
    

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
         cms_data AS (
            SELECT id, siteid, date, tags, categories, userneeds, link
            FROM prod.site_archive_post
            WHERE siteid = 10
        ),
        

        categories_first AS (
        ', @categories_sql_first, '
        UNION ALL SELECT ''Others''
        ),

        combinations_first AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories_first c
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'', ''Facebook'',''X'',''Newsletter'', "Others")
        ),
        pageview_first AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = 10 AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND r.realreferrer IN (  )
                 and t.postid is not null
            GROUP BY siteid, r.realreferrer, postid, date
        ),
        main_first AS (
            SELECT 
                postid,
                siteid,
                event_name,
                SUM(hits) AS t_totals
            FROM prod.events
            WHERE siteid = 10 AND Event_Action = ''Frontpage'' 
            AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
            and postid is not null
            GROUP BY postid, siteid, event_name
        ),
        transformed_data_first AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://fagbladet3f.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_first
        ),
        cms_groups_without_others_first as(
            SELECT siteid, id, date, "Fodsundhed og samfund" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Fodsundhed og samfund.*"
            UNION ALL
            
            SELECT siteid, id, date, "Forebyggelse" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Forebyggelse.*"
            UNION ALL
            
            SELECT siteid, id, date, "Forskning og viden" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Forskning og viden.*"
            UNION ALL
            
            SELECT siteid, id, date, "Praksis" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Praksis.*"
        ),
        cms_groups_first as (
            SELECT a.siteid, a.id, a.date, ''Others'' as tags, a.link
            FROM cms_data a
            LEFT JOIN cms_groups_without_others_first b
            ON a.siteid = b.siteid AND a.id = b.id
            WHERE b.siteid IS NULL

            UNION ALL

            SELECT * FROM cms_groups_without_others_first
        ),
        final_transformed_first AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
                p.tags
            FROM transformed_data_first s
            LEFT JOIN cms_groups_first p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = 10 
        ),
        site_archive_first AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                s.tags
            FROM cms_groups_first s
            RIGHT JOIN pageview_first p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = 10
        ),
        newsletter_data_first AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
                FROM prod.newsletter n
                WHERE n.site_id = 10 
                AND n.date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND n.referrer_name IN (  )
                AND n.post_id IS NOT NULL
                GROUP BY n.site_id, n.referrer_name, n.post_id, n.date
        ),
        grouped_visits_first AS (
             SELECT
                n.site_id,
                n.post_id,
                n.referrer_name as realreferrer,
                n.visits AS visits,
                s.tags
            FROM newsletter_data_first n
            LEFT JOIN cms_groups_first s
                    ON n.post_id = s.id AND s.siteid = n.site_id
        ),
        total_data_first As(
			select * from site_archive_first
            UNION
            select * from final_transformed_first
            UNION
            select * from grouped_visits_first
        ),
        summed_data_first AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations_first c
            LEFT JOIN total_data_first s ON c.realreferrer = s.realreferrer AND c.tag = s.tags
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'', ''Facebook'',''X'',''Newsletter'', "Others")
        ),
        percent_first AS (
            SELECT
                realreferrer,
                tag,
                total_visits,
                COALESCE((ROUND((total_visits * 100.0) / COALESCE(SUM(total_visits) OVER (PARTITION BY realreferrer), 0), 2)), 0) AS percentile,
                ROW_NUMBER() OVER (PARTITION BY realreferrer ORDER BY tag) AS tag_order
            FROM summed_data_first
        ),
        pivoted_data_first AS (
			SELECT
				realreferrer,
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Nyheder'',''Leder'',''Opinion'',''Analyse'',''Reportage'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Nyheder'',''Leder'',''Opinion'',''Analyse'',''Reportage'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Nyheder'',''Leder'',''Opinion'',''Analyse'',''Reportage'', "Others")), '']'') AS percentile
			FROM percent_first
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'', ''Facebook'',''X'',''Newsletter'', "Others")
		),
        final_first as(
			SELECT 	
				''Hvilken type indhold skaber engagement på tværs af trafikkanaler?'' AS label,
                ''Andelen af trafik til indgangssider for de forskellige trafikkanaler grupperet ift. indgangssidernes tags/kategorier. For Forside er artikler, der klikkes på fra forsiden i stedet for indgangssider.'' AS hint,
				categories AS cat, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile, ''}''), '']'') AS series
			FROM pivoted_data_first
			group by label,cat,hint
		), 
        

        categories_second AS (
        ', @categories_sql_second, '
        UNION ALL SELECT ''Others''
        ),

        combinations_second AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories_second c
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'', ''Facebook'',''X'',''Newsletter'', "Others")
        ),
        pageview_second AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = 10 AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND r.realreferrer IN (  )
                 and t.postid is not null
            GROUP BY siteid, r.realreferrer, postid, date
        ),
        main_second AS (
            SELECT 
                postid,
                siteid,
                event_name,
                SUM(hits) AS t_totals
            FROM prod.events
            WHERE siteid = 10 AND Event_Action = ''Frontpage'' 
            AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
            and postid is not null
            GROUP BY postid, siteid, event_name
        ),
        transformed_data_second AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://fagbladet3f.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_second
        ),
        cms_groups_without_others_second as(
            SELECT siteid, id, date, "Hjælp mig med at forstå" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Hjælp mig med at forstå.*"
            UNION ALL
            
            SELECT siteid, id, date, "Inspirer mig" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Inspirer mig.*"
            UNION ALL
            
            SELECT siteid, id, date, "Giv mig en fordel" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Giv mig en fordel.*"
        ),
        cms_groups_second as (
            SELECT a.siteid, a.id, a.date, ''Others'' as tags, a.link
            FROM cms_data a
            LEFT JOIN cms_groups_without_others_second b
            ON a.siteid = b.siteid AND a.id = b.id
            WHERE b.siteid IS NULL

            UNION ALL

            SELECT * FROM cms_groups_without_others_second
        ),
        final_transformed_second AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
                p.tags
            FROM transformed_data_second s
            LEFT JOIN cms_groups_second p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = 10 
        ),
        site_archive_second AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                s.tags
            FROM cms_groups_second s
            RIGHT JOIN pageview_second p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = 10
        ),
        newsletter_data_second AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
                FROM prod.newsletter n
                WHERE n.site_id = 10 
                AND n.date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND n.referrer_name IN (  )
                AND n.post_id IS NOT NULL
                GROUP BY n.site_id, n.referrer_name, n.post_id, n.date
        ),
        grouped_visits_second AS (
             SELECT
                n.site_id,
                n.post_id,
                n.referrer_name as realreferrer,
                n.visits AS visits,
                s.tags
            FROM newsletter_data_second n
            LEFT JOIN cms_groups_second s
                    ON n.post_id = s.id AND s.siteid = n.site_id
        ),
        total_data_second As(
			select * from site_archive_second
            UNION
            select * from final_transformed_second
            UNION
            select * from grouped_visits_second
        ),
        summed_data_second AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations_second c
            LEFT JOIN total_data_second s ON c.realreferrer = s.realreferrer AND c.tag = s.tags
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'', ''Facebook'',''X'',''Newsletter'', "Others")
        ),
        percent_second AS (
            SELECT
                realreferrer,
                tag,
                total_visits,
                COALESCE((ROUND((total_visits * 100.0) / COALESCE(SUM(total_visits) OVER (PARTITION BY realreferrer), 0), 2)), 0) AS percentile,
                ROW_NUMBER() OVER (PARTITION BY realreferrer ORDER BY tag) AS tag_order
            FROM summed_data_second
        ),
        pivoted_data_second AS (
			SELECT
				realreferrer,
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'', "Others")), '']'') AS percentile
			FROM percent_second
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'', ''Facebook'',''X'',''Newsletter'', "Others")
		),
        final_second as(
			SELECT 	
				''Hvilken type indhold skaber engagement på tværs af trafikkanaler?'' AS label,
                ''Andelen af trafik til indgangssider for de forskellige trafikkanaler grupperet ift. indgangssidernes tags/kategorier. For Forside er artikler, der klikkes på fra forsiden i stedet for indgangssider.'' AS hint,
				categories AS cat, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile, ''}''), '']'') AS series
			FROM pivoted_data_second
			group by label,cat,hint
		)
        

				SELECT 
			CONCAT(
				''{'',
					''"defaultTitle": "Kategorier",'',
					''"label": "'', jd.label, ''",'',
					''"hint": "'', jd.hint, ''",'',
					''"categories": '', jd.cat, '','',
					''"series": '', jd.series, '','',
					''"additional": ['',
                    
						''{'',
							''"title": "Brugerbehov",'',
							''"data": {'',
								''"label": "'', final_second.label, ''",'',
								''"categories": '', final_second.cat, '','',
								''"series": '', final_second.series, ''''
							''}''
						''}'' 
                    
                    
					'']''
				''}''
			) AS json_data
		FROM final_first jd
        
        CROSS JOIN final_second
        ;
    ');

    PREPARE dynamic_query FROM @sql_query;
    EXECUTE dynamic_query;
    DEALLOCATE PREPARE dynamic_query;
END

