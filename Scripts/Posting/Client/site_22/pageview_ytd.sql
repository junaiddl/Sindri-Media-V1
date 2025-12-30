


    

















CREATE PROCEDURE `Traffic_pageview_ytd_dbt_22`(
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
        WHERE realreferrer IN ( ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'' ) and siteid = 22'
    );
    
        SET @categories_sql_first = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'' ) and siteid = 22'
        );
    
        SET @categories_sql_second = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Evergreen'',''Testartikel'',''Redaktionelt'',''Andet'' ) and siteid = 22'
        );
    
        SET @categories_sql_third = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Forbrugerliv'',''Test'',''Kemi'',''Privatøkonomi'',''Rådgivning'' ) and siteid = 22'
        );
    

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
         cms_data AS (
            SELECT id, siteid, date, tags, categories, userneeds, link
            FROM prod.site_archive_post_string
            WHERE siteid = 22
        ),
        

        categories_first AS (
        ', @categories_sql_first, '
        UNION ALL SELECT ''Others''
        ),

        combinations_first AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories_first c
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
        ),
        pageview_first AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels_string AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = 22 AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'', ''Search'', ''Facebook'' )
                 and t.postid is not null
            GROUP BY siteid, r.realreferrer, postid, date
        ),
        main_first AS (
            SELECT 
                postid,
                siteid,
                event_name,
                SUM(hits) AS t_totals
            FROM prod.events_string
            WHERE siteid = 22 AND Event_Action = ''Frontpage'' 
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
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://taenk.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_first
        ),
        cms_groups_without_others_first as(
            SELECT siteid, id, date, "Opdater mig" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Opdater mig.*"
            UNION ALL
            
            SELECT siteid, id, date, "Forbind mig" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Forbind mig.*"
            UNION ALL
            
            SELECT siteid, id, date, "Hjælp mig med at forstå" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Hjælp mig med at forstå.*"
            UNION ALL
            
            SELECT siteid, id, date, "Hjælp mig med at forstå" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Help mig med at forsta.*"
            UNION ALL
            
            SELECT siteid, id, date, "Hjælp mig med at forstå" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Hjælp mig med at forsta.*"
            UNION ALL
            
            SELECT siteid, id, date, "Giv mig en fordel" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Giv mig en fordel.*"
            UNION ALL
            
            SELECT siteid, id, date, "Underhold mig" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Underhold mig.*"
            UNION ALL
            
            SELECT siteid, id, date, "Inspirer mig" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Inspirer mig.*"
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
            and s.siteid = 22 
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
            WHERE s.Siteid = 22
        ),
        newsletter_data_first AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
                FROM prod.newsletter_string n
                WHERE n.site_id = 22 
                AND n.date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND n.referrer_name IN ( ''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'' )
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
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
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
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'', "Others")), '']'') AS percentile
			FROM percent_first
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
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
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
        ),
        pageview_second AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels_string AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = 22 AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'', ''Search'', ''Facebook'' )
                 and t.postid is not null
            GROUP BY siteid, r.realreferrer, postid, date
        ),
        main_second AS (
            SELECT 
                postid,
                siteid,
                event_name,
                SUM(hits) AS t_totals
            FROM prod.events_string
            WHERE siteid = 22 AND Event_Action = ''Frontpage'' 
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
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://taenk.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_second
        ),
        cms_groups_without_others_second as(
            SELECT siteid, id, date, "Evergreen" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Evergreen.*"
            UNION ALL
            
            SELECT siteid, id, date, "Testartikel" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Testartikel.*"
            UNION ALL
            
            SELECT siteid, id, date, "Redaktionelt" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Redaktionelt.*"
            UNION ALL
            
            SELECT siteid, id, date, "Andet" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Andet.*"
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
            and s.siteid = 22 
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
            WHERE s.Siteid = 22
        ),
        newsletter_data_second AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
                FROM prod.newsletter_string n
                WHERE n.site_id = 22 
                AND n.date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND n.referrer_name IN ( ''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'' )
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
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
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
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Evergreen'',''Testartikel'',''Redaktionelt'',''Andet'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Evergreen'',''Testartikel'',''Redaktionelt'',''Andet'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Evergreen'',''Testartikel'',''Redaktionelt'',''Andet'', "Others")), '']'') AS percentile
			FROM percent_second
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
		),
        final_second as(
			SELECT 	
				''Hvilken type indhold skaber engagement på tværs af trafikkanaler?'' AS label,
                ''Andelen af trafik til indgangssider for de forskellige trafikkanaler grupperet ift. indgangssidernes tags/kategorier. For Forside er artikler, der klikkes på fra forsiden i stedet for indgangssider.'' AS hint,
				categories AS cat, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile, ''}''), '']'') AS series
			FROM pivoted_data_second
			group by label,cat,hint
		), 
        

        categories_third AS (
        ', @categories_sql_third, '
        UNION ALL SELECT ''Others''
        ),

        combinations_third AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories_third c
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
        ),
        pageview_third AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels_string AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = 22 AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'', ''Search'', ''Facebook'' )
                 and t.postid is not null
            GROUP BY siteid, r.realreferrer, postid, date
        ),
        main_third AS (
            SELECT 
                postid,
                siteid,
                event_name,
                SUM(hits) AS t_totals
            FROM prod.events_string
            WHERE siteid = 22 AND Event_Action = ''Frontpage'' 
            AND date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
            and postid is not null
            GROUP BY postid, siteid, event_name
        ),
        transformed_data_third AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://taenk.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_third
        ),
        cms_groups_without_others_third as(
            SELECT siteid, id, date, "Forbrugerliv" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Forbrugerliv.*"
            UNION ALL
            
            SELECT siteid, id, date, "Test" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Test.*"
            UNION ALL
            
            SELECT siteid, id, date, "Kemi" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Kemi.*"
            UNION ALL
            
            SELECT siteid, id, date, "Privatøkonomi" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Privatøkonomi.*"
            UNION ALL
            
            SELECT siteid, id, date, "Rådgivning" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Rådgivning.*"
        ),
        cms_groups_third as (
            SELECT a.siteid, a.id, a.date, ''Others'' as tags, a.link
            FROM cms_data a
            LEFT JOIN cms_groups_without_others_third b
            ON a.siteid = b.siteid AND a.id = b.id
            WHERE b.siteid IS NULL

            UNION ALL

            SELECT * FROM cms_groups_without_others_third
        ),
        final_transformed_third AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
                p.tags
            FROM transformed_data_third s
            LEFT JOIN cms_groups_third p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = 22 
        ),
        site_archive_third AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                s.tags
            FROM cms_groups_third s
            RIGHT JOIN pageview_third p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = 22
        ),
        newsletter_data_third AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
                FROM prod.newsletter_string n
                WHERE n.site_id = 22 
                AND n.date BETWEEN  MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND n.referrer_name IN ( ''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'' )
                AND n.post_id IS NOT NULL
                GROUP BY n.site_id, n.referrer_name, n.post_id, n.date
        ),
        grouped_visits_third AS (
             SELECT
                n.site_id,
                n.post_id,
                n.referrer_name as realreferrer,
                n.visits AS visits,
                s.tags
            FROM newsletter_data_third n
            LEFT JOIN cms_groups_third s
                    ON n.post_id = s.id AND s.siteid = n.site_id
        ),
        total_data_third As(
			select * from site_archive_third
            UNION
            select * from final_transformed_third
            UNION
            select * from grouped_visits_third
        ),
        summed_data_third AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations_third c
            LEFT JOIN total_data_third s ON c.realreferrer = s.realreferrer AND c.tag = s.tags
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
        ),
        percent_third AS (
            SELECT
                realreferrer,
                tag,
                total_visits,
                COALESCE((ROUND((total_visits * 100.0) / COALESCE(SUM(total_visits) OVER (PARTITION BY realreferrer), 0), 2)), 0) AS percentile,
                ROW_NUMBER() OVER (PARTITION BY realreferrer ORDER BY tag) AS tag_order
            FROM summed_data_third
        ),
        pivoted_data_third AS (
			SELECT
				realreferrer,
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Forbrugerliv'',''Test'',''Kemi'',''Privatøkonomi'',''Rådgivning'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Forbrugerliv'',''Test'',''Kemi'',''Privatøkonomi'',''Rådgivning'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Forbrugerliv'',''Test'',''Kemi'',''Privatøkonomi'',''Rådgivning'', "Others")), '']'') AS percentile
			FROM percent_third
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'',''NL Medlem'',''NL Ikke-Medlem'',''NL Int.'', "Others")
		),
        final_third as(
			SELECT 	
				''Hvilken type indhold skaber engagement på tværs af trafikkanaler?'' AS label,
                ''Andelen af trafik til indgangssider for de forskellige trafikkanaler grupperet ift. indgangssidernes tags/kategorier. For Forside er artikler, der klikkes på fra forsiden i stedet for indgangssider.'' AS hint,
				categories AS cat, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile, ''}''), '']'') AS series
			FROM pivoted_data_third
			group by label,cat,hint
		)
        

				SELECT 
			CONCAT(
				''{'',
					''"defaultTitle": "Brugerbehov",'',
					''"label": "'', jd.label, ''",'',
					''"hint": "'', jd.hint, ''",'',
					''"categories": '', jd.cat, '','',
					''"series": '', jd.series, '','',
					''"additional": ['',
                    
						''{'',
							''"title": "Artikeltype",'',
							''"data": {'',
								''"label": "'', final_second.label, ''",'',
								''"categories": '', final_second.cat, '','',
								''"series": '', final_second.series, ''''
							''}''
						''}'' 
                    '',''
                    
						''{'',
							''"title": "Sektion",'',
							''"data": {'',
								''"label": "'', final_third.label, ''",'',
								''"categories": '', final_third.cat, '','',
								''"series": '', final_third.series, ''''
							''}''
						''}'' 
                    
                    
					'']''
				''}''
			) AS json_data
		FROM final_first jd
        
        CROSS JOIN final_second
        
        CROSS JOIN final_third
        ;
    ');

    PREPARE dynamic_query FROM @sql_query;
    EXECUTE dynamic_query;
    DEALLOCATE PREPARE dynamic_query;
END

