


    



















CREATE PROCEDURE `Traffic_pageview_week_dbt_11`(
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
        WHERE realreferrer IN ( ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'' ) and siteid = 11'
    );
    
        SET @categories_sql_first = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Hjælp mig med at forstå'',''Forbind mig'',''Giv mig en fordel'',''Inspirer mig'', ''Opdater mig'', ''Underhold mig'' ) and siteid = 11'
        );
    
        SET @categories_sql_second = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Artikel'', ''Seneste nyt'', ''Guide'', ''Debat'', ''Feature'' ) and siteid = 11'
        );
    
        SET @categories_sql_third = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Billedkunst'',''Børnehaveklassen'',''Dansk'',''Dansk som andetsprog'',''DSA'',''Engelsk'',''Håndværk og design'',''Idræt'',''It'',''Kulturfag'',''Madkundskab'',''Matematik'',''Musik'',''Naturfag'',''PLC'',''PPR'',''Praktik'',''Senior'',''Sosu-undervisere'',''Specialpædagogik'',''Studie'',''Tysk og fransk'',''Uddannelsesvejledning'',''UU'' ) and siteid = 11'
        );
    

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
        cms_data AS (
            SELECT id, siteid, date, tags, categories, userneeds, link
            FROM prod.site_archive_post
            WHERE siteid = 11
        ),
        

        categories_first AS (
        ', @categories_sql_first, '
        UNION ALL SELECT ''Others''
        ),

        combinations_first AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories_first c
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
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
            WHERE siteid = 11 AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'',''Search'', ''Facebook'' )
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
            WHERE siteid = 11 AND Event_Action = ''Frontpage'' 
            AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
            and postid is not null
            GROUP BY postid, siteid, event_name
        ),
        transformed_data_first AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://www.folkeskolen.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_first
        ),
        cms_groups_without_others_first as(
            SELECT siteid, id, date, "Hjælp mig med at forstå" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Hjælp mig med at forstå.*"
            UNION ALL
            
            SELECT siteid, id, date, "Forbind mig" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Forbind mig.*"
            UNION ALL
            
            SELECT siteid, id, date, "Giv mig en fordel" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Giv mig en fordel.*"
            UNION ALL
            
            SELECT siteid, id, date, "Inspirer mig" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Inspirer mig.*"
            UNION ALL
            
            SELECT siteid, id, date, "Opdater mig" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Opdater mig.*"
            UNION ALL
            
            SELECT siteid, id, date, "Underhold mig" AS tags, link
            FROM cms_data
            WHERE userneeds REGEXP ".*Underhold mig.*"
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
            and s.siteid = 11 
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
            WHERE s.Siteid = 11
        ),
        newsletter_data_first AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
            FROM prod.newsletter n
            WHERE n.site_id = 11
                AND n.date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND n.referrer_name IN ( ''NL Red'', ''NL Fag'', ''NL Dag'',''NL DLF'' ,''NL Stud'' )
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
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
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
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Hjælp mig med at forstå'',''Forbind mig'',''Giv mig en fordel'',''Inspirer mig'', ''Opdater mig'', ''Underhold mig'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Hjælp mig med at forstå'',''Forbind mig'',''Giv mig en fordel'',''Inspirer mig'', ''Opdater mig'', ''Underhold mig'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Hjælp mig med at forstå'',''Forbind mig'',''Giv mig en fordel'',''Inspirer mig'', ''Opdater mig'', ''Underhold mig'', "Others")), '']'') AS percentile
			FROM percent_first
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
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
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
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
            WHERE siteid = 11 AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'',''Search'', ''Facebook'' )
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
            WHERE siteid = 11 AND Event_Action = ''Frontpage'' 
            AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
            and postid is not null
            GROUP BY postid, siteid, event_name
        ),
        transformed_data_second AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://www.folkeskolen.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_second
        ),
        cms_groups_without_others_second as(
            SELECT siteid, id, date, "Artikel" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Artikel.*"
            UNION ALL
            
            SELECT siteid, id, date, "Seneste nyt" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Seneste nyt.*"
            UNION ALL
            
            SELECT siteid, id, date, "Guide" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Guide.*"
            UNION ALL
            
            SELECT siteid, id, date, "Debat" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Debat.*"
            UNION ALL
            
            SELECT siteid, id, date, "Feature" AS tags, link
            FROM cms_data
            WHERE Categories REGEXP ".*Feature.*"
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
            and s.siteid = 11 
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
            WHERE s.Siteid = 11
        ),
        newsletter_data_second AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
            FROM prod.newsletter n
            WHERE n.site_id = 11
                AND n.date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND n.referrer_name IN ( ''NL Red'', ''NL Fag'', ''NL Dag'',''NL DLF'' ,''NL Stud'' )
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
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
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
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Artikel'', ''Seneste nyt'', ''Guide'', ''Debat'', ''Feature'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Artikel'', ''Seneste nyt'', ''Guide'', ''Debat'', ''Feature'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Artikel'', ''Seneste nyt'', ''Guide'', ''Debat'', ''Feature'', "Others")), '']'') AS percentile
			FROM percent_second
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
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
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
        ),
        pageview_third AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = 11 AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'',''Search'', ''Facebook'' )
                 and t.postid is not null
            GROUP BY siteid, r.realreferrer, postid, date
        ),
        main_third AS (
            SELECT 
                postid,
                siteid,
                event_name,
                SUM(hits) AS t_totals
            FROM prod.events
            WHERE siteid = 11 AND Event_Action = ''Frontpage'' 
            AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
            and postid is not null
            GROUP BY postid, siteid, event_name
        ),
        transformed_data_third AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://www.folkeskolen.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_third
        ),
        cms_groups_without_others_third as(
            SELECT siteid, id, date, "Billedkunst" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Billedkunst.*"
            UNION ALL
            
            SELECT siteid, id, date, "Børnehaveklassen" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Børnehaveklassen.*"
            UNION ALL
            
            SELECT siteid, id, date, "Dansk" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Dansk.*"
            UNION ALL
            
            SELECT siteid, id, date, "Dansk som andetsprog" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Dansk som andetsprog.*"
            UNION ALL
            
            SELECT siteid, id, date, "DSA" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*DSA.*"
            UNION ALL
            
            SELECT siteid, id, date, "Engelsk" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Engelsk.*"
            UNION ALL
            
            SELECT siteid, id, date, "Håndværk og design" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Håndværk og design.*"
            UNION ALL
            
            SELECT siteid, id, date, "Idræt" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Idræt.*"
            UNION ALL
            
            SELECT siteid, id, date, "It" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*It.*"
            UNION ALL
            
            SELECT siteid, id, date, "Kulturfag" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Kulturfag.*"
            UNION ALL
            
            SELECT siteid, id, date, "Madkundskab" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Madkundskab.*"
            UNION ALL
            
            SELECT siteid, id, date, "Matematik" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Matematik.*"
            UNION ALL
            
            SELECT siteid, id, date, "Musik" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Musik.*"
            UNION ALL
            
            SELECT siteid, id, date, "Naturfag" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Naturfag.*"
            UNION ALL
            
            SELECT siteid, id, date, "PLC" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*PLC.*"
            UNION ALL
            
            SELECT siteid, id, date, "PPR" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*PPR.*"
            UNION ALL
            
            SELECT siteid, id, date, "Praktik" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Praktik.*"
            UNION ALL
            
            SELECT siteid, id, date, "Senior" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Senior.*"
            UNION ALL
            
            SELECT siteid, id, date, "Sosu-undervisere" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Sosu-undervisere.*"
            UNION ALL
            
            SELECT siteid, id, date, "Specialpædagogik" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Specialpædagogik.*"
            UNION ALL
            
            SELECT siteid, id, date, "Studie" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Studie.*"
            UNION ALL
            
            SELECT siteid, id, date, "Tysk og fransk" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Tysk og fransk.*"
            UNION ALL
            
            SELECT siteid, id, date, "Uddannelsesvejledning" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*Uddannelsesvejledning.*"
            UNION ALL
            
            SELECT siteid, id, date, "UU" AS tags, link
            FROM cms_data
            WHERE Tags REGEXP ".*UU.*"
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
            and s.siteid = 11 
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
            WHERE s.Siteid = 11
        ),
        newsletter_data_third AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
            FROM prod.newsletter n
            WHERE n.site_id = 11
                AND n.date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND n.referrer_name IN ( ''NL Red'', ''NL Fag'', ''NL Dag'',''NL DLF'' ,''NL Stud'' )
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
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
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
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Billedkunst'',''Børnehaveklassen'',''Dansk'',''Dansk som andetsprog'',''DSA'',''Engelsk'',''Håndværk og design'',''Idræt'',''It'',''Kulturfag'',''Madkundskab'',''Matematik'',''Musik'',''Naturfag'',''PLC'',''PPR'',''Praktik'',''Senior'',''Sosu-undervisere'',''Specialpædagogik'',''Studie'',''Tysk og fransk'',''Uddannelsesvejledning'',''UU'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Billedkunst'',''Børnehaveklassen'',''Dansk'',''Dansk som andetsprog'',''DSA'',''Engelsk'',''Håndværk og design'',''Idræt'',''It'',''Kulturfag'',''Madkundskab'',''Matematik'',''Musik'',''Naturfag'',''PLC'',''PPR'',''Praktik'',''Senior'',''Sosu-undervisere'',''Specialpædagogik'',''Studie'',''Tysk og fransk'',''Uddannelsesvejledning'',''UU'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Billedkunst'',''Børnehaveklassen'',''Dansk'',''Dansk som andetsprog'',''DSA'',''Engelsk'',''Håndværk og design'',''Idræt'',''It'',''Kulturfag'',''Madkundskab'',''Matematik'',''Musik'',''Naturfag'',''PLC'',''PPR'',''Praktik'',''Senior'',''Sosu-undervisere'',''Specialpædagogik'',''Studie'',''Tysk og fransk'',''Uddannelsesvejledning'',''UU'', "Others")), '']'') AS percentile
			FROM percent_third
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'', ''NL Red'', ''NL Fag'', ''NL Dag'', ''NL DLF'', ''NL Stud'', "Others")
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
							''"title": "Fag",'',
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

