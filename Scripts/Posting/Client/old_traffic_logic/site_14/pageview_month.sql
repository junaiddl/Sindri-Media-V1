


    
















CREATE PROCEDURE `Traffic_pageview_month_dbt_14`(
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
        WHERE realreferrer IN ( ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'' ) and siteid = 14'
    );
    
        SET @categories_sql_first = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Opdater mig'',''Forbind mig'',''Hjælp mig med at forstå'',''Giv mig en fordel'',''Underhold mig'',''Inspirer mig'' ) and siteid = 14'
        );
    
        SET @categories_sql_second = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Debat'',''Artikel'',''Nyhedsoverblik'',''Guide'',''Digidoc'' ) and siteid = 14'
        );
    
        SET @categories_sql_third = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( ''Arbejdsmiljø'', ''Social dumping'',''Ulighed'',''Privatøkonomi'',''Fagligt'',''Uddannelse'' ) and siteid = 14'
        );
    

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
        

        categories_first AS (
        ', @categories_sql_first, '
        UNION ALL SELECT ''Others''
        ),

        combinations_first AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories_first c
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
            WHERE siteid = 14 AND date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'' )
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
            WHERE siteid = 14 AND Event_Action = ''Frontpage'' 
            AND date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
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
        final_transformed_first AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
            CASE
				WHEN userneeds REGEXP ".*Opdater mig.*" THEN "Opdater mig"
				WHEN userneeds REGEXP ".*Forbind mig.*" THEN "Forbind mig"
				WHEN userneeds REGEXP ".*Hjælp mig med at forstå.*" THEN "Hjælp mig med at forstå"
				WHEN userneeds REGEXP ".*Giv mig en fordel.*" THEN "Giv mig en fordel"
				WHEN userneeds REGEXP ".*Underhold mig.*" THEN "Underhold mig"
				WHEN userneeds REGEXP ".*Inspirer mig.*" THEN "Inspirer mig"
                ELSE ''others''
			END AS tags
            FROM transformed_data_first s
            LEFT JOIN prod.site_archive_post p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = 14 
        ),
        site_archive_first AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                CASE
                    WHEN userneeds REGEXP ".*Opdater mig.*" THEN "Opdater mig"
                    WHEN userneeds REGEXP ".*Forbind mig.*" THEN "Forbind mig"
                    WHEN userneeds REGEXP ".*Hjælp mig med at forstå.*" THEN "Hjælp mig med at forstå"
                    WHEN userneeds REGEXP ".*Giv mig en fordel.*" THEN "Giv mig en fordel"
                    WHEN userneeds REGEXP ".*Underhold mig.*" THEN "Underhold mig"
                    WHEN userneeds REGEXP ".*Inspirer mig.*" THEN "Inspirer mig"
                    ELSE ''others''
                END AS tags
            FROM prod.site_archive_post s
            RIGHT JOIN pageview_first p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = 14
        ),
        total_data_first As(
			select * from site_archive_first
            UNION
            select * from final_transformed_first
        ),
        summed_data_first AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations_first c
            LEFT JOIN total_data_first s ON c.realreferrer = s.realreferrer AND c.tag = s.tags
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
            WHERE siteid = 14 AND date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'' )
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
            WHERE siteid = 14 AND Event_Action = ''Frontpage'' 
            AND date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
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
        final_transformed_second AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
            CASE
				WHEN Categories REGEXP ".*Nyhedsoverblik.*" THEN "Nyhedsoverblik"
				WHEN Categories REGEXP ".*Debat.*" THEN "Debat"
				WHEN Categories REGEXP ".*Guide.*" THEN "Guide"
				WHEN Categories REGEXP ".*Artikel.*" THEN "Artikel"
				WHEN Categories REGEXP ".*Digidoc.*" THEN "Digidoc"
				WHEN Categories REGEXP ".*post.*" THEN "post"
				WHEN Categories REGEXP ".*news_overview.*" THEN "news_overview"
                ELSE ''others''
			END AS tags
            FROM transformed_data_second s
            LEFT JOIN prod.site_archive_post p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = 14 
        ),
        site_archive_second AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                CASE
                    WHEN Categories REGEXP ".*Nyhedsoverblik.*" THEN "Nyhedsoverblik"
                    WHEN Categories REGEXP ".*Debat.*" THEN "Debat"
                    WHEN Categories REGEXP ".*Guide.*" THEN "Guide"
                    WHEN Categories REGEXP ".*Artikel.*" THEN "Artikel"
                    WHEN Categories REGEXP ".*Digidoc.*" THEN "Digidoc"
                    WHEN Categories REGEXP ".*post.*" THEN "post"
                    WHEN Categories REGEXP ".*news_overview.*" THEN "news_overview"
                    ELSE ''others''
                END AS tags
            FROM prod.site_archive_post s
            RIGHT JOIN pageview_second p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = 14
        ),
        total_data_second As(
			select * from site_archive_second
            UNION
            select * from final_transformed_second
        ),
        summed_data_second AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations_second c
            LEFT JOIN total_data_second s ON c.realreferrer = s.realreferrer AND c.tag = s.tags
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Debat'',''Artikel'',''Nyhedsoverblik'',''Guide'',''Digidoc'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Debat'',''Artikel'',''Nyhedsoverblik'',''Guide'',''Digidoc'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Debat'',''Artikel'',''Nyhedsoverblik'',''Guide'',''Digidoc'', "Others")), '']'') AS percentile
			FROM percent_second
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
            ORDER BY FIELD(r.realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
            WHERE siteid = 14 AND date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND r.realreferrer IN ( ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'' )
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
            WHERE siteid = 14 AND Event_Action = ''Frontpage'' 
            AND date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
            and postid is not null
            GROUP BY postid, siteid, event_name
        ),
        transformed_data_third AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT(''https://fagbladet3f.dk/'', SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main_third
        ),
        final_transformed_third AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
            CASE
				WHEN Tags REGEXP ".*Arbejdsmiljø.*" THEN "Arbejdsmiljø"
				WHEN Tags REGEXP ".*Social dumping.*" THEN "Social dumping"
				WHEN Tags REGEXP ".*Ulighed.*" THEN "Ulighed"
				WHEN Tags REGEXP ".*Privatøkonomi.*" THEN "Privatøkonomi"
				WHEN Tags REGEXP ".*Fagligt.*" THEN "Fagligt"
				WHEN Tags REGEXP ".*Uddannelse.*" THEN "Uddannelse"
                ELSE ''others''
			END AS tags
            FROM transformed_data_third s
            LEFT JOIN prod.site_archive_post p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = 14 
        ),
        site_archive_third AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                CASE
                    WHEN Tags REGEXP ".*Arbejdsmiljø.*" THEN "Arbejdsmiljø"
                    WHEN Tags REGEXP ".*Social dumping.*" THEN "Social dumping"
                    WHEN Tags REGEXP ".*Ulighed.*" THEN "Ulighed"
                    WHEN Tags REGEXP ".*Privatøkonomi.*" THEN "Privatøkonomi"
                    WHEN Tags REGEXP ".*Fagligt.*" THEN "Fagligt"
                    WHEN Tags REGEXP ".*Uddannelse.*" THEN "Uddannelse"
                    ELSE ''others''
                END AS tags
            FROM prod.site_archive_post s
            RIGHT JOIN pageview_third p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = 14
        ),
        total_data_third As(
			select * from site_archive_third
            UNION
            select * from final_transformed_third
        ),
        summed_data_third AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations_third c
            LEFT JOIN total_data_third s ON c.realreferrer = s.realreferrer AND c.tag = s.tags
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, ''Arbejdsmiljø'', ''Social dumping'',''Ulighed'',''Privatøkonomi'',''Fagligt'',''Uddannelse'', "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, ''Arbejdsmiljø'', ''Social dumping'',''Ulighed'',''Privatøkonomi'',''Fagligt'',''Uddannelse'', "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, ''Arbejdsmiljø'', ''Social dumping'',''Ulighed'',''Privatøkonomi'',''Fagligt'',''Uddannelse'', "Others")), '']'') AS percentile
			FROM percent_third
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, ''Forside'',''Search'',''Facebook'',''NL <47'',''NL 48-60'',''NL >60'', "Others")
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
							''"title": "Emner",'',
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

