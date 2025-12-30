

SELECT JSON_OBJECT(
  'site', 11,
  'data', JSON_OBJECT(
    'items', JSON_ARRAY(
      JSON_OBJECT(
        'code', CONCAT('uge', WEEK(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY), 1)),
        'name', CONCAT('uge ', WEEK(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY), 1), ', ', YEAR(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY))),
        'period', CONCAT(
          DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY), '%e/%c'),
          '-',
          DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY), '%e/%c')
        )
      )
    ),
    'defaultCode', CONCAT('uge', WEEK(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY), 1))
  )
) AS json_result;
