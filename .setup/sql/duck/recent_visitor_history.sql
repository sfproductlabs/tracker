SELECT *
FROM hive.events
WHERE DATE_TRUNC('day', MAKE_DATE(year::INTEGER, month::INTEGER, day::INTEGER)) >= CURRENT_DATE - INTERVAL '7 days'
  AND DATE_TRUNC('day', MAKE_DATE(year::INTEGER, month::INTEGER, day::INTEGER)) <= CURRENT_DATE
  AND vid='90288b53-fd3a-11ef-a950-2f047503ba50'
order by created desc;
