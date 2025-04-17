select
  created,
  ename,
  params::json -> 'name' file_name
from
  hive.events
where
  ename = 'import_xlsx'
  or ename = 'import_csv'
order by
  created desc;
