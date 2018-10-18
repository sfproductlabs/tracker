-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under AGPL v3.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SFPLT

drop keyspace sfplt;
create keyspace sfplt WITH REPLICATION = {  'class':'NetworkTopologyStrategy', 'DC1':'1' }; --temp

use sfplt;

create table test (
  uid text,
  gender text,
  vid text,
  PRIMARY KEY (uid,vid)
);
