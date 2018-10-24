-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under AGPL v3.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SFPLA
drop keyspace sfpla;
create keyspace sfpla WITH REPLICATION = {  'class':'NetworkTopologyStrategy', 'DC1':'1' }; --analytics

use sfpla;

create table sequences (
  name text,
  seq int,
  PRIMARY KEY (name)
);
insert into sequences (name, seq) values('DB_VER',1);

create type geo_point (
 lat double,
 lon double
);

create type viewport (
 w bigint,
 h bigint
);


create table outcomes (
  outcome text,
  sink text,
  created date,
  url text,
  total counter,
  primary key((outcome), sink, url, created)
);

--This should only be written to once
create table visitors (
  vid text, --visitor-id
  sid timeuuid, --session-id
  eid text, --event-id, AKA content (also action in params) ex. Clicked button A
  etyp text, --event-type/page-type
  created timestamp, --where they are in user flow
  uid text, --user-id
  last text, --last action/slug/url (what i just clicked on)
  next text, --next action/slug/url (set to null each action, nulls indicate lost interest or bad ux)
  sink text, --local-optimum/experiment
  ver int, --sink version/variation
  score double, --score
  params map<text,text>, --all experiment params, (include global-optimum-experimentid[outcome], a/b[type])
  ip text, --ip
  latlon frozen<geo_point>, --location
  --Detail Fields
  country text, --ISO-2
  culture text, --EN-US
  source text, --referring domain, user, service [sms]
  medium text, --email,sms,ad,etc [invite]
  campaign text, --marketing campaign name [chat_invite] 
  ref text, --referrer uid
  aid text, --affiliate id
  device text, 
  browser text,
  os text,  
  tz text,
  vp frozen<viewport>,
  PRIMARY KEY ((vid))
);

--Session starts
create table sessions (
  vid text, 
  sid timeuuid, 
  eid text, 
  etyp text,
  created timestamp, 
  uid text, 
  last text, 
  next text, 
  sink text,
  ver int, 
  score double,
  params map<text,text>, 
  ip text,
  latlon frozen<geo_point>,
  --Detail Fields
  country text, 
  culture text, 
  source text,
  medium text,
  campaign text,
  ref text, 
  aid text,
  device text, 
  browser text,
  os text,  
  tz text,
  vp frozen<viewport>,
  PRIMARY KEY ((vid), sid)
)
WITH CLUSTERING ORDER BY (sid DESC);

create table events (
  vid text, 
  sid timeuuid, 
  xid text, --experiment id
  split text, --experiment split
  eid text,
  etyp text, 
  created timestamp, 
  uid text, 
  last text, 
  next text,   
  sink text, 
  ver int, 
  score double, 
  params map<text,text>, --filter? experiment params, (global-optimum-experimentid[outcome], a/b[type])
  duration bigint,  --time since last click
  ip text, 
  latlon frozen<geo_point>, 
  PRIMARY KEY ((vid, sid), created) --perhaps move sid from ck into pk
)
WITH CLUSTERING ORDER BY (created DESC);

--Session ends
create table ends (
  vid text, 
  sid timeuuid, 
  eid text, 
  etyp text,
  created timestamp, 
  uid text, 
  last text, 
  next text, 
  sink text,
  ver int, 
  score double,
  params map<text,text>, 
  duration bigint,
  ip text,
  latlon frozen<geo_point>,
  PRIMARY KEY ((vid), sid)
)
WITH CLUSTERING ORDER BY (sid DESC);

create table nodes (
  vid text, 
  uid text,
  ip text,
  sid timeuuid, 
  PRIMARY KEY ((vid), ip)
);

create table locations (
  vid text, 
  latlon frozen<geo_point>,
  uid text,
  sid timeuuid, 
  PRIMARY KEY ((vid, latlon))
);

create table aliases (
  vid text,
  uid text, 
  sid timeuuid, 
  PRIMARY KEY ((vid), uid)
);

create table users (
  uid text,
  vid text,   
  sid timeuuid, 
  PRIMARY KEY ((uid), vid)
);

create table dailies (  
  ip text, 
  day date,
  total counter,
  primary key((ip),day) 
)
WITH CLUSTERING ORDER BY (day DESC);

create table hits (  
  url text, 
  total counter,
  primary key((url)) 
);

create table ips (  
  ip text, 
  total counter,
  primary key((ip)) 
);

create table reqs (  
  vid text, 
  total counter,
  primary key((vid)) 
);

create table browsers (
  browser text, --user-agent
  total counter,
  primary key(browser)
);

create table referrers (
  url text,
  total counter,
  primary key((url)) 
);

create table referrals (
  ref text, --referrer uid, also external. EX. magazine
  vid text, 
  primary key((vid)) 
);

create table affiliates (
  aid text, --referrer uid, also external. EX. magazine
  vid text, 
  primary key((vid)) 
);

--NATS Specializations

-- Esp. Server Debugging
create table counters (  
  id text,   
  total counter,
  primary key((id)) 
);

-- Esp. Server Debugging
create table logs (  
  ldate date,
  created timestamp,
  ltime time, --nanosecond time for detailed server debugging
  id text, 
  name text, 
  host text, 
  hostname text, 
  owner text,
  ip text,
  level int, 
  msg text,
  params map<text,text>,
  primary key((ldate), created, ltime, id) 
)
WITH CLUSTERING ORDER BY (created DESC); --We make an exception here for using a clustered key instead of a PK so we can do realtime debugging

-- Esp. Server Debugging
create table updates (
  id text,
  updated timestamp,
  msg text,
  primary key(id)
);