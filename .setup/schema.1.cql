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

--This should only be written to once a user first visits (vid==sid)
create table visitors (
  vid timeuuid, --visitor-id
  sid timeuuid, --session-id
  app text, --app version
  created timestamp, --time.Now().UTC() on server
  uid text, --user-id
  last text, --last action/slug/url/referrer_url (what i just clicked on)
  url text, --this should always be url slug of current url
  ip text, --ip
  latlon frozen<geo_point>, --location
  ptype text, --page category/type 
  bhash text, --browser-hash
  --Experiment Fields
  xid text, --experiment id
  split text, --experiment split A/B
  ename text, ----event name e.g. "signup_attempt", event-id/event-name, AKA content (also action in params) ex. Clicked button A, joined-experiment-now
  etyp text, --event-type/page-type
  sink text, --local-optimum/experiment
  ver int, --sink version/variation
  score double, --score
  params map<text,text>, --all additional experiment params, (include global-optimum-experimentid[outcome], a/b[type])
  --Detail Fields
  country text, --ISO-2
  culture text, --EN-US
  source text, --referring domain, user, service [sms]
  medium text, --email,sms,ad,etc [invite]
  campaign text, --marketing campaign name [chat_invite] 
  ref text, --referrer uid
  aid text, --affiliate id
  browser text,
  device text, 
  os text,  
  tz text,
  vp frozen<viewport>,
  PRIMARY KEY ((vid))
);

--Session-starts written to once a user first starts a session
create table sessions (
  vid timeuuid, 
  sid timeuuid, 
  app text,
  created timestamp, 
  uid text, 
  last text, 
  url text, 
  ip text,
  latlon frozen<geo_point>,
  ptype text, 
  bhash text, 
  --Experiment Fields
  xid text, 
  split text, 
  ename text, 
  etyp text, 
  sink text, 
  ver int,
  score double, 
  params map<text,text>, 
  --Detail Fields
  country text, 
  culture text, 
  source text,
  medium text,
  campaign text,
  ref text, 
  aid text,
  browser text,
  device text, 
  os text,  
  tz text,
  vp frozen<viewport>,
  PRIMARY KEY ((vid), sid)
)
WITH CLUSTERING ORDER BY (sid DESC);

create table events (
  vid timeuuid, 
  sid timeuuid, 
  app text,
  created timestamp, 
  uid text, 
  last text, 
  url text, --current url slug
  ip text, 
  latlon frozen<geo_point>, 
  ptype text, 
  bhash text, 
  duration bigint,  --time since last click
  --Experiment Fields
  xid text, 
  split text, 
  ename text, 
  etyp text, 
  sink text, 
  ver int,
  score double, 
  params map<text,text>, 
  PRIMARY KEY ((vid, sid), created) --perhaps move sid from ck into pk
)
WITH CLUSTERING ORDER BY (created DESC);

create table nodes (
  vid timeuuid, 
  uid text,
  ip text,
  sid timeuuid, 
  PRIMARY KEY ((vid), ip)
);

create table locations (
  vid timeuuid, 
  latlon frozen<geo_point>,
  uid text,
  sid timeuuid, 
  PRIMARY KEY ((vid, latlon))
);

create table aliases (
  vid timeuuid,
  uid text, 
  sid timeuuid, 
  PRIMARY KEY ((vid), uid)
);

create table users (
  uid text,
  vid timeuuid,   
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
  vid timeuuid, 
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
  vid timeuuid, 
  primary key((vid)) 
);

create table affiliates (
  aid text, --referrer uid, also external. EX. magazine
  vid timeuuid, 
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
