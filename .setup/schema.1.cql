-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under AGPL v3.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SFPLA
--drop keyspace sfpla;
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


--how many successful outcomes/intentions/local optimizations this day
create table outcomes (
  outcome text,
  sink text,
  created date,
  url text,
  total counter,
  primary key((outcome), sink, url, created)
);

--This should only be written to once a user first visits (vid==sid), AKA acquisitions
create table visitors (
  vid timeuuid, --visitor-id
  sid timeuuid, --session-id
  app text, --app
  rel text, --app release/version
  created timestamp, --time.Now().UTC() on server
  uid uuid, --user-id
  last text, --last action/slug/url/referrer_url (what i just clicked on)
  url text, --this should always be url slug of **current** url
  ip inet, --client ip
  latlon frozen<geo_point>, --location
  ptyp text, --page category/type 
  bhash text, --browser-hash
  auth uuid, --author of **destination** content
  --Experiment Fields
  campaign text, --marketing campaign name [chat_invite] AKA utm_campaign
  xid text, --primary experiment id, hypothesis
  split text, --primary experiment split A/B
  ename text, ----event name e.g. "signup_attempt", event-id/event-name, AKA utm_content, AKA action in params ex. Clicked button A, joined-experiment-now
  etyp text, --event-type, category of events (views, category, component)
  ver int, --experiment/sink version/variation
  sink text, --local-optimum/intention, Ex. want user to sign up for offer X
  score double, --score (where user is in sink/intention)
  params map<text,text>, --all additional experiment params, (include global-optimum-experimentid[outcome], a/b[type], etc)
  --Landing Fields
  country text, --ISO-2
  culture text, --EN-US
  source text, --referring domain, user, service [sms] AKA utm_source
  medium text, --email,sms,ad,etc [invite] AKA utm_medium
  term text, --seo search query term AKA utm_term
  ref uuid, --referrer uid
  aff text, --affiliate id, promo-code
  browser text,
  device text, 
  os text,  
  tz text,
  vp frozen<viewport>,
  PRIMARY KEY ((vid))
);

--Session-starts written to once a user first starts a session, AKA session-starts
create table sessions (
  vid timeuuid, 
  sid timeuuid, 
  app text,
  rel text, 
  created timestamp, 
  uid uuid, 
  last text, 
  url text, 
  ip inet,
  latlon frozen<geo_point>,
  ptyp text, 
  bhash text, 
  auth uuid, 
  duration bigint,  --time since last click/session
  --Experiment Fields
  campaign text,
  xid text, 
  split text, 
  ename text, 
  etyp text,
  ver int, 
  sink text, 
  score double, 
  params map<text,text>, 
  --Landing Fields
  country text, 
  culture text, 
  source text,
  medium text,
  term text, 
  ref uuid, 
  aff text,
  browser text,
  device text, 
  os text,  
  tz text,
  vp frozen<viewport>,
  PRIMARY KEY ((vid), sid)
)
WITH CLUSTERING ORDER BY (sid DESC);

create table events (
  eid timeuuid, --event id, server generated, unique to every event
  vid timeuuid, 
  sid timeuuid, 
  app text,
  rel text, 
  created timestamp, 
  uid uuid, 
  last text, 
  url text, 
  ip inet, 
  latlon frozen<geo_point>, 
  ptyp text, 
  bhash text, 
  auth uuid, 
  duration bigint,  
  --Experiment Fields
  campaign text,
  xid text, 
  split text, 
  ename text, 
  etyp text, 
  ver int,
  sink text, 
  score double, 
  params   map<text,text>, 
  --Additional Fields
  targets  map<text,frozen<set<text>>>, --type, components viewed on page/server to see whats working. Ex. {{videos : {"1.mov"}}, {ads: {"intro1", "book", "test"}}
  rid uuid, --related id/cluster based event on original/related eid or internal id/reference (ex. newsletterid)
  PRIMARY KEY (eid) --perhaps move sid from ck into pk
);

create table nodes (
  vid timeuuid, 
  uid uuid,
  ip inet,
  sid timeuuid, 
  PRIMARY KEY ((vid), ip)
);

create table locations (
  vid timeuuid, 
  latlon frozen<geo_point>,
  uid uuid,
  sid timeuuid, 
  PRIMARY KEY ((vid, latlon))
);

create table aliases (
  vid timeuuid,
  uid uuid, 
  sid timeuuid, 
  PRIMARY KEY ((vid), uid)
);

create table users (
  uid uuid,
  vid timeuuid,   
  sid timeuuid, 
  PRIMARY KEY ((uid), vid)
);

create table usernames (
  uhash text,
  vid timeuuid,   
  sid timeuuid, 
  PRIMARY KEY ((uhash), vid)
);


create table emails (
  ehash text, --from email
  vid timeuuid,   
  sid timeuuid,
  PRIMARY KEY ((ehash))
);

create table dailies (  
  ip inet, 
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
  ip inet, 
  total counter,
  primary key((ip)) 
);

create table reqs (  
  vid timeuuid, 
  total counter,
  primary key((vid)) 
);

create table browsers (
  bhash text,
  browser text, --user-agent
  total counter,
  primary key((bhash), browser)
);

create table referrers (
  url text,
  total counter,
  primary key((url)) 
);

create table referrals (
  ref uuid, --referrer uid
  vid timeuuid, 
  gen int, --growth loop / generation
  primary key((vid)) 
);

create table affiliates (
  aff text, --external. EX. magazine
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
  id timeuuid,
  ldate date,
  created timestamp,
  ltime time, --nanosecond time for detailed server debugging
  topic text, 
  name text, 
  host text, 
  hostname text, 
  owner uuid,
  ip inet,
  level int, 
  msg text,
  params map<text,text>,
  primary key((id)) 
);


-- Esp. Server Debugging
create table updates (
  id text,
  updated timestamp,
  msg text,
  primary key(id)
);
