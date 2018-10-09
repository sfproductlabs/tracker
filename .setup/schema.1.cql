-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under AGPL v3.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SFPLA
drop keyspace sfpla;
create keyspace sfpla WITH REPLICATION = {  'class':'NetworkTopologyStrategy', 'DC1':'1' };

use sfpla;

create table sequences (
  name text,
  seq int,
  PRIMARY KEY (name)
);
insert into sequences (name, seq) values('DB_VER',1);


create table counters (  
  id text,   
  total counter,
  primary key((id)) 
);

create table logs (  
  id text, 
  ldate date,
  ltime time,
  name text, 
  host text, 
  hostname text, 
  owner text,
  ip text,
  level int, 
  msg text,
  params frozen<map<text,text>>,
  primary key((id, ldate, ltime)) 
);

create table updates (
  id text,
  updated timestamp,
  msg text,
  primary key(id)
);


create type geo_point (
 lat double,
 lon double
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
create table acquisitions (
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
  params frozen<map<text,text>>, --all experiment params, (include global-optimum-experimentid[outcome], a/b[type])
  duration bigint, --time since last click
  ip text, --ip
  latlon frozen<geo_point>, --location
  --Detail Fields
  country text, --ISO-3
  culture text, --EN-US
  gender text,
  source text, --referring domain, user, service
  medium text, --email,sms,ad,etc
  campaign text, --marketing campaign name  
  device text, 
  browser text,
  os text,  
  tz text,
  --EXCEPTIONS
  email timestamp, --when we got their email EXCEPTION TO THE RULE OF WRITE ONCE

  PRIMARY KEY ((vid))
);

--Session starts
create table starts (
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
  params frozen<map<text,text>>, 
  duration bigint,
  ip text,
  latlon frozen<geo_point>,
  --Detail Fields
  country text, 
  culture text, 
  gender text,
  source text,
  medium text,
  campaign text,
  device text, 
  browser text,
  os text,  
  tz text,
  PRIMARY KEY ((vid), sid)
);

create table events (
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
  params frozen<map<text,text>>, --filter? experiment params, (global-optimum-experimentid[outcome], a/b[type])
  duration bigint, 
  ip text, 
  latlon frozen<geo_point>, 
  PRIMARY KEY ((vid, sid), created) --perhaps move sid from ck into pk
);

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
  params frozen<map<text,text>>, 
  duration bigint,
  ip text,
  latlon frozen<geo_point>,
  PRIMARY KEY ((vid), sid)
);




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
);

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

