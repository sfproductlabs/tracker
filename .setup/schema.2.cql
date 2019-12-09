-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under AGPL v3.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SFPLA
--drop keyspace sfpla;
-- CREATE KEYSPACE sfpla WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '2'}  AND durable_writes = true;
-- create keyspace sfpla WITH REPLICATION = {  'class':'NetworkTopologyStrategy', 'dc1':'1' }; --analytics
--For a dev machine...
--CREATE KEYSPACE sfpla WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}

use sfpla;

create table sequences (
  name text,
  seq int,
  PRIMARY KEY (name)
);
insert into sequences (name, seq) values('DB_VER',2);

create type geo_point (
 lat double,
 lon double
);

create type viewport (
 w bigint,
 h bigint
);

create table hosts (
  hhash text,
  hostname text,
  primary key((hhash), hostname) 
);

--how many successful outcomes/intentions/local optimizations this day
create table outcomes (
  hhash text,
  outcome text,
  sink text,
  created date,
  url text,
  total counter,
  primary key((hhash, outcome), sink, url, created)
);

--This should only be written to once a user first visits (vid==sid), AKA acquisitions
create table visitors (
  vid timeuuid, --visitor-id
  did text, --device-id
  sid timeuuid, --session-id
  hhash text, --host-hash
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
  xid text, --primary experiment id, hypothesis
  split text, --primary experiment split A/B
  ename text, ----event name e.g. "signup_attempt", event-id/event-name, AKA utm_content, AKA action in params ex. Clicked button A, joined-experiment-now
  etyp text, --event-type, category of events (views, category, component)
  ver int, --experiment/sink version/variation
  sink text, --local-optimum/intention, Ex. want user to sign up for offer X
  score double, --score (where user is in sink/intention)
  params map<text,text>, --all additional experiment params, (include global-optimum-experimentid[outcome], a/b[type], etc)
  --Landing Fields
  gaid text, --google advertising id
  idfa text, --apple advertising id
  country text, --ISO-2
  culture text, --EN-US
  source text, --referring domain, user, service [sms] AKA utm_source
  medium text, --email,sms,ad,etc [invite] AKA utm_medium
  campaign text, --marketing campaign name [chat_invite] AKA utm_campaign
  term text, --seo search query term AKA utm_term
  ref uuid, --referrer vid
  rcode text, --referrer code
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
  did text, 
  sid timeuuid, 
  hhash text,
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
  xid text, 
  split text, 
  ename text, 
  etyp text,
  ver int, 
  sink text, 
  score double, 
  params map<text,text>, 
  --Landing Fields
  gaid text,
  idfa text,
  country text, 
  culture text, 
  source text,
  medium text,
  campaign text,
  term text, 
  ref uuid, 
  rcode text, 
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
  hhash text,
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
  xid text, 
  split text, 
  ename text, 
  source text, 
  medium text, 
  campaign text, 
  term text,
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
  hhash text,
  vid timeuuid, 
  uid uuid,
  ip inet,
  sid timeuuid, 
  PRIMARY KEY ((hhash, vid), ip)
);

create table locations (
  hhash text,
  vid timeuuid, 
  latlon frozen<geo_point>,
  uid uuid,
  sid timeuuid, 
  PRIMARY KEY ((hhash, vid, latlon))
);

create table aliases (
  hhash text,
  vid timeuuid,
  uid uuid, 
  sid timeuuid, 
  PRIMARY KEY ((hhash, vid), uid)
);

create table users (
  hhash text,
  uid uuid,
  vid timeuuid,   
  sid timeuuid, 
  PRIMARY KEY ((hhash, uid), vid)
);

create table usernames (
  hhash text,
  uhash text,
  vid timeuuid,   
  sid timeuuid, 
  PRIMARY KEY ((hhash, uhash), vid)
);

create table cells (
  hhash text,
  chash text, --from cell phone
  vid timeuuid,   
  sid timeuuid,
  PRIMARY KEY ((hhash, chash))
);

create table emails (
  hhash text,
  ehash text, --from email
  vid timeuuid,   
  sid timeuuid,
  PRIMARY KEY ((hhash, ehash))
);

create table hits (  
  hhash text,
  url text, 
  total counter,
  primary key((hhash, url)) 
);

create table ips (  
  hhash text,
  ip inet, 
  total counter,
  primary key((hhash, ip)) 
);

--last routed for this IP
create table routed (  
  hhash text,
  ip inet, 
  url text, 
  primary key((hhash, ip)) 
);

create table reqs (  
  hhash text,
  vid timeuuid, 
  total counter,
  primary key((hhash, vid)) 
);

create table browsers (
  hhash text,
  bhash text,
  browser text, --user-agent
  total counter,
  primary key((hhash, bhash), browser)
);

create table referrers (
  hhash text,
  url text,
  total counter,
  primary key((hhash, url)) 
);

create table referrals (
  hhash text,
  ref uuid, --referrer uid
  vid timeuuid, 
  gen int, --growth loop / generation
  primary key((hhash, vid)) 
);

create table referred (
  hhash text,
  rcode text, --referrer code
  vid timeuuid, 
  gen int, --growth loop / generation
  primary key((hhash, vid)) 
);

create table affiliates (
  hhash text,
  aff text, --external. EX. magazine
  vid timeuuid, 
  primary key((hhash, vid)) 
);

create table redirects (
  hhash text,
  urlfrom text, --without the protocol (https)
  urlto text, --with protocol
  updated timestamp,  
  updater uuid,
  primary key(urlfrom)
);

create table redirect_history (
  urlfrom text, --without the protocol (https)
  hostfrom text,
  slugfrom text,
  urlto text, --with protocol
  hostto text,
  pathto text,
  searchto text,
  updated timestamp,  
  updater uuid,
  primary key((hostfrom),updated)
)
WITH CLUSTERING ORDER BY (updated DESC);
CREATE INDEX hostto_redirect_history_idx ON redirect_history ( hostto );

create table accounts (
  uid uuid, 
  pwd text,
  ip inet,
  msg text,
  expires timestamp,
  creds map<text,frozen<map<text,text>>>, --host, claim[yes]
  created timestamp,
  owner uuid,
  primary key((uid))
);
--https://localhost:8443/rpi/v1/redirects/14fb0860-b4bf-11e9-8971-7b80435315ac/password/nytimes.com
--insert into accounts (uid,pwd,msg,expires,creds,created,owner) values (14fb0860-b4bf-11e9-8971-7b80435315ac,'W6ph5Mm5Pz8GgiULbPgzG37mj9g=', 'demo admin user',toTimestamp('2999-01-01'),{'*':{'*':'*'}}, toTimestamp(now()),14fb0860-b4bf-11e9-8971-7b80435315ac);
--insert into redirects (urlfrom,urlto) values ('localhost:8443/tv','https://google.com');

--INTERNAL & EXTERNAL SERVICES
create table services (
  name    text,
  secret  text, --secret hash
  roles   set<text>,
  expiry date,
  created timestamp,
  owner uuid,
  updated timestamp,
  updater uuid,
  primary key       (name)
);

--EVENTING
create table queues (
  id timeuuid,
  src text, --source action Ex. sms
  sid timeuuid, --source id Ex. sms-id
  skey text, --source key (if not a uuid) like for cohorts: "name"
  ip text, --Requestor IP
  host text, --Host executing service
  schedule timestamp,
  started timestamp,
  completed timestamp,
  updated timestamp,
  updater uuid,
  created timestamp,  
  owner uuid,
  PRIMARY KEY ((id))
);
CREATE INDEX queues_type_idx ON queues ( src );
CREATE INDEX queues_completed_idx ON queues ( completed );
CREATE INDEX queues_started_idx ON queues ( started );


create table action_names (
  name text,
  PRIMARY KEY ((name))
);


create table actions (
  sid timeuuid, --source id Ex. message-id
  src text, --source action Ex. message, queues
  did uuid, --unique id differentiator (Ex uid)
  dsrc text, --Ex diffentiator source ex. uid
  meta map<text,text>, --METADATA, SPLIT etc.
  exqid timeuuid, --executing queue id
  created timestamp,  
  started timestamp,
  completed timestamp,
  PRIMARY KEY (sid,did)
);

--external actions
create table actions_ext (
  sid text, --source id, audit id, inc. external Ex. SES message-id
  svc text, --Ex. SES, 'message', 'sms, 'action'
  iid timeuuid, --internal id (Esp. action id)
  uid uuid, --optional user id
  created timestamp,
  updated timestamp,
  meta map<text,text>, --METADATA Ex. Clean to ehash *not* email:xxx.x.com, bounce:true, etc.
  PRIMARY KEY ((sid),svc)
)
WITH default_time_to_live = 1209600; --2 weeks

--should never be updated
create table cohorts (
  name text,
  uids_url text,
  imported int, --successful imports (count)
  started timestamp,
  completed timestamp,
  created timestamp,
  owner uuid,
  PRIMARY KEY ((name))
);

create table messages (
  id timeuuid,
  subject text,
  template text, --use a url for now (should be a github/gitlab api selector to a list of templates made in static.staging.homodea.com)
  app text, --app 
  rel text, 
  ver int, --version
  schedule timestamp,
  started timestamp,
  completed timestamp,
  ptyp text, 
  auth text, --author
  xid text, --experiment id 
  cohorts set<text>,
  ehashes set<text>, 
  chashes set<text>,
  split decimal, --% random 1=1%
  source text,
  medium text,
  campaign text,
  term text, 
  sink text,
  score double,
  promo text, --promo code to pass through
  ref uuid, --referrer uid (Select)
  aff text, --affiliate uname or code (Select)
  repl map<text,text>, --text to replace ex Hi {{fn}} -> becomes -> Hi Andrew, Prepend "text." to use Raw text instead of the user record Ex. {{text.msg}}. Only start off with one "text." message body in Admin UI (text.msg)
  created timestamp,
  owner uuid,
  updated timestamp,
  updater uuid,
  PRIMARY KEY ((id))
);


--NATS Specializations
--limit service usage
create table dailies (  
  ip inet, 
  day date,
  total counter,
  primary key((ip),day) 
)
WITH CLUSTERING ORDER BY (day DESC);

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
