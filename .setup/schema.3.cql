-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under AGPL v3.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SFPLA
--drop keyspace sfpla;
CREATE KEYSPACE sfpla WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '1'}  AND durable_writes = true;
-- create keyspace sfpla WITH REPLICATION = {  'class':'NetworkTopologyStrategy', 'dc1':'1' }; --analytics
--For a dev machine...
-- CREATE KEYSPACE sfpla WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
-- ALTER KEYSPACE sfpla WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 2};

use sfpla;

create table sequences (
  name text,
  seq int,
  PRIMARY KEY (name)
);
insert into sequences (name, seq) values('DB_VER',3);

create type geo_point (
 lat double,
 lon double
);

create type viewport (
 w bigint,
 h bigint
);


create type payment (
  invid timeuuid, --invoice id
  invoiced timestamp,
  product text, --our product name
  pid timeuuid, --product id
  pcat text, --product category (subscription/premium)
  man text, --outside manufacturer
  model text, --outside manufacturer's model code
  qty double,
  duration text, --aka subscription_type
  starts date, --product date starts
  ends date, --product date ends
  price double, --original list price
  discount double, --actual (not %)
  revenue double, --what hits the bank
  margin double,  --what we made off it
  cost double, --est costs (can include intangible costs)
  tax double, --vat, gst , etc.
  tax_rate double,  
  commission double, --fees for app store, play store etc
  referral double, --user/entity fee for referral
  fees double, --additional service fees
  subtotal double, --what should have been paid (inc-tax)
  total double, --what should have been paid for the whole invoice (inc-tax)
  payment double, --payment = actual amount customer paid
  paid timestamp,
);

create type geo_pol (
 country text, --iso2 ex. US, DE, AU
 rcode text, --State ex. CA, BW
 region text, --State ex. California
 county text, --Ex city San Francisco, County San Francisco (legislative sub-region)
 city text, --Ex. Brisbane
 zip text
);

create table countries (
  country text, --iso2 ex US
  name text, --ex United States of America
  continent text, -- North America
  PRIMARY KEY ((country))
);

--Note this is not a good use of cassandra, better to use rocks
create table geo_ip (
 ipc text, --ip class used to distribute pk (Starting with Ex: ip6, ip4 - could/should use a bracket/bitmap[best]/divider[ok] instead Ex. ip6_1_of_2048 OR [ips >> 116])
 ips varint, --ip start
 ipe varint, --ip end
 ipis inet, --ip start address representation
 ipie inet, --ip end address representation
 country text, 
 region text, --could be california or CA
 city text,
 lat double,
 lon double,
 tz text,
 zip text,
 PRIMARY KEY ((ipc),ips,ipe)
);
--CREATE INDEX ips_geo_ip_idx ON geo_ip ( ips );
--CREATE INDEX ipe_geo_ip_idx ON geo_ip ( ipe );

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
  cflags bigint, --compliances represented via N notation == 1001110101111
  created timestamp, --time.Now().UTC() on server
  uid timeuuid, --user-id
  last text, --last action/slug/url/referrer_url (what i just clicked on)
  url text, --this should always be url slug of **current** url
  ip inet, --client ip
  iphash text, 
  latlon frozen<geo_point>, --location
  ptyp text, --page category/type 
  bhash text, --browser-hash
  auth timeuuid, --author of **destination** content
  --Experiment Fields
  xid text, --primary experiment id, hypothesis
  split text, --primary experiment split A/B
  ename text, ----event name e.g. "signup_attempt", event-id/event-name, AKA utm_content, AKA action in params ex. Clicked button A, joined-experiment-now
  etyp text, --event-type, category of events (views, category, component)
  ver int, --experiment/sink version/variation
  sink text, --local-optimum/intention, Ex. want user to sign up for offer X
  score double, --score (where user is in sink/intention)
  params map<text,text>, --all additional experiment params,passive multivariate experiments (xid,split), (include global-optimum-experimentid[outcome], a/b[type], etc)
  nparams map<text,double>, --numerical version of params
  --Landing Fields
  gaid text, --google advertising id
  idfa text, --apple advertising id
  msid text, --microsoft advertising id
  fbid text, --facebook advertising id
  country text, --ISO-2
  region text, --state/region name
  city text, --city
  zip text, --zip/postal code
  culture text, --EN-US
  source text, --referring domain, user, service [sms] AKA utm_source
  medium text, --email,sms,ad,etc [invite] AKA utm_medium
  campaign text, --marketing campaign name [chat_invite] AKA utm_campaign
  term text, --seo search query term AKA utm_term
  ref timeuuid, --referrer vid
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
  cflags bigint,
  created timestamp, 
  uid timeuuid, 
  last text, 
  url text, 
  ip inet,
  iphash text, 
  latlon frozen<geo_point>,
  ptyp text, 
  bhash text, 
  auth timeuuid, 
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
  nparams map<text,double>,
  --Landing Fields
  gaid text,
  idfa text,
  msid text,
  fbid text,
  country text, 
  region text,
  city text,
  zip text,
  culture text, 
  source text,
  medium text,
  campaign text,
  term text, 
  ref timeuuid, 
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
  cflags bigint,
  created timestamp, 
  uid timeuuid, 
  last text, 
  url text, 
  ip inet,
  iphash text, 
  latlon frozen<geo_point>, 
  ptyp text, 
  bhash text, 
  auth timeuuid, 
  duration bigint,  
  --Experiment Fields
  xid text, 
  split text, 
  ename text, 
  source text, 
  medium text, 
  campaign text, 
  country text,
  region text,
  city text,
  zip text,
  term text,
  etyp text, 
  ver int,
  sink text, 
  score double, 
  params   map<text,text>, 
  nparams map<text,double>,
  --Additional Fields
  payment frozen<payment>,
  targets  map<text,frozen<set<text>>>, --type, components viewed on page/server to see whats working. Ex. {{videos : {"1.mov"}}, {ads: {"intro1", "book", "test"}}
  relation text, --related object (period delimited) ex. xcs.thread
  rid timeuuid, --relation id/cluster based event on original/related eid or internal id/reference (ex. newsletterid)
  PRIMARY KEY (eid) --perhaps move sid from ck into pk
);

create table events_recent (
  eid timeuuid, --event id, server generated, unique to every event
  vid timeuuid, 
  sid timeuuid, 
  hhash text,
  app text,
  rel text, 
  cflags bigint,
  created timestamp, 
  uid timeuuid, 
  last text, 
  url text, 
  ip inet,
  iphash text, 
  latlon frozen<geo_point>, 
  ptyp text, 
  bhash text, 
  auth timeuuid, 
  duration bigint,  
  --Experiment Fields
  xid text, 
  split text, 
  ename text, 
  source text, 
  medium text, 
  campaign text, 
  country text,
  region text,
  city text,
  zip text,
  term text,
  etyp text, 
  ver int,
  sink text, 
  score double, 
  params   map<text,text>, 
  nparams map<text,double>,
  --Additional Fields
  payment frozen<payment>,
  targets  map<text,frozen<set<text>>>, --type, components viewed on page/server to see whats working. Ex. {{videos : {"1.mov"}}, {ads: {"intro1", "book", "test"}}
  relation text, --related object (period delimited) ex. xcs.thread
  rid timeuuid, --relation id/cluster based event on original/related eid or internal id/reference (ex. newsletterid)
  PRIMARY KEY (eid) --perhaps move sid from ck into pk
) WITH default_time_to_live = 2678400; --31 days
ALTER TABLE events_recent WITH gc_grace_seconds = 14400;

create table nodes (
  hhash text,
  vid timeuuid, 
  uid timeuuid,
  iphash text,
  ip inet,
  sid timeuuid, 
  PRIMARY KEY ((hhash, vid), iphash)
);

create table locations (
  hhash text,
  vid timeuuid, 
  latlon frozen<geo_point>,
  uid timeuuid,
  sid timeuuid, 
  PRIMARY KEY ((hhash, vid, latlon))
);

create table aliases (
  hhash text,
  vid timeuuid,
  uid timeuuid, 
  sid timeuuid, 
  PRIMARY KEY ((hhash, vid), uid)
);

create table users (
  hhash text,
  uid timeuuid,
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
  ref timeuuid, --referrer uid
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
  updater timeuuid,
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
  updater timeuuid,
  primary key((hostfrom),updated)
)
WITH CLUSTERING ORDER BY (updated DESC);
CREATE INDEX hostto_redirect_history_idx ON redirect_history ( hostto );

create table accounts (
  uid timeuuid, 
  pwd text,
  ip inet,
  msg text,
  expires timestamp,
  creds map<text,frozen<map<text,text>>>, --host, claim[yes]
  created timestamp,
  owner timeuuid,
  primary key((uid))
);
--https://localhost:8443/rpi/v1/redirects/14fb0860-b4bf-11e9-8971-7b80435315ac/password/nytimes.com
insert into sfpla.accounts (uid,pwd,msg,expires,creds,created,owner) values (14fb0860-b4bf-11e9-8971-7b80435315ac,'W6ph5Mm5Pz8GgiULbPgzG37mj9g=', 'demo admin user',toTimestamp('2999-01-01'),{'*':{'*':'*'}}, toTimestamp(now()),14fb0860-b4bf-11e9-8971-7b80435315ac);
--password=password
--insert into redirects (urlfrom,urlto) values ('localhost:8443/tv','https://google.com');

--INTERNAL & EXTERNAL SERVICES
create table services (
  name    text,
  secret  text, --secret hash
  roles   set<text>,
  expiry date,
  created timestamp,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  primary key       (name)
);

--EVENTING
create table queues (
  id timeuuid,
  src text, --source action Ex. sms
  sid timeuuid, --source id Ex. sms-id
  skey text, --source key (if not a timeuuid) like for cohorts: "name"
  ip text, --Requestor IP
  host text, --Host executing service
  schedule timestamp,
  started timestamp,
  completed timestamp,
  updated timestamp,
  updater timeuuid,
  created timestamp,  
  owner timeuuid,
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
  did timeuuid, --unique id differentiator (Ex uid)
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
  uid timeuuid, --optional user id
  created timestamp,
  updated timestamp,
  meta map<text,text>, --METADATA Ex. Clean to ehash *not* email:xxx.x.com, bounce:true, etc.
  PRIMARY KEY ((sid),svc)
)
WITH default_time_to_live = 1209600; --2 weeks

--should never be updated
create table cohorts (
  name text,
  uids_url text, --uids_url
  imported int, --successful imports (count)
  started timestamp,
  completed timestamp,
  created timestamp,
  owner timeuuid,
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
  split double, --% random 1=1%
  splitn text, --split name
  source text,
  medium text,
  campaign text,
  term text, 
  sink text,
  score double,
  promo text, --promo code to pass through
  ref timeuuid, --referrer uid (Select)
  aff text, --affiliate uname or code (Select)
  repl map<text,text>, --text to replace ex Hi {{fn}} -> becomes -> Hi Andrew, Prepend "text." to use Raw text instead of the user record Ex. {{text.msg}}. Only start off with one "text." message body in Admin UI (text.msg)
  created timestamp,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
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
  owner timeuuid,
  ip inet,
  iphash text,
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


create table zips (
  country text, 
  zip text, --needs to be normalized
  region text, 
  rcode text,
  county text,
  city text,
  culture text, 
  population int,
  men int,
  women int,
  hispanic double,
  white double,
  black double,
  native double,
  asian double,
  pacific double,
  voters int,
  income double,
  incomeerr double,
  incomepercap double,
  incomepercaperr double,
  poverty double,
  childpoverty double,
  professional double,
  service double,
  office double,
  construction double,
  production double,
  drive double,
  carpool double,
  transit double,
  walk double,
  othertransport double,
  workathome double,
  meancommute double,
  employed int,
  privatework double,
  publicwork double,
  selfemployed double,
  familywork double,
  unemployment double,
  latlon frozen<geo_point>,  
  loc frozen<geo_pol>, --not used rn
  created timestamp, 
  primary key((country,zip))
);
--COPY sfpla.zips (zip,country,region,county,population,men,women,hispanic,white,black,native,asian,pacific,voters,income, incomeerr, incomepercap, incomepercaperr, poverty, childpoverty, professional,service,  office,  construction,  production, drive, carpool, transit, walk , othertransport, workathome ,  meancommute, employed , privatework, publicwork , selfemployed, familywork , unemployment, city, rcode, created, latlon) FROM 'zipdata.csv' WITH HEADER = TRUE;
ALTER TABLE zips WITH gc_grace_seconds = 60;

--- PRIVACY ---
create table jurisdictions (
  regulation text, --Ex. gdpr
  compliance  text, --Ex. cookie-time
  seq int,
  rules map<text,text>, -- Ex. essential,session:comfort,30:analytics,730;retargeting,90
  locs frozen<set<geo_pol>>, --Ex. DE,BW,Baden-Baden, AT (Austria)
  created timestamp,  
  PRIMARY KEY ((regulation), compliance, seq)
)
WITH CLUSTERING ORDER BY (compliance ASC, seq DESC);
CREATE INDEX locs_jurisdictions_idx ON jurisdictions ( full(locs) );
--select * from jurisdictions where locs = {{country : 'us'}} ;

create table agreements (
  vid timeuuid, 
  created timestamp,  
  compliances map<text,frozen<set<text>>>,
  cflags bigint, --compliances represented via N notation == 1001110101111
  sid timeuuid, 
  uid timeuuid, 
  avid timeuuid, --anonymized vid (this is what the vids are converted to)
  hhash text, 
  app text, 
  rel text, 
  url text, --this should always be url slug of **current** url
  ip inet,
  iphash text, 
  gaid text,
  idfa text,
  msid text,
  fbid text,
  country text, 
  region text,
  culture text, 
  source text,
  medium text,
  campaign text,
  term text, 
  ref timeuuid, 
  rcode text, 
  aff text,
  browser text,
  bhash text,
  device text, 
  os text,  
  tz text,
  vp frozen<viewport>,
  loc frozen<geo_pol>,
  latlon frozen<geo_point>,
  zip text,
  owner timeuuid,
  org timeuuid,
  PRIMARY KEY ((vid)) --note all queries should filter hhash
);

--history of all agreements
create table agreed (
  vid timeuuid, 
  created timestamp,  
  compliances map<text,frozen<set<text>>>,
  cflags bigint,
  sid timeuuid, 
  uid timeuuid, 
  avid timeuuid, 
  hhash text, 
  app text, 
  rel text, 
  url text, 
  ip inet,
  iphash text, 
  gaid text,
  idfa text,
  msid text,
  fbid text,
  country text, 
  region text,
  culture text, 
  source text,
  medium text,
  campaign text,
  term text, 
  ref timeuuid, 
  rcode text, 
  aff text,
  browser text,
  bhash text,
  device text, 
  os text,  
  tz text,
  vp frozen<viewport>,
  loc frozen<geo_pol>,
  latlon frozen<geo_point>,
  zip text,
  owner timeuuid,
  org timeuuid,
  PRIMARY KEY ((vid), created) --note all queries should filter hhash
)
WITH CLUSTERING ORDER BY (created DESC);

-- GROWTH Extended --

--total lifetime value, by user
create table tlv (
  hhash text,
  uid timeuuid,
  vid timeuuid,   
  sid timeuuid, 
  payments set<frozen<payment>>,
  paid double,
  org timeuuid,
  updated timestamp,
  updater timeuuid,
  created timestamp,  
  owner timeuuid,
  PRIMARY KEY ((hhash, uid))
);

--total lifetime value, by user, order
create table tlvu (
  hhash text,
  uid timeuuid,
  vid timeuuid,   
  sid timeuuid, 
  orid timeuuid, --order id
  payments set<frozen<payment>>,
  paid double,
  org timeuuid,
  updated timestamp,
  updater timeuuid,
  created timestamp,  
  owner timeuuid,
  PRIMARY KEY ((hhash, uid), orid)
);

--total lifetime value, by vid, order
create table tlvv (
  hhash text,
  uid timeuuid,
  vid timeuuid,   
  sid timeuuid, 
  orid timeuuid, 
  payments set<frozen<payment>>,
  paid double,
  org timeuuid,
  updated timestamp,
  updater timeuuid,
  created timestamp,  
  owner timeuuid,
  PRIMARY KEY ((hhash, vid), orid)
);


