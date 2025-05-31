-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under AGPL v3.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SFPLA

use sfpla;

-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under Apache-2.0.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SF Product Labs MSGXC Schema
--drop keyspace msgxc;
-- CREATE KEYSPACE sfpl WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '1'}  AND durable_writes = true;

-- Create userhosts table and drop the old users table from schema.3.cql
CREATE TABLE IF NOT EXISTS userhosts (
  hhash text,
  uid timeuuid,
  vid timeuuid,   
  sid timeuuid, 
  PRIMARY KEY ((hhash, uid), vid)
);

-- Drop the old users table as we'll define a new one
DROP TABLE IF EXISTS users;

-- Remove services table from schema.3.cql as we'll redefine it
DROP TABLE IF EXISTS services;

-- Remove the old sequences table's data to prevent conflicts
-- UPDATE sequences SET seq = 3 WHERE name = 'DB_VER';

-- Merge with existing tables.

------------------------------ EXISTING START

-- Note: These tables are defined similarly in schema.3.cql, we're dropping and recreating them

create type address (
  fullname text,
  st1 text,
  st2 text,
  city text,
  province text,
  country text,
  zip text,
  active boolean,
  type text,
  phone text
);

create table orgs (
  org      timeuuid,
  parent   timeuuid,
  root timeuuid,
  lname text, --legal name
  hname text, --human name
  notes text,
  roles    set<text>,
  rights   set<text>,
  etype text, --entity type (legal)
  country  text,  
  lang text,
  hq address,
  addresses    map<text, frozen<address>>,
  created timestamp,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  primary key (org)
);

create materialized view conglomerate as 
select * from orgs
where root is not null
primary key (root, org)
with clustering order by (root DESC, org ASC);

create table users (
  uid         timeuuid,
  uname       text,
  pwd         text,
  uhash       text, --username hash
  email       text,
  ehash       text, --email hash
  vids        set<timeuuid>, --visitor ids
  roles      set<text>, --global noun = Ex. ADMIN
  rights     set<text>, --global verb = Ex. EAT_BREAKFAST
  ref timeuuid, --referral
  aff text, --affiliate
  promo text, --promo code
  origin_url text,
  ip text,
  ips set<text>,
  params map<text,text>, --experiment params
  cohorts set<text>, --exp cohorts
  splits map<text,text>, --experimentid : split (xid_splitname)
  lang text,
  created timestamp,
  org timeuuid,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  primary key       (uid)
);
CREATE INDEX user_cohorts_idx ON users ( cohorts );


--Used for update/creation only
create table user_emails (
  email       text,
  uid         timeuuid,
  primary key       (email)
); 
CREATE INDEX user_emails_uid_idx ON user_emails ( uid );

--Used for update/creation only
create table user_usernames (
  uname       text,
  uid         timeuuid,
  primary key       (uname)
);
CREATE INDEX user_usernames_uid_idx ON user_usernames ( uid );

-- The sequences table already exists in schema.3.cql
-- Removing duplicate definition and using an update statement instead
update sequences set seq = 2 where name = 'MSGXC_VER';

--INTERNAL & EXTERNAL SERVICES - Recreating the services table with new structure
create table services (
  svc    text,
  secret  text, --secret hash
  roles   set<text>,
  rights  set<text>,
  expiry date,
  created timestamp,
  org timeuuid,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  primary key       (svc)
);

--EVENTING - Dropping and recreating the queues table with the new structure
DROP TABLE IF EXISTS queues;
DROP INDEX IF EXISTS queues_type_idx;
DROP INDEX IF EXISTS queues_completed_idx;
DROP INDEX IF EXISTS queues_started_idx;

create table queues (
  svc text, --service origin
  qid timeuuid,
  qtype text, --source action/type Ex. sms
  sid timeuuid, --source id Ex. sms-id
  skey text, --source key (if not a uuid) like for cohorts: "name"
  ip text, --Requestor IP
  host text, --Host executing service
  schedule timestamp,
  started timestamp,
  completed timestamp,
  created timestamp,
  org timeuuid,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY ((qid))
)
WITH default_time_to_live = 1209600; --2 week Tombstones
CREATE INDEX queues_svc_idx ON queues ( svc );
CREATE INDEX queues_qtype_idx ON queues ( qtype );
CREATE INDEX queues_completed_idx ON queues ( completed );
CREATE INDEX queues_started_idx ON queues ( started );


-- Dropping and recreating the cohorts table with the new structure
DROP TABLE IF EXISTS cohorts;

create table cohorts (
  cohort text,
  uids_url text, --uids_url
  imported int, --successful imports (count)
  started timestamp,
  completed timestamp,
  created timestamp,
  org timeuuid,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY ((cohort))
);


--limit service usage - already defined in schema.3.cql

-- Esp. Server Debugging - already defined in schema.3.cql

-- Esp. Server Debugging - already defined in schema.3.cql

-- Esp. Server Debugging - already defined in schema.3.cql
------------------------------ EXISTING END


-------------------START ENTIRELY NEW

create type mcert (
  id timeuuid,
  name text,
  algo text, --ex ecc
  spec text, --ex 192spec2k1
  sz text, --serialization type (hex,num,json)
  opub  text, --asymmetric owner public key (der)
  opriv text, --OPTIONAL STORE asymmetric owner private key (hex)
  tpub text, --asymmetric thread public key
  tpriv text, --OPTIONAL STORE asymmetric thread private key
  sym text, --symmetric enc method (aes,rsa,elgamal)
  sver text, --version
  sspec text, --dhpubprivsha256
  skey text, --OPTIONAL symmetric/shared key
  dhpub text,
  dhpriv text, --OPTIONAL
  created timestamp,
  expires timestamp
);


--advanced permission (org and owner in everything else)
create type perm (
  org timeuuid,
  role timeuuid,
  right text,
  owner timeuuid,
  noun text,
  verb text,
  blk boolean --blacklist
);

--security audit table for join requests etc.
CREATE TABLE msec (
    tid timeuuid, --thread id    
    secid timeuuid, --uuid for this record
    perm frozen<perm>, 
    pending boolean,
    approver timeuuid,
    approved timestamp,
    created timestamp,
    org timeuuid,
    owner timeuuid,
    updatedms bigint, --participant updated CHECK
    updated timestamp,
    updater timeuuid,
    PRIMARY KEY ((tid), secid)
)
WITH CLUSTERING ORDER BY (secid DESC);
CREATE INDEX msec_pending_idx ON msec ( pending );

CREATE TABLE mthreads (
  --THREAD INFO
    tid timeuuid, --thread id
    alias text, --human identifier
    name text, --human readable name
    ddata text, --default data to send
    post text, --note/description of thread/post
    mtempl map<text, text>, --default template eg. {email,00001} --template text, --use a url for now
    mcert mcert,
    cats set<text>, --categories
    mtypes set<text>,  --message type (ws - websocket,apn - apple push ios,fcm - firebase/android,wpn - web push notification,sms,em - email,emw - email weekly,emd - email daily, emm - email monthly) 
    fmtypes set<text>, --  specific faluire / fallback mtypes (to override app defaults)
    cmtypes set<text>, --  specific mtypes for anything with a pmid (child messages, to override app defaults)
    urgency int,   
    sys boolean, --system message
    ephemeral int, --seconds until expiry (null == keep)
    archived boolean,
  --PERMISSIONS
    --Prioritized from most specific to most general >>>
    --Priority HIGH
    admins set<timeuuid>, --approve new members etc.
    --Priority MEDIUM
    opens boolean, --publicly subscribable, still add uid to subs [Default: false]
    openp boolean, --publicly publishable, still add uid to pubs [Default: false]
    --Priority LOW
    perms set<frozen<perm>>, --additional perms
  --TRACKING
    app text, --app 
    rel text, 
    ver int, --version
    ptyp text, 
    etyp text,
    ename text,
    auth text, --author
    xid text, --experiment id 
    cohorts set<text>,
    splits map<text,int>, --split=(xid_splitname) : number of participants
    source text,
    medium text,
    campaign text,
    term text, 
    promo text, --promo code to pass through
    ref timeuuid, --referrer uid (Select)
    aff text, --affiliate uname or code (Select)
    provider text, -- Specific provider (Facebook Ads, Google Ads, Mailchimp, Twilio, etc.)
    provider_campaign_id text, -- ID of campaign in provider's system
    provider_account_id text, -- Account ID in provider's system
    provider_metrics map<text, double>, -- Metrics reported by provider
    provider_cost double, -- Cost reported by provider
    provider_status text, -- Status in provider's system
  --RECIPIENTS
    broadcast boolean,
    derive boolean, --update subs based on cohorts/splits
    sent set<timeuuid>, --outstanding uids left to publish to for this experiment
    outs set<timeuuid>, --outstanding uids left to publish to for this experiment
    subs set<timeuuid>, --admins+writers+subs=subs (only this index required to subscribe)
    pubs set<timeuuid>, --uids who can publish to admins+pubs=pubs (only this index required to publish)
    prefs map<timeuuid, frozen<set<text>>>, --msg preferences (user,mtypes) apn,~,wpn etc.  
    ftrack boolean, --track failed deliveries
    strack boolean, --track successful deliveries
  --INTEREST
    interest map<text, int>, -- Ex. { attempted: 542, sent: 500, hearts : 44 , views : 55, clicks etc.}  
    perf map<text,double>, --Performance. {sink : score}
  --IMPRESSION TRACKING
    planned_impressions bigint, -- expected impression count
    actual_impressions bigint, -- actual impression count (manually incremented)
    impression_goal bigint, -- goal for number of impressions
    impression_budget double, -- budget for impressions if paid
    cost_per_impression double, -- actual cost per impression
    vid_targets set<timeuuid>, -- target visitor ids if known
    funnel_stage text, -- awareness, consideration, conversion, retention, advocacy
    total_conversions bigint, -- count of conversions from this thread
    conversion_value double, -- monetary value of conversions
    variants map<text, text>, -- simple variant mapping (id -> json data)
    variant_weights map<text, int>, -- traffic allocation weight by variant
    winner_variant text, -- winning variant after test concludes
  --AUDIENCE TARGETING
    audience_params map<text, text>, -- Psychographic params: age_range, curiosity_level, position, etc.
    audience_metrics map<text, double>, -- Numerical metrics related to audience targeting
    audience_segments set<text>, -- Predefined audience segments this content targets
  --CONTENT ASSUMPTIONS
    content_keywords set<text>, -- Keywords targeted in content creation
    content_assumptions map<text, text>, -- Assumptions made during content creation
    content_metrics map<text, double>, -- Performance metrics by content attributes
    content_intention text, -- Primary intention of the content (inform, persuade, entertain)
  --MULTI-ARMED BANDIT TESTING
    abz_enabled boolean, -- Whether multi-armed bandit testing is enabled
    abz_algorithm text, -- Algorithm type (epsilon-greedy, UCB, Thompson sampling, etc.)
    abz_reward_metric text, -- Primary metric used as reward (conversion, click, revenue)
    abz_reward_value double, -- Current cumulative reward value
    abz_exploration_rate double, -- Exploration rate (epsilon value)
    abz_learning_rate double, -- Learning rate for some algorithms
    abz_start_time timestamp, -- When the test started
    abz_sample_size bigint, -- Current sample size
    abz_min_sample_size bigint, -- Minimum sample size before optimization
    abz_confidence_level double, -- Statistical confidence level
    abz_winner_threshold double, -- Threshold to declare a winner
    abz_auto_optimize boolean, -- Whether to automatically optimize traffic
    abz_auto_stop boolean, -- Whether to automatically stop test when winner found
    abz_status text, -- running, paused, completed
    abz_params map<text, text>, -- Algorithm-specific parameters
    abz_infinite_armed boolean, -- Whether this is an infinite armed bandit test
    abz_param_space map<text, text>, -- Continuous parameter space definitions
    abz_model_type text, -- For infinite armed: GP, random forest, neural network, etc.
    abz_model_params map<text, text>, -- Model hyperparameters
    abz_acquisition_function text, -- EI, UCB, PI, etc. for Bayesian optimization
  --CAMPAIGN MANAGEMENT
    campaign_id timeuuid, -- Campaign this thread belongs to
    campaign_phase text, -- Phase within campaign (teaser, launch, follow-up, etc)
    campaign_priority int, -- Priority within campaign
    campaign_budget_allocation double, -- Budget allocated to this thread within campaign
    campaign_status text, -- active, paused, scheduled, completed, archived
  --ADVANCED ATTRIBUTION
    attribution_model text, -- first-touch, last-touch, linear, time-decay, position-based, custom
    attribution_weight double, -- Weight in multi-touch attribution
    attribution_window int, -- Attribution window in days
    attribution_params map<text, text>, -- Model-specific parameters
  --REGULATORY COMPLIANCE
    requires_consent boolean, -- Whether explicit consent is required
    consent_types set<text>, -- Types of consent required (marketing, profiling, etc.)
    regional_compliance map<text, boolean>, -- Compliance flags by region (GDPR, CCPA, etc.)
    frequency_caps map<text, int>, -- Max frequency by jurisdiction
    data_retention int, -- Retention period in days
  --CONTENT VERSIONING
    content_version int, -- Version number
    content_status text, -- draft, review, approved, published
    content_approver timeuuid, -- Who approved the content
    content_approval_date timestamp, -- When content was approved
    content_history set<timeuuid>, -- References to previous versions
    content_creator timeuuid, -- Who created the content
    content_editors set<timeuuid>, -- Who edited the content
  --PERSONALIZATION RULES
    personalization_enabled boolean, -- Whether personalization is enabled
    personalization_rules map<text, text>, -- Rules for dynamic content selection
    personalization_algorithm text, -- Algorithm used for personalization
    fallback_content map<text, text>, -- Default content if personalization fails
    personalization_segments map<text, text>, -- Segment-specific content mappings
  --INTEGRATION REFERENCES
    external_ids map<text, text>, -- IDs in external systems (CRM, CDP, etc.)
    webhook_configs map<text, text>, -- Webhook configurations
    api_references map<text, text>, -- API reference tracking
  --COMPETITIVE INTELLIGENCE
    competitor_references set<text>, -- References to competitor campaigns
    competitive_position text, -- Market positioning
    competitive_analysis text, -- Analysis of competitive landscape
  --PERFORMANCE BENCHMARKING
    industry_benchmarks map<text, double>, -- Industry standard metrics
    internal_benchmarks map<text, double>, -- Company historical benchmarks
    benchmark_comparison map<text, double>, -- Performance vs benchmarks
  --OWNER
    deleted timestamp,
    created timestamp,
    org timeuuid,
    owner timeuuid,
    updatedms bigint, --participant updated CHECK
    updated timestamp,
    updater timeuuid,
    PRIMARY KEY ((tid))
);

--TRIAGE
--Messages that are in the middle of being processed
--IF threadid==messageid = 1st Messsage!!!
create table mtriage (
  --MESSAGE
  tid timeuuid, --threadid
  mid timeuuid, --message id
  pmid timeuuid, --parent message id = CHILD
  subject text,
  msg text, --text
  data text, --json
  urgency int,  
  sys boolean, --system message 
  broadcast boolean,
  mtempl text, --use a url for now (see mthreads)
  repl map<text,text>, --text to replace ex Hi {{fn}} -> becomes -> Hi Andrew, Prepend "text." to use Raw text instead of the user record Ex. {{text.msg}}. Only start off with one "text." message body in Admin UI (text.msg)
  --SCHEDULE
  svc text, --Ex. SES, 'message', 'sms, 'action'
  qid timeuuid, --executing queue id -- IMPORTANT !!!!
  rid timeuuid, --Relation id id Ex. message-id
  relation text, --Relation source audit id, inc. external Ex. xcs.mthread
  meta map<text,text>, --METADATA, SPLIT etc.
  scheduled timestamp,
  started timestamp,
  completed timestamp,
  --AUDITING
  mtypes set<text>, --attempted mtypes (update after actually sent)
  users set<timeuuid>, --keep this here for pull mid > lastloggedin
  deliveries set<timeuuid>, --uid delivered, update after ACK of wsocket
  failures set<timeuuid>, --uid failures
  --TRACKING
  xid text,
  split text,
  --PERMISSIONS
  perms set<frozen<perm>>, --additional perms 
  --OWNERSHIP
  deleted timestamp,
  keep boolean, --keep this message on the server
  createdms bigint, --INDEX and use this for last-seen & alerts ALONGWITH users
  created timestamp,
  org timeuuid,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY ((tid), mid)
)
WITH CLUSTERING ORDER BY (mid DESC)
AND default_time_to_live = 3628800 --Tombstones after 6 weeks
AND GC_GRACE_SECONDS = 3600; --Force regular tombstone clearouts


--PERMANENT ARCHIVE
--Store for messages that have been scheduled or sent
--IF threadid==messageid = 1st Messsage!!!
create table mstore (
  --MESSAGE
  tid timeuuid, --threadid
  mid timeuuid, --message id
  pmid timeuuid, --parent message id = CHILD
  subject text,
  msg text, --text
  data text, --json
  urgency int,  
  sys boolean, --system message 
  broadcast boolean,
  mtempl text, --use a url for now (see mthreads)
  repl map<text,text>, --text to replace ex Hi {{fn}} -> becomes -> Hi Andrew, Prepend "text." to use Raw text instead of the user record Ex. {{text.msg}}. Only start off with one "text." message body in Admin UI (text.msg)
  --SCHEDULE
  svc text, --Ex. SES, 'message', 'sms, 'action'
  qid timeuuid, --executing queue id -- IMPORTANT !!!!
  rid timeuuid, --Relation id id Ex. message-id
  relation text, --Relation source audit id, inc. external Ex. xcs.mthread
  meta map<text,text>, --METADATA etc.
  planned timestamp,
  scheduled timestamp, --scheduled is removed once processed to limit index size
  started timestamp,
  completed timestamp,
  --AUDITING
  mtypes set<text>, --attempted mtypes (update after actually sent)
  users set<timeuuid>, --keep this here for pull mid > lastloggedin
  deliveries set<timeuuid>, --uid delivered, update after ACK of wsocket
  failures set<timeuuid>, --uid failures
  --TRACKING
  xid text,
  split text,
  --PERMISSIONS
  perms set<frozen<perm>>, --additional perms 
  --OWNERSHIP
  deleted timestamp,
  keep boolean, --keep this message on the server
  createdms bigint, --INDEX and use this for last-seen & alerts ALONGWITH users
  created timestamp,
  org timeuuid,
  owner timeuuid,
  updated timestamp,
  updater timeuuid, 
  --ADDITIONAL FIELDS
  interest map<text, int>, -- Ex. { attempted: 542, sent: 500, hearts : 44 , views : 55, clicks etc.} 
  perf map<text,double>, --Performance. {sink : score}
  hide timestamp, --use ephemeral value in thread
  hidden boolean, --REQUIRED: set to false
  funnel_stage text, -- match with threads
  conversion_events bigint, -- count of conversions from this message
  PRIMARY KEY ((tid), mid)
)
WITH CLUSTERING ORDER BY (mid DESC);
CREATE INDEX mstore_scheduled_idx ON mstore ( scheduled ); --Make scheduled null once processed

create type mdevice (
  mtype text,
  did text,
  updated timestamp
);

create table mfailures (
 tid timeuuid,
 mid timeuuid, --mtriageid
 uid timeuuid,
 mtype text,
 mdevice frozen<mdevice>,
 failure text, --failure type (Ex. nopened, noack)
 retries int, 
 died timestamp, --stop retries
 created timestamp,
 org timeuuid,
 owner timeuuid,
 updated timestamp,
 updater timeuuid,
 PRIMARY KEY ((tid), mid, uid, mtype, mdevice)
)
WITH CLUSTERING ORDER BY (mid DESC);

-- Add the messaging columns to the users table
alter table users add mdevices list<frozen<mdevice>>; --prioritized list of devices
alter table users add mtypes list<text>; --[mtype] prioritized list of preferences ~ => disabled
alter table users add mlast bigint; --(ms) last updated/looked at a mxc messages
alter table users add cell text; 
alter table users add chash text; --cell hash 
alter table users add mcerts set<frozen<mcert>>; 

-------------------END ENTIRELY NEW
-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under Apache-2.0.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE


--Describe the splits
create table splits (
  cohort text, --Ex. males, * = special case, all users by default
  sfam text, --Split family (we can have multiple representations of the same cohort), this is a grouping element only and in a lot of instances cohort=family. Ex. (for cohort->family), Males->males or Males->males2020 or another better example might be *->AB
  split text, --Ex. A
  seq int, --Ex. 1 for first split
  users set<timeuuid>, --set of users in this split
  pct double, --**CANNOT** have both users and pct (percent)! Percents in a family must equal 100. Ex. for A/B, 50.0
  created timestamp,
  org timeuuid,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (split) --split must be unique and could actually look like (cohort,family,split) but we want split to be unique across all cohorts
);

-------------------START EXPERIMENTAL SCHEMA

-- To run:
-- cqlsh --ssl -f schema.1.cql 

-- geo_point is already defined in schema.3.cql, we'll use the existing type
-- create type geo_point (
--  lat double,
--  lon double
-- );

alter table users add latlon frozen<geo_point>; --location
alter table users add caption text; -- what a user calls themselves
alter table users add gender boolean;
alter table users add dob date;
alter table users add image_url text; 
alter table users add image_url_original text; 
alter table users add v_score decimal; 
alter table users add fn text; 
alter table users add ln text; 
alter table users add locked timestamp;
alter table users add active timestamp;
alter table users add magic text;
alter table users add magic_exp timestamp;
alter table users add magic_attempts int;
alter table users add magic_attempted timestamp;
alter table users add terms_accepted timestamp;
alter table users add perms map<timeuuid,frozen<map<text,text>>>; --org specific: org, claim[yes]
CREATE INDEX IF NOT EXISTS user_cell_idx ON users ( cell );

alter table orgs add host text;
alter table orgs add hhash text;
alter table orgs add taxid  text;
alter table orgs add terms_accepted timestamp;
alter table orgs add mcerts set<frozen<mcert>>; 
alter table orgs add expiry date;
alter table orgs add email text;
alter table orgs add phone text;

insert into sequences (name, seq) values('SFPL_VER',1);
CREATE INDEX IF NOT EXISTS user_uname_idx ON users ( uname );
--Used for update/creation only
create table user_cells (
  cell       text,
  uid         timeuuid,
  primary key       (cell)
);
CREATE INDEX IF NOT EXISTS user_cells_uid_idx ON user_cells ( uid );

create table user_verifications (
  uid         timeuuid,
  vmethod text,
  verifier timeuuid,
  created timestamp,
  org timeuuid,
  owner timeuuid,
  updated timestamp,
  updater timeuuid,
  primary key       ((uid), vmethod)
);

create table files (
  slug text,
  fprint text, --hash fingerprint sha256
  ftype text, --v=video, i=image, d=doc 
  suffix text, --jpg, jpeg doc etc.
  name text, --new/reconstructed (optimized) filename
  mime text, --default client action (new) mime-type
  description text, --notes entered at upload
  latlon frozen<geo_point>,
  oname text, --original filename
  ocreated timestamp, --original created date
  omime text, --original mime-type
  alt text, --alt tag
  meta map<text,text>, --metadata
  x int, --width pixels
  y int, --height pixels
  size int, --original file size
  private boolean, --requires cookie/auth
  bucket text,
  url_prefix text, --private/public/etc if we proxy
  ourl text, --original url
  hqurl text,
  mqurl text,
  lqurl text,
  thumb text,
  used_in set<text>, --ex. controllers/slugs
  owner timeuuid,
  o_r boolean, --owner read
  o_w boolean, --owner write
  orgs set<timeuuid>,
  g_r boolean, --org/group read
  g_w boolean, --org/group write
  roles set<text>,
  r_r boolean, --role-read
  r_w boolean, --role write
  created timestamp,
  updated timestamp,
  expires timestamp,
  updater timeuuid,
  PRIMARY KEY (slug)
);

--Payment Provider
create table pprovider(
    name text,
    created timestamp,
    owner timeuuid,
    updated timestamp,
    updater timeuuid,
    primary key (name)
);
CREATE INDEX pprovider_owner_idx ON pprovider ( owner );

--Payment Authorization
create table pauth(
    id timeuuid,
    pprovider text,
    pcustomer text,
    psource text,
    meta  text, --token response
    priority int,
    active timestamp,
    created timestamp,
    owner timeuuid,
    updated timestamp,
    updater timeuuid,
    primary key (id)
);
CREATE INDEX pauth_owner_idx ON pauth ( owner );

create table pconfirmation (
    id text, --Ex. evt_3434234234
    ctype text, --Ex. create.customer charge.succeeded
    ref text, --Ex. cus_34234234324
    rtype text, --Ex. customer
    meta   text, --token response
    created timestamp,
    owner timeuuid,
    updated timestamp,
    updater timeuuid,
    primary key (id)
);

CREATE TABLE permissions (
    oid timeuuid,    -- org id
    rtype text,      -- resource type: "folder"
    rpath text,             -- resource path: /* or /folder1/x.pdf
    obj text,              -- user or org or * (everyone)
    ref uuid,         --user or org   00000000-0000-0000-0000-000000000000 corresponds to "everyone"
    action text,    -- "OPEN" etc (for standard rights) or "*" (for all rights) or "folder:SYNC" (for custom rights)
    effect boolean,  -- True = "allow" or False = "deny"
    PRIMARY KEY ((oid, rtype), rpath, ref, action)
);

CREATE CUSTOM INDEX permissions_oid_idx ON permissions (oid) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';  
CREATE CUSTOM INDEX rpath_oid_idx ON permissions (rpath) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';  
--CREATE CUSTOM INDEX obj_oid_idx ON permissions (obj) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';  
CREATE CUSTOM INDEX ref_oid_idx ON permissions (ref) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';  
CREATE CUSTOM INDEX action_oid_idx ON permissions (action) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex';  

-- NOTES:
-- WE MUST LIMIT THE RPATH TO 10 DIVISIONS /../.../...

-- SELECT * FROM permissions 
--   where 
--     oid = 982c4900-3256-11ef-9772-1e1480865e16 
--     AND rtype = 'folder' 
--     AND rpath IN ('/', '/default', '/default/folder1', '/default/folder1/file1')
--     --AND obj IN ('*', 'u', 'g') -- dont need this in query as we have 00000000-0000-0000-0000-000000000000 for everyone
--     AND ref IN (00000000-0000-0000-0000-000000000000,982c4900-3256-11ef-9772-1e1480865e16)
--     AND action IN ('folder:SYNC', '*', 'OPEN')
--     --AND effect = true -- dont need this for query BUT MUST CROSS-CHECK IN CODE
--     ;


	-- ACTIONS
  ------------

	-- LIST                       //Ex. see file in folder
	-- OPEN                       //Open the document & see properties
	-- COMMENT                    //Comment in sidebar
	-- APPEND                     //Non destructive changes
	-- COPY                       //Clone a file to my directory
	-- EDIT                       //Destructive changes
	-- SHARE_INTERNAL             //Share to someone inside the original org
	-- SHARE_EXTERNAL             //Share to someone outside the original org
	-- MOVE                       //Move the document
	-- DESTROY
	-- ADMIN
	-- IMPEROSONATE

-------------------END EXPERIMENTAL SCHEMA

-------------------START VISITOR IMPRESSION TRACKING

-- Thread-Visitor Relationship (for anonymous tracking)
create table thread_visitors (
  tid timeuuid, -- thread id
  vid timeuuid, -- visitor id from cookie
  variant_id text, -- which variant they saw
  first_impression timestamp,
  last_impression timestamp,
  impression_count int,
  interactions set<text>, -- types of interactions (click, view, etc)
  conversion boolean, -- whether this visitor converted
  conversion_value double, -- value of conversion if any
  PRIMARY KEY ((tid), vid)
);

-- Daily impression tracking
create table impression_daily (
  tid timeuuid, -- thread id
  day date, -- for daily aggregation
  variant_id text, -- 'all' for all variants or specific variant id
  total_impressions bigint, -- manually incremented count
  anonymous_impressions bigint, -- from visitors without uid
  identified_impressions bigint, -- from known users
  unique_visitors bigint, -- unique visitors
  conversions bigint, -- conversion events
  updated timestamp, -- last update time
  PRIMARY KEY ((tid), day, variant_id)
) WITH CLUSTERING ORDER BY (day DESC, variant_id ASC);

-- Channel-specific Tracking
create table channel_metrics (
  channel text, -- channel name (web, social, etc)
  day date, -- date for daily rollup
  impressions bigint, -- total impressions
  unique_visitors bigint, -- unique visitors
  clicks bigint, -- click events
  conversions bigint, -- conversion events
  conversion_value double, -- value of conversions
  updated timestamp, -- last update time
  PRIMARY KEY ((channel), day)
) WITH CLUSTERING ORDER BY (day DESC);

-- Hourly metrics (if needed)
create table impression_hourly (
  tid timeuuid, -- thread id
  day date, 
  hour int, -- 0-23
  variant_id text, -- 'all' for all variants or specific variant id
  total_impressions bigint,
  anonymous_impressions bigint,
  identified_impressions bigint,
  unique_visitors bigint,
  conversions bigint,
  updated timestamp,
  PRIMARY KEY ((tid, day), hour, variant_id)
) WITH CLUSTERING ORDER BY (hour ASC, variant_id ASC);

insert into sequences (name, seq) values('VISITOR_TRACKING_VER',1);

-------------------END VISITOR IMPRESSION TRACKING

-------------------START EXTENDED MARKETING FEATURES

-- Campaign Management
create table campaigns (
  campaign_id timeuuid,
  name text,
  description text,
  objective text, -- Awareness, consideration, conversion, etc.
  start_date timestamp,
  end_date timestamp,
  status text, -- planning, active, paused, completed
  total_budget double,
  spent_budget double,
  primary_channel text,
  channels set<text>,
  threads set<timeuuid>, -- All threads in this campaign
  kpis map<text, double>, -- Key performance indicators and targets
  results map<text, double>, -- Actual results
  success_metrics set<text>, -- Which metrics determine success
  owner timeuuid,
  team set<timeuuid>, -- Team members involved
  tags set<text>,
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (campaign_id)
);

-- Cross-channel Journey Tracking
create table customer_journeys (
  journey_id timeuuid,
  uid timeuuid, -- User ID
  vid timeuuid, -- Visitor ID (if anonymous)
  journey_name text, -- Name of journey template if following one
  touchpoints list<frozen<map<text, text>>>, -- Ordered list of touchpoints
  channel_sequence list<text>, -- Sequence of channels used
  thread_sequence list<timeuuid>, -- Sequence of threads interacted with
  variant_sequence list<text>, -- Sequence of variants seen
  time_between_touchpoints list<bigint>, -- Time in seconds between touchpoints
  journey_start timestamp,
  journey_end timestamp,
  journey_status text, -- active, completed, abandoned
  conversion_value double,
  conversion_point int, -- At which touchpoint conversion happened
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((uid), journey_id)
) WITH CLUSTERING ORDER BY (journey_id DESC);

-- Journey Templates
create table journey_templates (
  template_id timeuuid,
  name text,
  description text,
  objective text,
  steps list<frozen<map<text, text>>>, -- Predefined journey steps
  expected_duration int, -- Expected journey duration in days
  target_audience set<text>, -- Target audience segments
  success_criteria text,
  avg_conversion_rate double,
  active boolean,
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (template_id)
);

-- Performance Benchmarks
create table benchmarks (
  benchmark_id text, -- Industry, category, etc.
  metric text, -- CTR, conversion rate, etc.
  channel text, -- Email, social, search, etc.
  industry text,
  audience_segment text,
  timeframe text, -- Last updated timeframe
  value double, -- Benchmark value
  source text, -- Source of benchmark data
  confidence double, -- Confidence level
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((benchmark_id), metric, channel)
) WITH CLUSTERING ORDER BY (metric ASC, channel ASC);

-- Content Library
create table content_library (
  content_id timeuuid,
  title text,
  description text,
  content_type text, -- article, image, video, etc.
  content_url text,
  keywords set<text>,
  categories set<text>,
  personas set<text>, -- Target personas
  lifecycle_stage text, -- Awareness, consideration, decision
  author timeuuid,
  approval_status text,
  usage_count int, -- How many times used
  performance_score double, -- Overall performance rating
  performance_metrics map<text, double>, -- Specific performance metrics
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (content_id)
);

-- Attribution Models
create table attribution_models (
  model_id text,
  name text,
  description text,
  model_type text, -- first-touch, last-touch, linear, time-decay, position-based, custom
  parameters map<text, text>, -- Model-specific parameters
  decay_function text, -- For time-decay models
  position_weights list<double>, -- For position-based models
  custom_logic text, -- For custom models
  attribution_window int, -- In days
  lookback_window int, -- In days
  channels set<text>, -- Applicable channels
  active boolean,
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (model_id)
);

-- Regulatory Compliance Settings
create table compliance_settings (
  region text, -- GDPR, CCPA, etc.
  setting_key text, -- consent_required, max_frequency, etc.
  setting_value text,
  description text,
  applies_to set<text>, -- Channels or message types
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY ((region), setting_key)
) WITH CLUSTERING ORDER BY (setting_key ASC);

-- Competitor Analysis
create table competitors (
  competitor_id text,
  name text,
  website text,
  primary_channels set<text>,
  strengths set<text>,
  weaknesses set<text>,
  positioning text,
  target_segments set<text>,
  campaign_examples map<text, text>, -- Campaign name to description
  benchmarks map<text, double>, -- Metric to value
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (competitor_id)
);

-- Integration Configurations
create table integrations (
  integration_id text, -- CRM, CDP, etc.
  name text,
  provider text,
  api_key text,
  api_endpoint text,
  webhook_url text,
  mapping map<text, text>, -- Field mappings
  sync_frequency text,
  last_sync timestamp,
  status text,
  error_log text,
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (integration_id)
);

-- Marketing Channel Providers
create table channel_providers (
  provider_id text, -- Unique provider identifier
  name text, -- Display name (Facebook Ads, Mailchimp, Twilio, etc.)
  channel text, -- Primary channel (social, email, sms, etc.)
  channels set<text>, -- All supported channels
  api_credentials map<text, text>, -- Credentials (encrypted/tokenized)
  api_endpoint text, -- Base API endpoint
  webhook_url text, -- Webhook for callbacks
  default_settings map<text, text>, -- Default configuration
  rate_limits map<text, int>, -- API rate limits
  pricing_model text, -- CPM, CPC, subscription, etc.
  cost_structure map<text, double>, -- Pricing details
  performance_metrics map<text, double>, -- Overall provider performance
  active boolean, -- Whether provider is active
  notes text,
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (provider_id)
);

-- Provider Campaigns
create table provider_campaigns (
  provider_id text, -- Channel provider
  campaign_id text, -- ID in provider's system
  internal_campaign_id timeuuid, -- Reference to internal campaign
  internal_thread_id timeuuid, -- Reference to internal thread
  name text, -- Campaign name in provider's system
  status text, -- Status in provider's system
  start_date timestamp,
  end_date timestamp,
  budget double,
  spent double,
  impressions bigint,
  clicks bigint,
  conversions bigint,
  metrics map<text, double>, -- All metrics from provider
  targeting json, -- Provider-specific targeting settings
  creative_ids set<text>, -- IDs of creatives in provider's system
  last_sync timestamp, -- Last data sync
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY ((provider_id), campaign_id)
) WITH CLUSTERING ORDER BY (campaign_id ASC);

-- Provider Account Metrics
create table provider_metrics (
  provider_id text,
  date date,
  account_id text, -- Account ID in provider's system
  spend double,
  impressions bigint,
  clicks bigint,
  conversions bigint,
  metrics map<text, double>, -- All metrics from provider
  updated timestamp,
  PRIMARY KEY ((provider_id), date, account_id)
) WITH CLUSTERING ORDER BY (date DESC, account_id ASC);

-- Aggregate Channel Performance
create table channel_performance (
  channel text, -- email, social, search, display, etc.
  provider_id text, -- Specific provider
  date date,
  impressions bigint,
  clicks bigint,
  conversions bigint,
  spend double,
  revenue double,
  roi double,
  metrics map<text, double>, -- Other performance metrics
  updated timestamp,
  PRIMARY KEY ((channel), date, provider_id)
) WITH CLUSTERING ORDER BY (date DESC, provider_id ASC);

insert into sequences (name, seq) values('MARKETING_EXTENDED_VER',1);

-------------------END EXTENDED MARKETING FEATURES

-------------------START ALGORITHMIC TESTING FEATURES

-- Variant Performance for Multi-armed Bandit
create table variant_performance (
  tid timeuuid, -- Thread ID
  variant_id text, -- Variant identifier
  impressions bigint, -- Number of impressions
  rewards bigint, -- Number of rewards (conversions, clicks, etc.)
  reward_value double, -- Cumulative reward value
  probability double, -- Current probability of showing this variant
  expected_value double, -- Expected value based on algorithm
  confidence_interval_lower double, -- Lower bound of confidence interval
  confidence_interval_upper double, -- Upper bound of confidence interval
  standard_deviation double, -- Standard deviation of rewards
  last_served timestamp, -- Last time this variant was served
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((tid), variant_id)
);

-- Multi-armed Bandit Decision Log
create table bandit_decisions (
  tid timeuuid, -- Thread ID
  decision_time timestamp, -- When decision was made
  decision_id timeuuid, -- Unique decision ID
  chosen_variant text, -- Which variant was chosen
  context map<text, text>, -- Decision context
  reward_received boolean, -- Whether reward was received
  reward_value double, -- Value of reward if received
  reward_time timestamp, -- When reward was received
  algorithm_state text, -- Serialized state of algorithm at decision time
  exploration boolean, -- Whether this was exploration or exploitation
  cumulative_regret double, -- Estimated regret at this point
  PRIMARY KEY ((tid), decision_time, decision_id)
) WITH CLUSTERING ORDER BY (decision_time DESC, decision_id ASC);

-- Algorithm Configurations
create table abz_algorithms (
  algorithm_id text, -- Unique algorithm identifier
  name text, -- Algorithm name
  description text, -- Algorithm description
  type text, -- epsilon-greedy, UCB, Thompson sampling, etc.
  parameters map<text, text>, -- Default parameters
  exploration_parameter text, -- Parameter controlling exploration
  supports_contextual boolean, -- Whether algorithm supports contextual bandits
  supports_infinite_arms boolean, -- Whether algorithm supports infinite arms
  model_based boolean, -- Whether algorithm is model-based
  code_reference text, -- Reference to implementation
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (algorithm_id)
);

-- Multi-armed Bandit Tests
create table abz_tests (
  test_id timeuuid, -- Unique test identifier
  tid timeuuid, -- Thread ID
  name text, -- Test name
  description text, -- Test description
  algorithm_id text, -- Algorithm used
  algorithm_params map<text, text>, -- Algorithm parameters for this test
  reward_metric text, -- Metric used as reward
  variants set<text>, -- Variants in test
  start_time timestamp,
  end_time timestamp,
  status text, -- running, paused, completed
  winner_variant text, -- Winning variant if test completed
  confidence_level double, -- Confidence in result
  min_sample_size bigint, -- Minimum sample size required
  actual_sample_size bigint, -- Actual sample size achieved
  exploration_rate double, -- Exploration rate used
  auto_optimize boolean, -- Whether auto-optimization was enabled
  optimization_start timestamp, -- When optimization began (after min sample)
  contextual_features set<text>, -- Features used for contextual decisions
  notes text,
  infinite_armed boolean, -- Whether using infinite armed approach
  parameter_space json, -- JSON description of parameter space
  model_type text, -- Model type for infinite armed
  acquisition_function text, -- Acquisition function for Bayesian optimization
  parent_test_id timeuuid, -- Parent test for hierarchical experiments
  optimization_level text, -- 'local', 'global', 'hybrid'
  optimization_scope text, -- What's being optimized: 'content', 'targeting', 'timing', etc.
  search_radius double, -- For local optimization: radius around current best point
  knowledge_transfer boolean, -- Whether to transfer knowledge to/from related tests
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (test_id)
);

-- Contextual Bandit Features
create table contextual_features (
  feature_id text, -- Feature identifier
  name text, -- Feature name
  description text, -- Feature description
  data_type text, -- numeric, categorical, boolean
  source text, -- user, session, event, etc.
  path text, -- Path to extract feature
  preprocessing text, -- Preprocessing steps
  importance double, -- Feature importance
  buckets int, -- Number of buckets for discretization
  active boolean, -- Whether feature is active
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (feature_id)
);

-- User Context for Contextual Bandits
create table user_context (
  uid timeuuid, -- User ID
  vid timeuuid, -- Visitor ID
  context_time timestamp, -- When context was captured
  features map<text, text>, -- Feature values
  segments set<text>, -- Computed segments
  PRIMARY KEY ((uid, vid), context_time)
) WITH CLUSTERING ORDER BY (context_time DESC);

-- Add tables for infinite armed bandits

-- Parameter Space Definitions
create table parameter_spaces (
  space_id text, -- Unique space identifier
  name text, -- Space name
  description text, -- Description
  dimensions map<text, frozen<map<text, text>>>, -- Parameter dimensions with min, max, type, etc.
  constraints text, -- Constraints on parameter combinations
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (space_id)
);

-- Continuous Parameter Trials
create table parameter_trials (
  trial_id timeuuid, -- Unique trial identifier
  test_id timeuuid, -- Reference to abz_test
  parameters map<text, double>, -- Parameter values for this trial
  reward double, -- Observed reward
  uncertainty double, -- Uncertainty estimate
  iteration int, -- Iteration number
  expected_improvement double, -- Expected improvement
  trial_time timestamp, -- When trial was conducted
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((test_id), trial_id)
) WITH CLUSTERING ORDER BY (trial_id DESC);

-- Model States for Infinite Armed Bandits
create table model_states (
  test_id timeuuid, -- Reference to abz_test
  iteration int, -- Iteration number
  model_type text, -- GP, random forest, etc.
  model_state text, -- Serialized model state
  hyperparameters map<text, double>, -- Model hyperparameters
  performance_metrics map<text, double>, -- Model performance metrics
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((test_id), iteration)
) WITH CLUSTERING ORDER BY (iteration DESC);

insert into sequences (name, seq) values('ABZ_TESTING_VER',1);

-------------------END ALGORITHMIC TESTING FEATURES

-------------------START HIERARCHICAL OPTIMIZATION FEATURES

-- Hierarchical Experiment Structure
create table experiment_hierarchy (
  root_experiment_id timeuuid, -- Top-level experiment
  experiment_id timeuuid, -- This experiment
  parent_id timeuuid, -- Parent experiment (null for root)
  level int, -- Hierarchy level (0 for root)
  path text, -- Path from root e.g., "root/branch1/branch2"
  description text, -- Purpose of this level
  optimization_level text, -- 'local', 'global', 'hybrid'
  optimization_scope text, -- What's being optimized at this level
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((root_experiment_id), experiment_id)
) WITH CLUSTERING ORDER BY (experiment_id ASC);

-- Knowledge Transfer Between Experiments
create table experiment_knowledge (
  source_experiment_id timeuuid, -- Source experiment
  target_experiment_id timeuuid, -- Target experiment
  transfer_type text, -- 'model', 'parameters', 'insights', etc.
  transfer_time timestamp,
  transfer_data text, -- Serialized knowledge being transferred
  parameters_transferred map<text, text>, -- Parameters that were transferred
  performance_before map<text, double>, -- Performance metrics before transfer
  performance_after map<text, double>, -- Performance metrics after transfer
  transfer_success boolean,
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((source_experiment_id, target_experiment_id), transfer_time)
) WITH CLUSTERING ORDER BY (transfer_time DESC);

-- Global vs Local Performance Comparison
create table optimization_comparison (
  root_experiment_id timeuuid, -- Top-level experiment
  comparison_time timestamp,
  global_best map<text, text>, -- Best global parameters
  global_reward double, -- Best global reward
  global_confidence double, -- Confidence in global result
  local_experiments map<timeuuid, frozen<map<text, text>>>, -- Local experiment results
  local_best map<text, text>, -- Best local parameters
  local_reward double, -- Best local reward
  local_confidence double, -- Confidence in local result
  improvement_global_to_local double, -- Improvement from global to local
  recommendation text, -- Recommended action based on comparison
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((root_experiment_id), comparison_time)
) WITH CLUSTERING ORDER BY (comparison_time DESC);

-- Parameter Space Regions
create table parameter_regions (
  experiment_id timeuuid, -- Experiment ID
  region_id text, -- Region identifier
  region_type text, -- 'promising', 'explored', 'unexplored', etc.
  center_point map<text, double>, -- Center of region
  radius double, -- Radius or size of region
  dimensions map<text, frozen<map<text, double>>>, -- Min/max bounds for each dimension
  performance_statistics map<text, double>, -- Stats about region performance
  exploration_count int, -- How many times region was explored
  priority double, -- Priority for future exploration
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((experiment_id), region_id)
);

-- Multi-level Optimization Strategy
create table optimization_strategies (
  strategy_id text, -- Strategy identifier
  name text, -- Strategy name
  description text, -- Strategy description
  levels int, -- Number of hierarchy levels
  level_config map<int, frozen<map<text, text>>>, -- Configuration for each level
  exploration_allocation map<text, double>, -- Resource allocation between levels
  knowledge_sharing_rules map<text, text>, -- Rules for sharing knowledge
  termination_conditions map<text, text>, -- When to terminate strategy
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (strategy_id)
);

insert into sequences (name, seq) values('HIERARCHICAL_OPT_VER',1);

-------------------END HIERARCHICAL OPTIMIZATION FEATURES

-------------------START PRODUCT MARKET FIT FEATURES

-- User Satisfaction Surveys
create table pmf_surveys (
  survey_id timeuuid,
  uid timeuuid, -- User ID
  survey_type text, -- NPS, CSAT, PMF, etc.
  score int, -- Numerical score (0-10 for NPS, etc.)
  very_disappointed boolean, -- Sean Ellis "very disappointed" metric
  primary_value text, -- Main value derived from product
  primary_use_case text, -- Main use case
  alternatives set<text>, -- Alternative solutions mentioned
  feature_requests set<text>, -- Requested features
  pain_points set<text>, -- Pain points mentioned
  verbatim text, -- Raw feedback text
  survey_sent timestamp,
  survey_completed timestamp,
  segment text, -- User segment
  persona text, -- User persona
  tenure int, -- Days as customer
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((uid), survey_id)
) WITH CLUSTERING ORDER BY (survey_id DESC);

-- Cohort Retention Analysis
create table retention_cohorts (
  cohort_date date, -- Acquisition date cohort
  cohort_type text, -- acquisition_date, feature_adoption, etc.
  cohort_id text, -- Identifier for this cohort
  segment text, -- User segment
  users set<timeuuid>, -- Users in cohort
  size int, -- Number of users in cohort
  day_1 int, -- Count of active users on day 1
  day_3 int,
  day_7 int,
  day_14 int,
  day_30 int,
  day_60 int,
  day_90 int,
  retention map<int, double>, -- Day -> retention rate
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((cohort_type), cohort_date, cohort_id)
) WITH CLUSTERING ORDER BY (cohort_date DESC, cohort_id ASC);

-- Product Usage Metrics
create table product_usage (
  uid timeuuid, -- User ID
  date date,
  session_count int, -- Number of sessions
  total_time bigint, -- Time spent in seconds
  features_used set<text>, -- Features used
  feature_usage map<text, int>, -- Feature -> usage count
  core_action_count int, -- Count of core actions
  depth_score double, -- Engagement depth score
  frequency_score double, -- Usage frequency score
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((uid), date)
) WITH CLUSTERING ORDER BY (date DESC);

-- Feature Usage Analytics
create table feature_usage (
  feature_id text, -- Feature identifier
  date date,
  segment text, -- User segment
  unique_users int, -- Number of unique users
  total_uses int, -- Total usage count
  avg_uses_per_user double, -- Average uses per user
  adoption_rate double, -- % of users who use this feature
  retention_impact double, -- Correlation with retention
  satisfaction_impact double, -- Correlation with satisfaction
  engagement_time bigint, -- Time spent with feature
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((feature_id), date, segment)
) WITH CLUSTERING ORDER BY (date DESC, segment ASC);

-- PMF Segment Analysis
create table pmf_segments (
  segment_id text, -- Segment identifier
  segment_name text,
  segment_description text,
  criteria map<text, text>, -- Defining criteria
  size int, -- Number of users in segment
  very_disappointed_pct double, -- % who would be very disappointed
  satisfaction_score double, -- Average satisfaction
  growth_rate double, -- Growth rate
  retention_rate double, -- Retention rate
  revenue double, -- Revenue from segment
  ltv double, -- Lifetime value
  acquisition_cost double, -- Acquisition cost
  feature_priorities set<text>, -- Priority features for segment
  primary_value set<text>, -- Main value propositions
  use_cases set<text>, -- Main use cases
  updated_date date,
  created timestamp,
  updated timestamp,
  PRIMARY KEY (segment_id)
);

-- Product-Channel Fit
create table channel_fit (
  channel text, -- Marketing channel
  segment text, -- User segment
  cac double, -- Customer acquisition cost
  conversion_rate double, -- Conversion rate
  activation_rate double, -- Activation rate
  retention_rate double, -- Retention rate
  ltv double, -- Lifetime value
  roi double, -- Return on investment
  payback_period int, -- Payback period in days
  pmf_score double, -- Product-channel fit score (0-100)
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((channel), segment)
);

-- Pricing Analysis
create table pricing_feedback (
  uid timeuuid, -- User ID
  price_point double, -- Tested price point
  willingness boolean, -- Willing to pay this price
  max_price double, -- Maximum price willing to pay
  value_rating int, -- Value for money rating (1-10)
  comparable_products set<text>, -- Products compared to
  comparable_prices map<text, double>, -- Comparable product prices
  features_worth_paying set<text>, -- Features worth paying for
  deal_breakers set<text>, -- Deal breaker features
  survey_id timeuuid, -- Related survey
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((uid), price_point)
) WITH CLUSTERING ORDER BY (price_point ASC);

-- Qualitative Feedback
create table qualitative_feedback (
  feedback_id timeuuid,
  uid timeuuid, -- User ID
  source text, -- survey, support, interview, etc.
  category text, -- feature request, bug, praise, etc.
  sentiment text, -- positive, negative, neutral
  feedback_text text, -- The actual feedback
  features set<text>, -- Related features
  tags set<text>, -- Analysis tags
  priority int, -- Priority (1-5)
  status text, -- new, reviewed, actioned, closed
  impact_score int, -- Estimated impact (1-10)
  effort_score int, -- Estimated effort (1-10)
  created timestamp,
  updated timestamp,
  PRIMARY KEY (feedback_id)
);
CREATE INDEX qualitative_feedback_uid_idx ON qualitative_feedback (uid);
CREATE INDEX qualitative_feedback_category_idx ON qualitative_feedback (category);

-- PMF Score Tracking
create table pmf_scores (
  date date,
  segment text, -- User segment
  very_disappointed_pct double, -- % who would be very disappointed
  nps double, -- Net Promoter Score
  active_user_pct double, -- % of users active
  word_of_mouth_coefficient double, -- Virality measure
  activation_rate double, -- % of users who activate
  retention_rate double, -- % of users retained
  feature_completion_rate double, -- % of core features used
  session_frequency double, -- Sessions per week
  pmf_score double, -- Composite PMF score (0-100)
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((date), segment)
) WITH CLUSTERING ORDER BY (segment ASC);

insert into sequences (name, seq) values('PMF_VER',1);

-------------------END PRODUCT MARKET FIT FEATURES

-------------------START CREATOR ECONOMY FEATURES

-- Content Creators
create table content_creators (
  creator_id timeuuid,
  name text,
  username text,
  email text,
  bio text,
  website text,
  social_profiles map<text, text>, -- platform -> profile URL
  expertise set<text>, -- Areas of expertise
  content_types set<text>, -- Types of content they create
  topics set<text>, -- Topics they cover
  status text, -- active, pending, inactive, banned
  rating double, -- Internal quality rating (1-5)
  tier text, -- tier level (bronze, silver, gold, etc.)
  payment_method text, -- preferred payment method
  payment_details text, -- encrypted payment details
  tax_info text, -- tax identification information
  contract_terms text, -- specific terms for this creator
  contract_start timestamp,
  contract_end timestamp,
  created timestamp,
  updated timestamp,
  PRIMARY KEY (creator_id)
);
CREATE INDEX content_creators_username_idx ON content_creators (username);
CREATE INDEX content_creators_status_idx ON content_creators (status);

-- Creator Content
create table creator_content (
  content_id timeuuid,
  creator_id timeuuid, -- Link to creator
  thread_id timeuuid, -- Link to thread using this content
  title text,
  description text,
  content_type text, -- article, image, video, etc.
  content_url text, -- URL to the content
  license text, -- license type
  rights_usage text, -- usage rights
  exclusivity boolean, -- whether content is exclusive
  approval_status text, -- pending, approved, rejected
  approval_notes text,
  approver timeuuid, -- Who approved the content
  approved_date timestamp,
  publication_date timestamp,
  expiration_date timestamp,
  topics set<text>,
  tags set<text>,
  created timestamp,
  updated timestamp,
  PRIMARY KEY (content_id)
);
CREATE INDEX creator_content_creator_idx ON creator_content (creator_id);
CREATE INDEX creator_content_thread_idx ON creator_content (thread_id);

-- Content Performance
create table content_performance (
  content_id timeuuid,
  date date,
  impressions bigint,
  likes bigint, -- The key metric for payment
  shares bigint,
  comments bigint,
  clicks bigint,
  conversions bigint,
  time_spent bigint, -- Time spent in seconds
  avg_completion double, -- For video/articles
  performance_score double, -- Composite performance score
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((content_id), date)
) WITH CLUSTERING ORDER BY (date DESC);

-- Creator Earnings
create table creator_earnings (
  creator_id timeuuid,
  period_start date,
  period_end date,
  content_id timeuuid,
  impressions bigint,
  likes bigint,
  likes_rate double, -- $ per 1000 likes (CPM model)
  earnings double, -- Calculated earnings
  bonus double, -- Additional bonus if applicable
  total_earnings double, -- Total including bonus
  status text, -- calculated, approved, paid
  payment_id timeuuid, -- Link to payment record
  payment_date timestamp,
  notes text,
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((creator_id), period_start, period_end, content_id)
) WITH CLUSTERING ORDER BY (period_start DESC, period_end DESC, content_id ASC);

-- Creator Payments
create table creator_payments (
  payment_id timeuuid,
  creator_id timeuuid,
  amount double,
  currency text,
  payment_method text,
  transaction_id text, -- External payment reference
  status text, -- pending, processing, completed, failed
  period_start date,
  period_end date,
  payment_date timestamp,
  receipt_url text,
  notes text,
  created timestamp,
  updated timestamp,
  PRIMARY KEY (payment_id)
);
CREATE INDEX creator_payments_creator_idx ON creator_payments (creator_id);

-- Creator Analytics
create table creator_analytics (
  creator_id timeuuid,
  date date,
  total_content int, -- Total pieces of content
  active_content int, -- Currently active content
  total_impressions bigint,
  total_likes bigint,
  total_shares bigint,
  total_comments bigint,
  total_clicks bigint,
  total_conversions bigint,
  engagement_rate double, -- Overall engagement rate
  like_rate double, -- Likes per impression
  conversion_rate double, -- Conversions per impression
  avg_performance_score double,
  earnings double, -- Earnings for this date
  created timestamp,
  updated timestamp,
  PRIMARY KEY ((creator_id), date)
) WITH CLUSTERING ORDER BY (date DESC);

-- Payment Settings
create table payment_settings (
  setting_id text, -- e.g., "like_rate", "min_payout", etc.
  value double, -- Numerical value of setting
  description text,
  effective_date timestamp,
  end_date timestamp,
  created timestamp,
  updated timestamp,
  updater timeuuid,
  PRIMARY KEY (setting_id, effective_date)
) WITH CLUSTERING ORDER BY (effective_date DESC);

-- Content Attribution in Threads
alter table mthreads add creator_id timeuuid; -- Link to content creator
alter table mthreads add content_id timeuuid; -- Link to specific creator content
alter table mthreads add creator_compensation boolean; -- Whether creator is compensated
alter table mthreads add creator_compensation_rate double; -- $ per 1000 likes
alter table mthreads add creator_compensation_model text; -- likes, impressions, conversions
alter table mthreads add creator_compensation_cap double; -- Maximum payout
alter table mthreads add creator_notes text; -- Notes about the creator content

insert into sequences (name, seq) values('CREATOR_ECONOMY_VER',1);

-------------------END CREATOR ECONOMY FEATURES





