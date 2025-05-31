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





