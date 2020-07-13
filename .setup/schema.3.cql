-- To run:
-- cqlsh --ssl -f schema.1.cql 
-- Licensed under AGPL v3.  Copyright (c) 2018 SF Product Labs. All Rights Reserved.
-- See LICENSE

-- SFPLA

use sfpla;

insert into sequences (name, seq) values('DB_VER',3);

create type geo_pol (
 country text, --iso2 ex. US, DE, AU
 region text, --State ex. CA, BW
 city text --Ex. Brisbane
);

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

create table agreements (
  vid timeuuid, --visitor-id
  created timestamp,  
  compliances  map<text,frozen<set<text>>>,
  sid timeuuid, --session-id
  uid timeuuid, --user-id
  avid timeuuid, --anonymized vid
  hhash text, --host-hash
  app text, --app
  rel text, --app release/version
  url text, --this should always be url slug of **current** url
  ip inet, --client ip
  iphash text, 
  loc geo_pol,
  owner timeuuid,
  org timeuuid,
  PRIMARY KEY ((vid), created)
)
WITH CLUSTERING ORDER BY (created DESC);
