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
 county text, --Ex city San Francisco, County San Francisco (legislative sub-region)
 city text --Ex. Brisbane
);

create table countries (
  country text,
  name text,
  continent text,
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
 region text, 
 city text,
 lat double,
 lon double,
 tz text,
 zip text,
 PRIMARY KEY ((ipc),ips,ipe)
);
--CREATE INDEX ips_geo_ip_idx ON geo_ip ( ips );
--CREATE INDEX ipe_geo_ip_idx ON geo_ip ( ipe );

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
  compliance_flags bigint, --compliances represented via N notation == 1001110101111
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
  PRIMARY KEY ((vid))
);


create table agreed (
  vid timeuuid, 
  created timestamp,  
  compliances map<text,frozen<set<text>>>,
  compliance_flags bigint,
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
  PRIMARY KEY ((vid), created)
)
WITH CLUSTERING ORDER BY (created DESC);