use sfpla;

insert into sequences (name, seq) values('DB_VER', 2);

create table redirects (
  urlfrom text,
  hostfrom text,
  slugfrom text,
  urlto text,
  hostto text,
  pathto text,
  searchto text,
  updated timestamp,  
  updater text,
  primary key((hostfrom),updated)
)
WITH CLUSTERING ORDER BY (updated DESC);
CREATE INDEX hostto_redirects_idx ON redirects ( hostto );


create type cred (
  host text,
  claim set<text>
);
create table accounts (
  uid uuid, 
  key text,
  ip inet,
  msg text,
  expires timestamp,
  creds map<text,frozen<map<text,text>>>, --host, claim[yes]
  created timestamp,
  owner uuid,
  primary key((uid))
);

--insert into accounts (uid,key,msg,expires,creds,created,owner) values (00000000-0000-0000-0000-000000000000,'adc83b19e793491b1c6ea0fd8b46cd9f32e592fc', 'demo admin user',toTimestamp('2999-01-01'),{'*':{'*':'*'}}, toTimestamp(now()),00000000-0000-0000-0000-000000000000);