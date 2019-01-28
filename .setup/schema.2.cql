use sfpla;

insert into sequences (name, seq) values('DB_VER', 2);

create table redirects (
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
--https://localhost:8443/rpi/v1/redirects/00000000-0000-0000-0000-000000000000/password/nytimes.com
--insert into accounts (uid,pwd,msg,expires,creds,created,owner) values (00000000-0000-0000-0000-000000000000,'W6ph5Mm5Pz8GgiULbPgzG37mj9g=', 'demo admin user',toTimestamp('2999-01-01'),{'*':{'*':'*'}}, toTimestamp(now()),00000000-0000-0000-0000-000000000000);
--insert into redirects (urlfrom,urlto) values ('localhost:8443/tv','https://google.com');