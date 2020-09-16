#!/bin/bash

# setup
# 

#curl -w "\n" -k -XDELETE "http://$CASSANDRA_HOST:9200/sfplax"
#curl -XPUT -H 'Content-Type: application/json' http://$CASSANDRA_HOST:9200/sfplax -d'{}'

if [[ -z "${CASSANDRA_HOST}" ]]; then
  CASSANDRA_HOST="localhost"
  else 
  CASSANDRA_HOST="cassandra1" #Paste this in console, this is never hit but you should pass it to the script
fi

curl -w "\n" -k -XDELETE "http://$CASSANDRA_HOST:9200/events_recent"

# events_recent
curl -w "\n" -k -H 'Content-Type: application/json'  -XPUT  "http://$CASSANDRA_HOST:9200/events_recent/" -d '{
    "settings" : { "keyspace" : "sfpla" },
    "mappings": {
        "events_recent": {
            "properties" : {
                "eid": { "type": "keyword", "index": true, "cql_collection": "singleton" },
                "vid": { "type": "keyword", "index": true, "cql_collection": "singleton" },
                 "latlon" : { "type": "geo_point", "index": true, "cql_collection": "singleton" },
                "params" :  { 
                  "type": "nested", 
                  "include_in_parent": true,
                  "cql_struct" : "opaque_map",
                  "cql_collection" : "singleton"
                }
            }
        }
    }
}'


nodetool flush sfpla
nodetool rebuild_index sfpla events_recent events_recent_idx

# OPTIONAL MERGE INDEXES
# curl -w "\n" -k -H 'Content-Type: application/json'  -XPOST  "http://$CASSANDRA_HOST:9200/_reindex" -d '{
#   "source": {
#     "index": ["mtriage", "mfailures"]
#   },
#   "dest": {
#     "index": "sfplx"
#   }
# }'

#########################
#### TESTS


curl -XGET -H 'Content-Type: application/json' "http://$CASSANDRA_HOST:9200/events_recent/_search?pretty" -d '
{
  "_source": {
        "excludes": [ "eid" ]
  },
  "query" : {
        "terms" : {
          "eid" : ["c4156dbc-e5a2-11ea-964c-3af9d39c7fb0"]
        }
  }
}'


curl -XGET -H -k http://$CASSANDRA_HOST:9200/events_recent/_search?pretty=true&q=rock

curl -XGET -H -k http://$CASSANDRA_HOST:9200/sfplax/_search?pretty=true&q=*:*

curl -XGET http://$CASSANDRA_HOST:9200/_cluster/state?pretty


curl -w "\n" -k -H 'Content-Type: application/json'  -XPOST  "http://$CASSANDRA_HOST:9200/_opendistro/_sql/?format=csv" -d '{
  "query": "SELECT * from events_recent limit 1"
}'