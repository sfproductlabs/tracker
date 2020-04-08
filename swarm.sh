
#!/bin/bash
#jq manual https://stedolan.github.io/jq/manual/
dig +short tasks.nats_nats. | grep -v "8.8.8.8" | awk '{print "\\\""$1"\\\""}' | paste -s -d, - | awk '{system("cat config.json | jq \"(.Notify[1].Hosts) |= ["$1"]\"")}' > /app/tracker/temp.config.json
dig +short tasks.nats_nats. | grep -v "8.8.8.8" | awk '{print "\\\""$1"\\\""}' | paste -s -d, - | awk '{system("cat /app/tracker/temp.config.json | jq \"(.Consume[1].Hosts) |= ["$1"]\"")}' > /app/tracker/temp2.config.json
mv /app/tracker/temp2.config.json /app/tracker/temp.config.json
sed -i 's;./.setup/keys;/app/tracker/.setup/keys;g' /app/tracker/temp.config.json