
#!/bin/bash
dig +short tasks.nats_nats. | grep -v "8.8.8.8" | awk '{system("echo nats-route://"$1":6222")}' | xargs | sed -e 's/ /,/g' | sed "s;/;\\\\/;g" | sed -r "s/nats-route:\/\/nats-seed1:6222/$(awk '{print $1}')/g" /app/nats/seed.conf > /app/nats/temp.conf