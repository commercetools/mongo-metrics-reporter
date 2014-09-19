## mongodb-graphite-stats

Sends mongodb `db.serverStatus()` to the graphite.

### Run

Application is distributed as a docker container, which can be found here:

https://registry.hub.docker.com/u/tenshi/mongodb-graphite/

So you just need to pull it:

    docker pull tenshi/mongodb-graphite
     
and then run it:

    docker run -d tenshi/mongodb-graphite
    
### Configure

The docker container accept following environment variables:

* **GRAPHITE_HOST** - default "localhost"
* **GRAPHITE_PORT** - default 2003
* **GRAPHITE_PREFIX** - default "mongo_lock_stats"
* **MONGO_HOST** - default "localhost"
* **MONGO_PORT** - default 27017
* **REPORT_INTERVAL** - the graphite reporting interval. You can write human-readable interval representation like "1 minute" or "23 seconds". By default "1 minute"

So if your carbon-cache is running on 192.168.10.105:8111, mongo is on host 192.168.10.105 and you would like to report status every 2 seconds, then you can use this config:

    docker run -d \
      -e MONGO_HOST=192.168.10.105 \
      -e GRAPHITE_PORT=8111 \
      -e GRAPHITE_HOST=192.168.10.105 \
      -e "REPORT_INTERVAL=2 seconds" \
      tenshi/mongodb-graphite