## mongo-metrics-reporter

[![Join the chat at https://gitter.im/commercetools/mongo-metrics-reporter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/commercetools/mongo-metrics-reporter?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Sends mongodb `db.serverStatus()` to the Graphite and InfluxDb.

### Run

Application is distributed as a docker container, which can be found here:

https://registry.hub.docker.com/u/tenshi/mongo-metrics-reporter/

So you just need to pull it:

    docker pull tenshi/mongo-metrics-reporter
     
and then run it:

    docker run -d tenshi/mongo-metrics-reporter
    
### Configure

The docker container accept following environment variables:

* **REPORTER** - either "influxDb" or "graphite" (by default "influxDb")
* **INFLUX_DB_URL** - default "http://localhost:8086"
* **INFLUX_DB_USERNAME** - default "root"
* **INFLUX_DB_PASSWORD** - default "root"
* **INFLUX_DB_DATABASE_NAME** - default "test"
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
      tenshi/mongo-metrics-reporter