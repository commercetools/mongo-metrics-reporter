## v2.3 (2016-04-18)

* Added support for multiple mongo hosts in influxDB reporter. Metrics would be reported for every provided host independently. 
  This change also replaces `databaseId` with the host name (you can include/exclude port in the config)  
* Added support for mongo host auto-discovery. 
  
## v2.2 (2016-01-12)

* Updated to official influxdb-java v2.1 driver
* InfluxDB reporter now supports Mongo 3.0 metrics
  * This includes wiredTiger engine metrics

## v2.1 (2015-10-29)

* Updated to official influxdb-java v2.0 driver
* Do not override the retention policy

## v2.0 (2015-09-22)

* InfluxDB support
* Improved and more generic configuration