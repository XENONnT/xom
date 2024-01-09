XOM - Xenon Offline Monitoring
=

* Free software: BSD license
* Documentation: Available on internal XENON wiki

Features
--------

* Analyses XENONnT data and monitors useful quantities through a web application
* The web app is the xenon grafana

Usage
--------
* choose the analysis to be computed in the /utils/xomconfig.cfg and configure them 

* source setup_xom.sh:
       loads singularity 
       sets the environment (development)

* python start_xom.py:
  --prefix: for test purpose use the prefix you want, the official xom is the prod_ prefix. Sets a prefix to the measurement names in the data base.
  --clean: will cleanup the database with the given prefix and the job files



Todo list:
------------------------ 
cron tab instead of a while true
job out files handling
in dblib: declare once the query_api etc
handle the import of xomlib 
describe well the running procedure

write check functions
The way to delete the records is now by deleting +- 1us of the time of the record. Maybe cleaner way exist

make the test mode more uniform
