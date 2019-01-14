###############################################################################
# This example will grab all of the volumes in the system and grab the ops
# and time for each and write them to a "volumestats.csv" file.  Here are all
# of the stats possible for the volume workload:
#
#  `latency` float DEFAULT NULL,
#  `ops` float DEFAULT NULL,
#  `totalData` float DEFAULT NULL,
#  `readData` float DEFAULT NULL,
#  `readLatency` float DEFAULT NULL,
#  `readOps` float DEFAULT NULL,
#  `writeData` float DEFAULT NULL,
#  `writeLatency` float DEFAULT NULL,
#  `writeOps` float DEFAULT NULL,
#  `cacheMissRate` float DEFAULT NULL,
#  `cacheMissRateBase` bigint(20) DEFAULT NULL,
#  `logicalSize` bigint(20) DEFAULT NULL,
#  `opsPerTB` float DEFAULT NULL,
#  `maxIOPS` float DEFAULT NULL,
#  `maxMBPS` float DEFAULT NULL,
#  `minIOPS` float DEFAULT NULL,
#  `maxIOPSPerTB` float DEFAULT NULL,
#  `minIOPSPerTB` float DEFAULT NULL,
#
###############################################################################

# These come standard with a python install
import sys, string, os, time
from datetime import datetime, timedelta

# This module can be pulled down from the mysql page
# https://dev.mysql.com/doc/connector-python/en/connector-python-installation-binary.html
import mysql.connector

# Columns to output to the file
COLUMNS = "objid, name, clusterid, time, ops"

#
# Server Parameters
#
SERVER   = "ocum-main-int.gdl.englab.netapp.com" 
USER     = "root" 
PASSWORD = "******" 

#
# Connect to the server
#
dbConn = mysql.connector.connect( host=SERVER,
                                  user=USER, 
                                  passwd=PASSWORD,
                                  db="")
dbWorker = dbConn.cursor()


#
# If you want to write the data to a file
#
outfile = open("volumestats.csv", "w")
outfile.write(COLUMNS + "\n")

#
# We want to grab the latest stat 10 minutes back.
#
tenMinsBack = datetime.today() - timedelta(hours=0, minutes=10)
# Convert to milliseconds which is what the DB has
tenMinsBack = time.mktime(tenMinsBack.timetuple()) * 1000

#
# Now grab all the volumes in the DB no matter the cluster
#
cmd = "SELECT objid, name, clusterId " \
          "FROM netapp_model_view.volume"
dbWorker.execute(cmd)
volumes = dbWorker.fetchall()
print "Total Volumes = " + str(len(volumes))
for volume in volumes:
    objid, name, clusterId = volume
    print "Working on volume: " + name
    # Need to map the volume to its workload id to get it's stats 
    cmd = "SELECT objid from netapp_model.qos_workload " \
             "WHERE holderId={}".format(objid)
    dbWorker.execute(cmd)
    workload = dbWorker.fetchall()

    # Sometimes a volume might not have a workload
    if ( len(workload) == 0 ): continue
    
    wobjid = workload[0][0]
    # Now get the stats for this workload
    cmd = "SELECT time, ops " \
             "FROM netapp_performance.sample_qos_volume_workload_{} " \
	     "WHERE objid={} " \
	       "AND time >= {} limit 1".format(clusterId, wobjid, int(tenMinsBack))
    dbWorker.execute(cmd)
    wstats = dbWorker.fetchall()
    
    # Make sure there are actual stats
    if ( len(wstats) == 0 ): 
	print "No stats"
        continue
    wstatsItem = wstats[0]
    etime, ops = wstatsItem

    # Now write the data to the file
    outfile.write("{}, {}, {}, {}, {}\n".format(objid, name, clusterId, int(etime), ops))
    
# Now close the file
outfile.close()

