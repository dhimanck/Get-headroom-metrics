###############################################################################
# This example will grab an Aggregate in the system by a specified name and grab the
# headroom data.
#
# Here are all of the stats possible for the aggregate:
#
# sample_opm_headroom_aggr_<clusterId>
#  `serviceTime` float DEFAULT NULL,
#  `obOptimalPointUtil` float DEFAULT NULL,
#  `obOptimalPointLatency` float DEFAULT NULL,
#  `obOptimalPointOps` float DEFAULT NULL,
#  `obcOptimalPointUtil` float DEFAULT NULL,
#  `obcOptimalPointLatency` float DEFAULT NULL,
#  `obcOptimalPointOps` float DEFAULT NULL,
#  `obConfidenceLevel` float DEFAULT NULL,
#  `mOptimalPointUtil` float DEFAULT NULL,
#  `mOptimalPointLatency` float DEFAULT NULL,
#  `mOptimalPointOps` float DEFAULT NULL,
#  `mcOptimalPointUtil` float DEFAULT NULL,
#  `mcOptimalPointLatency` float DEFAULT NULL,
#  `mcOptimalPointOps` float DEFAULT NULL,
#  `mConfidenceLevel` float DEFAULT NULL,
#  `cOperationalPointAllWorkloads` float DEFAULT NULL,
#  `cOperationalPointInternalWorkloads` float DEFAULT NULL,
#  `oOperationalPointAllWorkloads` float DEFAULT NULL,
#  `oOperationalPointInternalWorkloads` float DEFAULT NULL,
#  `cHRoomUsedPercent` float DEFAULT NULL,
#  `cHRoomUsedNoInternalPercent` float DEFAULT NULL,
#  `isObservationBased` tinyint(1) DEFAULT NULL,
#  `availableOps` float DEFAULT NULL,
#
#
# sample_opm_headroom_aggr_<clusterId>
#   `objid` bigint(20) NOT NULL,
#   `time` bigint(20) NOT NULL,
#   `empty` tinyint(1) DEFAULT NULL,
#   `serviceTime` float DEFAULT NULL,
#   `obOptimalPointUtil` float DEFAULT NULL,
#   `obOptimalPointLatency` float DEFAULT NULL,
#   `obOptimalPointOps` float DEFAULT NULL,
#   `obcOptimalPointUtil` float DEFAULT NULL,
#   `obcOptimalPointLatency` float DEFAULT NULL,
#   `obcOptimalPointOps` float DEFAULT NULL,
#   `obConfidenceLevel` float DEFAULT NULL,
#   `mOptimalPointUtil` float DEFAULT NULL,
#   `mOptimalPointLatency` float DEFAULT NULL,
#   `mOptimalPointOps` float DEFAULT NULL,
#   `mcOptimalPointUtil` float DEFAULT NULL,
#   `mcOptimalPointLatency` float DEFAULT NULL,
#   `mcOptimalPointOps` float DEFAULT NULL,
#   `mConfidenceLevel` float DEFAULT NULL,
#   `cOperationalPointAllWorkloads` float DEFAULT NULL,
#   `cOperationalPointInternalWorkloads` float DEFAULT NULL,
#   `oOperationalPointAllWorkloads` float DEFAULT NULL,
#   `oOperationalPointInternalWorkloads` float DEFAULT NULL,
#   `cHRoomUsedPercent` float DEFAULT NULL,
#   `cHRoomUsedNoInternalPercent` float DEFAULT NULL,
#   `isObservationBased` tinyint(1) DEFAULT NULL,
#   `availableOps` float DEFAULT NULL,
#
###############################################################################

# These come standard with a python install
import sys, string, os, time
from datetime import datetime, timedelta

# This module can be pulled down from the mysql page
# https://dev.mysql.com/doc/connector-python/en/connector-python-installation-binary.html
import mysql.connector

#
# Server Parameters
#
SERVER   = "ocum-main-int.gdl.englab.netapp.com" 
USER     = "root" 
PASSWORD = "*****" 

#
# Connect to the server
#
dbConn = mysql.connector.connect( host=SERVER,
                                  user=USER, 
                                  passwd=PASSWORD,
                                  db="")
dbWorker = dbConn.cursor()

#
# We want to grab the latest stat 10 minutes back.
#
tenMinsBack = datetime.today() - timedelta(hours=0, minutes=10)
# Convert to milliseconds which is what the DB has
tenMinsBackMs = time.mktime(tenMinsBack.timetuple()) * 1000

AGGR_NAME = "\'aggr_automation\'"

#
# Now grab the aggregate in the DB with the specific name
#
cmd = "SELECT objid, name, clusterId " \
         "FROM netapp_model_view.aggregate " \
         "WHERE name = {}".format(AGGR_NAME)
dbWorker.execute(cmd)
aggrFound = dbWorker.fetchall()
if ( len(aggrFound) == 0 ):
    print "No aggregate found with name = " + AGGR_NAME
    sys.exit(0)
if ( len(aggrFound) > 1 ):
    print "More than one aggregate found with name = " + AGGR_NAME
    sys.exit(0)

objid, name, clusterId = aggrFound[0]
print "Working on aggr: " + name
    
# Now get the stats for this aggr
cmd = "SELECT time, cHRoomUsedPercent, availableOps " \
         "FROM netapp_performance.sample_opm_headroom_aggr_{} " \
	 "WHERE objid={} AND time >= {} limit 1".format(clusterId, objid, int(tenMinsBackMs))
dbWorker.execute(cmd)
astats = dbWorker.fetchall()
    
# Make sure there are actual stats
if ( len(astats) == 0 ): 
    print "No stats"
    sys.exit(0)

atime, cHRoomUsedPercent, availableOps = astats[0]
date = datetime.fromtimestamp(atime/1000)

# Now write the data to the screen
print "objId: %d, name: %s, clusterId: %d, date: %s, usedHeadRoom: %f, availableOps: %f" % (objid, name, clusterId, date, cHRoomUsedPercent, availableOps)
    
