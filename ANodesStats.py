###############################################################################
# This example will grab a node in the system by a specified name and grab the
# totalOps, headroom and available ops and time and output to the screen.
#
# Here are all of the stats possible for the node:
#
# sample_node :
#  `cpuBusy` float DEFAULT NULL,
#  `avgProcessorBusy` float DEFAULT NULL,
#  `cpuElapsedTime` bigint(20) unsigned DEFAULT NULL,
#  `netDataRecv` float DEFAULT NULL,
#  `netDataSent` float DEFAULT NULL,
#  `readOps` float DEFAULT NULL,
#  `writeOps` float DEFAULT NULL,
#  `totalOps` float DEFAULT NULL,
#  `otherOps` float DEFAULT NULL,
#  `cifsOps` float DEFAULT NULL,
#  `fcpOps` float DEFAULT NULL,
#  `iscsiOps` float DEFAULT NULL,
#  `nfsOps` float DEFAULT NULL,
#  `readLatency` float DEFAULT NULL,
#  `writeLatency` float DEFAULT NULL,
#  `avgLatency` float DEFAULT NULL,
#  `fcpDataRecv` float DEFAULT NULL,
#  `fcpDataSent` float DEFAULT NULL,
#  `opmSysReadThroughput` float DEFAULT NULL,
#  `opmSysWriteThroughput` float DEFAULT NULL,
#  `opmSysThroughput` float DEFAULT NULL,
#  `opmSysUtilization` float DEFAULT NULL,
#  `totalCpMSecs` bigint(20) unsigned DEFAULT NULL,
#  `p2Flush` bigint(20) unsigned DEFAULT NULL,
#  `totalKahunaBusy` bigint(20) unsigned DEFAULT NULL,
#  `maxProcessorElapsedTime` bigint(20) unsigned DEFAULT NULL,
#  `deltaPollTimeInMSecs` bigint(20) unsigned DEFAULT NULL,
#  `maxAggregateUtilization` float DEFAULT NULL,
#  `percentCleanTimeUtil` float DEFAULT NULL,
#  `diskShelfFibreChannelLoopUtilization` float DEFAULT NULL,
#  `nvmfOps` float DEFAULT NULL,
#  `nvmfDataRecv` float DEFAULT NULL,
#  `nvmfDataSent` float DEFAULT NULL,
#
# sample_opm_headroom_cpuCREATE TABLE `sample_opm_headroom_cpu` (
#  `objid` bigint(20) NOT NULL,
#  `time` bigint(20) NOT NULL,
#  `empty` tinyint(1) DEFAULT NULL,
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
#  `haLatencyProjection` float DEFAULT NULL,
#  `haHRoomUsedProjection` float DEFAULT NULL,
#  `haUtilProjection` float DEFAULT NULL,
#  `haOpsProjection` float DEFAULT NULL,
#  `isObservationBased` tinyint(1) DEFAULT NULL,
#  `haHRoomUsedNoInternalProjection` float DEFAULT NULL,
#  `availableOps` float DEFAULT NULL,
#
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

NODE_NAME = "\'ocum-mobility-01\'"

#
# Now grab the node in the DB with the specific name
#
cmd = "SELECT objid, name, clusterId " \
         "FROM netapp_model_view.node " \
         "WHERE name = {}".format(NODE_NAME)
dbWorker.execute(cmd)
nodeFound = dbWorker.fetchall()
if ( len(nodeFound) == 0 ):
    print "No node found with name = " + NODE_NAME
    sys.exit(0)
if ( len(nodeFound) > 1 ):
    print "More than one node found with name = " + NODE_NAME
    sys.exit(0)

objid, name, clusterId = nodeFound[0]
print "Working on node: " + name
    
# Now get the stats for this node
cmd = "SELECT time, totalOps " \
         "FROM netapp_performance.sample_node " \
	 "WHERE objid={} AND time >= {} limit 1".format(objid, int(tenMinsBackMs))
dbWorker.execute(cmd)
nstats = dbWorker.fetchall()
    
# Make sure there are actual stats
if ( len(nstats) == 0 ): 
    print "No stats"
    sys.exit(0)

etime, ops = nstats[0]

# Now get the calculated stats for this node
cmd = "SELECT time, cHRoomUsedPercent, availableOps " \
         "FROM netapp_performance.sample_opm_headroom_cpu " \
	 "WHERE objid={} AND time >= {} limit 1".format(objid, int(tenMinsBackMs))
dbWorker.execute(cmd)
hstats = dbWorker.fetchall()
    
# Make sure there are actual stats
if ( len(hstats) == 0 ): 
    print "No headroom stats"
    sys.exit(0)

htime, headroom, availableOps = hstats[0]
date = datetime.fromtimestamp(htime/1000)

# Now write the data to the file
print "objId: %d, name: %s, clusterId: %d, date: %s, ops: %f, usedheadroom %f, availableOps: %f" % (objid, name, clusterId, date, ops, headroom, availableOps)
    
