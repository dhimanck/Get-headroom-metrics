###############################################################################
# This example will grab the specified Qos_Policy and find all of the volumes
# in the policy and then dump out the latest stats:
#
#   ops, latency, qos limit delay
#
# for each volume in the QosPolicy
#
# The stats possible for the volume :
#
#  sample_qos_volume_workload_<clusterId>:
#      `latency` float DEFAULT NULL,
#      `ops` float DEFAULT NULL,
#      `totalData` float DEFAULT NULL,
#      `readData` float DEFAULT NULL,
#      `readLatency` float DEFAULT NULL,
#      `readOps` float DEFAULT NULL,
#      `writeData` float DEFAULT NULL,
#      `writeLatency` float DEFAULT NULL,
#      `writeOps` float DEFAULT NULL,
#      `cacheMissRate` float DEFAULT NULL,
#      `cacheMissRateBase` bigint(20) DEFAULT NULL,
#      `logicalSize` bigint(20) DEFAULT NULL,
#      `opsPerTB` float DEFAULT NULL,
#      `maxIOPS` float DEFAULT NULL,
#      `maxMBPS` float DEFAULT NULL,
#      `minIOPS` float DEFAULT NULL,
#      `maxIOPSPerTB` float DEFAULT NULL,
#      `minIOPSPerTB` float DEFAULT NULL,
#
#  sample_qos_workload_queue_nblade_<clusterId>:
#      `readOps` float DEFAULT NULL,
#      `writeOps` float DEFAULT NULL,
#      `otherOps` float DEFAULT NULL,
#      `readLatency` float DEFAULT NULL,
#      `writeLatency` float DEFAULT NULL,
#      `otherLatency` float DEFAULT NULL,
#      `readData` float DEFAULT NULL,
#      `writeData` float DEFAULT NULL,
#      `cpuNBladeResidenceTime` bigint(20) DEFAULT NULL,
#      `cpuNBladeUtilServiceTime` bigint(20) DEFAULT NULL,
#      `delayClusterInterconnectWaitTime` bigint(20) DEFAULT NULL,
#      `delayQosLimitWaitTime` bigint(20) DEFAULT NULL,
#      `delayNetworkWaitTime` bigint(20) DEFAULT NULL,
#      `collectionPeriod` bigint(20) DEFAULT NULL,
#
###############################################################################

# These come standard with a python install
import sys, string, os, time
from datetime import datetime, timedelta

# This module can be pulled down from the mysql page
# https://dev.mysql.com/doc/connector-python/en/connector-python-installation-binary.html
import mysql.connector

# Columns to output to the file
COLUMNS = "objid, name, clusterid, time, ops, workload latency (ms), latency at qos delay center (ms/op)"

#
# Server Parameters
#
SERVER   = "ocum-main-int.gdl.englab.netapp.com" 
USER     = "root" 
PASSWORD = "*******" 

#
# Connect to the server
#
dbConn = mysql.connector.connect( host=SERVER,
                                  user=USER, 
                                  passwd=PASSWORD,
                                  db="")
dbWorker = dbConn.cursor()

# Change this to the Qos Policy of your choice
QOS_POLICY = "\'auto_qos_vol_all\'"

#
# If you want to write the data to a file
#
outfile = open("qospolicy_volumestats.csv", "w")
outfile.write(COLUMNS + "\n")

#
# We want to grab the latest stat 10 minutes back.
#
tenMinsBack = datetime.today() - timedelta(hours=0, minutes=10)
# Convert to milliseconds which is what the DB has
tenMinsBackMs = time.mktime(tenMinsBack.timetuple()) * 1000

#
# Need to find the DB id of the policy group first.
#
cmd = "SELECT objid " \
         "FROM netapp_model_view.qos_policy_group " \
	 " WHERE policyGroup = {}".format(QOS_POLICY)
dbWorker.execute(cmd)
policyGroupList = dbWorker.fetchall()
if ( len(policyGroupList) == 0 ):
    print "No QoS Policy Group with that name in the system"
    sys.exit(0)

policyGroupId = policyGroupList[0][0]

#
# Now grab all the volumes in the policy group
#
cmd = "SELECT objid, name, clusterId " \
          "FROM netapp_model_view.volume " \
	   "WHERE qosPolicyGroupId = {}".format(policyGroupId)
dbWorker.execute(cmd)
volumes = dbWorker.fetchall()
print "Total Volumes in QoS Policy Group = " + str(len(volumes))
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
    cmd = "SELECT time, ops, latency " \
             "FROM netapp_performance.sample_qos_volume_workload_{} " \
	     "WHERE objid={} " \
	       "AND time >= {} limit 1".format(clusterId, wobjid, int(tenMinsBackMs))
    dbWorker.execute(cmd)
    wstats = dbWorker.fetchall()
    
    # Make sure there are actual stats
    if ( len(wstats) == 0 ): 
	print "No stats, skipping"
        continue
    etime, ops, latency = wstats[0]
    print datetime.fromtimestamp(etime/1000)

    #############################################################################
    # Now lets get the Qos limit delay center for this volume.  We need to get all the nodes 
    # that this volume is being accessed from.  Once we have that, we can get the nblade stats
    # from the sample_qos_workload_queue_nblade_<clusterId> table.
    cmd = "SELECT objid " \
             "FROM netapp_model_view.qos_workload_node_relationship " \
	     "WHERE workloadId = {}".format(wobjid)
    dbWorker.execute(cmd)
    nodeRelationships = dbWorker.fetchall()

    nbladeStats = []
    for nodeRelationship in nodeRelationships:
	objid = nodeRelationship[0]
	print objid
        cmd = "SELECT time, delayQosLimitWaitTime " \
                 "FROM netapp_performance.sample_qos_workload_queue_nblade_{} " \
	         "WHERE objid={} " \
	           "AND time >= {} limit 1".format(clusterId, objid, int(tenMinsBackMs))
        dbWorker.execute(cmd)
        qosstats = dbWorker.fetchall()
	# Might not be any stats if no delay
	if ( len(qosstats) == 0 ):
            continue
	print qosstats
	nbladeStats.append(qosstats[0])

    # Check for no stats
    time, qosLimitLatency = (0,0)
    if ( len(nbladeStats) != 0 ):
	qtime, qosLimitLatency = nbladeStats[0]
        print datetime.fromtimestamp(qtime/1000)

    # The Qos Limit is a waitime per op in microseconds.  So we need to divide it by ops to 
    # get the real latency value and then by 1000 to convert to milliseconds
    qosLimit = (qosLimitLatency/ops)/1000
    date = datetime.fromtimestamp(etime/1000)

    # Now write the data to the file
    outfile.write("{}, {}, {}, {}, {}, {}, {}\n".format(objid, name, clusterId, date, ops, latency, qosLimit))
    
# Now close the file
outfile.close()

