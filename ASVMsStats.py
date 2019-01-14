###############################################################################
# This example will grab a svm in the system by a specified name and grab the
# total_ops and time and output to the screen.
#
# There are a few tables that the SVM draws its information from:
#
#   sample_cifsvserver
#      `cifsLatencyBase` bigint(20) unsigned DEFAULT NULL,
#      `cifsLatency` float DEFAULT NULL,
#      `cifsOps` float DEFAULT NULL,
#      `cifsReadLatency` float DEFAULT NULL,
#      `cifsReadOps` float DEFAULT NULL,
#      `cifsWriteLatency` float DEFAULT NULL,
#      `cifsWriteOps` float DEFAULT NULL,
#      `cifsReadThroughput` float DEFAULT NULL,
#      `cifsWriteThroughput` float DEFAULT NULL,
#      `opmCifsThroughput` float DEFAULT NULL,
#
#   sample_vserver
#      `opmOps` float DEFAULT NULL,
#      `opmReadOps` float DEFAULT NULL,
#      `opmWriteOps` float DEFAULT NULL,
#      `opmThroughput` float DEFAULT NULL,
#      `opmReadThroughput` float DEFAULT NULL,
#      `opmWriteThroughput` float DEFAULT NULL,
#      `opmLatency` float DEFAULT NULL,
#      `opmReadLatency` float DEFAULT NULL,
#      `opmWriteLatency` float DEFAULT NULL,
#
#   sample_iscsilifvserver
#      `dataInBlocks` bigint(20) unsigned DEFAULT NULL,
#      `dataOutBlocks` bigint(20) unsigned DEFAULT NULL,
#      `iscsiReadOps` float DEFAULT NULL,
#      `iscsiWriteOps` float DEFAULT NULL,
#      `protocolErrors` float DEFAULT NULL,
#      `readData` float DEFAULT NULL,
#      `writeData` float DEFAULT NULL,
#      `cmdTransfered` bigint(20) unsigned DEFAULT NULL,
#      `avgLatency` float DEFAULT NULL,
#      `avgOtherLatency` float DEFAULT NULL,
#      `avgReadLatency` float DEFAULT NULL,
#      `avgWriteLatency` float DEFAULT NULL,
#      `iscsiOtherOps` float DEFAULT NULL,
#      `opmIscsiLifOps` float DEFAULT NULL,
#      `opmIscsiLifThroughput` float DEFAULT NULL,
#
#   sample_networklifvserver
#      `recvData` float DEFAULT NULL,
#      `recvErrors` float DEFAULT NULL,
#      `recvPacket` float DEFAULT NULL,
#      `sentData` float DEFAULT NULL,
#      `sentErrors` float DEFAULT NULL,
#      `sentPacket` float DEFAULT NULL,
#      `opmLifPackets` float DEFAULT NULL,
#      `opmLifThroughput` float DEFAULT NULL,
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
# We want to grab the latest stat 10 minutes back.
#
tenMinsBack = datetime.today() - timedelta(hours=0, minutes=10)
# Convert to milliseconds which is what the DB has
tenMinsBackMs = time.mktime(tenMinsBack.timetuple()) * 1000

SVM_NAME = "\'auto_svm_all\'"

#
# Now grab the vserver in the DB with the specific name
#
cmd = "SELECT objid, name, clusterId " \
         "FROM netapp_model_view.vserver " \
         "WHERE name = {}".format(SVM_NAME)
dbWorker.execute(cmd)
vserverFound = dbWorker.fetchall()
if ( len(vserverFound) == 0 ):
    print "No SVM found with name = " + SVM_NAME
    sys.exit(0)
if ( len(vserverFound) > 1 ):
    print "More than one SVM found with name = " + SVM_NAME
    sys.exit(0)

objid, name, clusterId = vserverFound[0]
print "Working on SVM: " + name
    
# Now get the stats for this vserver
cmd = "SELECT time, opmOps " \
         "FROM netapp_performance.sample_vserver " \
	 "WHERE objid={} AND time >= {} limit 1".format(objid, int(tenMinsBackMs))
dbWorker.execute(cmd)
sstats = dbWorker.fetchall()
    
# Make sure there are actual stats
if ( len(sstats) == 0 ): 
    print "No stats"
    sys.exit(0)

etime, ops = sstats[0]

# Now write the data to the file
print "%d, %s, %d, %d, %f" % (objid, name, clusterId, etime, ops)
    
