#!/usr/bin/env python3

import datetime
import sys
import os
import re
import json
import shutil
from gppylib.commands.gp import SegmentRewind, SegmentStart
from gppylib.db import dbconn
from gppylib.commands import unix
from gppylib.commands.pg import DbStatus
from gppylib import gplog
from gppylib.commands.base import *
from gppylib.gparray import Segment
from time import sleep
from gppylib.operations.buildMirrorSegments import gDatabaseDirectories, gDatabaseFiles

class PgConfigureRewind(Command):
    def __init__(self, name, cmdstr, logger, rewindSeg, shouldDeleteProgressFile, progressMode):
        self.logger = logger
        self.rewindSeg = rewindSeg
        # self.shouldDeleteProgressFile = shouldDeleteProgressFile
        self.progressMode = progressMode

        Command.__init__(self, name, cmdstr)

    def run(self):
        self.logger.debug('Do CHECKPOINT on %s (port: %d) before running pg_rewind.' % (self.rewindSeg.sourceHostname, self.rewindSeg.sourcePort))
        dburl = dbconn.DbURL(hostname=self.rewindSeg.sourceHostname,
                             port=self.rewindSeg.sourcePort,
                             dbname='template1')
        conn = dbconn.connect(dburl, utility=True)
        dbconn.execSQL(conn, "CHECKPOINT")
        conn.commit()
        conn.close()

        # If the postmaster.pid still exists and another process
        # is actively using that pid, pg_rewind will fail when it
        # tries to start the failed segment in single-user
        # mode. It should be safe to remove the postmaster.pid
        # file since we do not expect the failed segment to be up.
        self.remove_postmaster_pid_from_remotehost(
            self.rewindSeg.targetSegment.getSegmentHostName(),
            self.rewindSeg.targetSegment.getSegmentDataDirectory())

        # Note the command name, we use the dbid later to
        # correlate the command results with GpMirrorToBuild
        # object.
        cmd = SegmentRewind('rewind dbid: %s' %
                               self.rewindSeg.targetSegment.getSegmentDbId(),
                               self.rewindSeg.targetSegment.getSegmentHostName(),
                               self.rewindSeg.targetSegment.getSegmentDataDirectory(),
                               self.rewindSeg.sourceHostname,
                               self.rewindSeg.sourcePort,
                               self.rewindSeg.progressFile,
                               verbose=True)


        self.logger.info("Running pg_basebackup with progress output temporarily in %s" % self.progressFile)



        cmd.run(validateAfter=True)
        self.set_results(CommandResult(0, b'', b'', True, False))

        # if self.__progressMode != GpMirrorListToBuild.Progress.NONE:
        #     progressCmd = GpMirrorListToBuild.ProgressCommand("tail the last line of the file",
        #                                               "set -o pipefail; touch -a {0}; tail -1 {0} | tr '\\r' '\\n' | tail -1".format(
        #                                                   pipes.quote(self.rewindSeg.progressFile)),
        #                                               self.rewindSeg.targetSegment.getSegmentDbId(),
        #                                               self.rewindSeg.progressFile)

       # TODO  remove of progress file
        # if not self.progressFile:
        #     os.remove(self.progressFile)

        self.logger.info("Successfully ran pg_basebackup: %s" % cmd.cmdStr)

        self.logger.info("Starting mirrors")  # need to remove once rewind and basebaackup is merged as wrapper

        seg = Segment(None,None,self.dbid,None,'n','d',self.syncWithSegmentHostname,self.syncWithSegmentHostname,self.port,self.datadir)

        #start the individual segment
        segStartCmd = SegmentStart(
            name="Starting new segment dbid %s on host %s." % (str(self.dbid), self.syncWithSegmentHostname)
            , gpdb=seg
            , numContentsInCluster=0  # Starting seg on it's own.
            , era=None
            , mirrormode="mirror"  #TBD
            , utilityMode=True
            , specialMode=None) #TBD
        # , ctxt=LOCAL
        # , remoteHost=seg.getSegmentHostName()
        # , pg_ctl_wait=True
        # , timeout=SEGMENT_TIMEOUT_DEFAULT)

        logger.info("segment start command %s" %segStartCmd)

        segStartCmd.run(validateAfter=True)

        return


    def remove_postmaster_pid_from_remotehost(self, host, datadir):
        cmd = Command(name = 'remove the postmaster.pid file',
                           cmdStr = 'rm -f %s/postmaster.pid' % datadir,
                           ctxt=gp.REMOTE, remoteHost = host)
        cmd.run()

        return_code = cmd.get_return_code()
        if return_code != 0:
            raise ExecutionError("Failed while trying to remove postmaster.pid.", cmd)
