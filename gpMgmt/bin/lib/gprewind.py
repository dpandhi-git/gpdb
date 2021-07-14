#!/usr/bin/env python3

import datetime
import sys
import os
import re
import json
import shutil
from gppylib.commands.gp import SegmentRewind, SegmentStart
from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.db import dbconn
from gppylib.commands import unix
from gppylib.commands.pg import DbStatus
from gppylib import gplog
from gppylib.commands.base import *
from gppylib.gparray import Segment
from time import sleep
from gppylib.operations.buildMirrorSegments import gDatabaseDirectories, gDatabaseFiles

description = ("""

Configure rewind segment directories for installation into a pre-existing GPDB array.

Used by at least gpexpand, gprecoverseg, and gpaddmirrors

""")
DEFAULT_BATCH_SIZE=8
EXECNAME = os.path.split(__file__)[-1]


class PgConfigureRewind(Command):
    def __init__(self, name, cmdstr, logger,sourceHostname, sourcePort,targetdbid, targetHostname,targetdatadir,targetPort, progressFile, shouldDeleteProgressFile, progressMode):
        self.logger = logger
        self.sourceHostname = sourceHostname
        self.sourcePort = sourcePort
        self.targetdbid = targetdbid
        self.targetHostname = targetHostname
        self.targetdatadir = targetdatadir
        self.targetPort = targetPort
        self.progressFile = progressFile
        # self.shouldDeleteProgressFile = shouldDeleteProgressFile
        self.progressMode = progressMode
        Command.__init__(self, name, cmdstr)

    def run(self):
        self.logger.debug('Do CHECKPOINT on %s (port: %d) before running pg_rewind.' % (self.sourceHostname, self.sourcePort))
        dburl = dbconn.DbURL(hostname=self.sourceHostname,
                             port=self.sourcePort,
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
            self.targetHostName,
            self.targetdatadir )

        #TODO check verbose to be on or not

        # Note the command name, we use the dbid later to
        # correlate the command results with GpMirrorToBuild
        # object.
        cmd = SegmentRewind('rewind dbid: %s' %
                               self.targetdbid,
                               self.targetHostName,
                               self.targetdatadir ,
                               self.sourceHostname,
                               self.sourcePort,
                               self.progressFile,
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

        self.logger.info("Successfully ran pg_rewind: %s" % cmd.cmdStr)

        self.logger.info("Starting mirrors")  # need to remove once rewind and basebaackup is merged as wrapper

        seg = Segment(None,None,self.targetdbid,None,'n','d',self.targetHostName,self.targetHostName,self.targetPort,self.targetdatadir)

        #start the individual segment
        segStartCmd = SegmentStart(
            name="Starting new segment dbid %s on host %s." % (str(self.targetdbid), self.targetHostName)
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


def parseargs():
    parser = OptParser(option_class=OptChecker,
                       description=' '.join(description.split()),
                       version='%prog version $Revision: $')
    parser.set_usage('%prog is a utility script used by gpexpand, gprecoverseg, and gpaddmirrors and is not intended to be run separately.')
    parser.remove_option('-h')
    parser.add_option('-v','--verbose', action='store_true', help='debug output.')
    parser.add_option('-c', '--confinfo', type='string')
   # parser.add_option('-n', '--newsegments', action='store_true')
    parser.add_option('-b', '--batch-size', type='int', default=DEFAULT_BATCH_SIZE, metavar='<batch_size>')
    # parser.add_option("-V", "--validation-only", dest="validationOnly", action='store_true', default=False)
    # parser.add_option("-W", "--write-gpid-file-only", dest="writeGpidFileOnly", action='store_true', default=False)
    # parser.add_option('-f', '--force-overwrite', dest='forceoverwrite', action='store_true', default=False)
    parser.add_option('-l', '--log-dir', dest="logfileDirectory", type="string")

    parser.set_defaults(verbose=False, filters=[], slice=(None, None))

    # Parse the command line arguments
    (options, args) = parser.parse_args()

    if not options.confinfo:
        raise Exception('Missing --confinfo argument.')

    if options.batch_size <= 0:
        logger.warn('batch_size was less than zero.  Setting to 1.')
        options.batch_size = 1

    # if options.validationOnly and options.writeGpidFileOnly:
    #     raise Exception('Only one of --validation-only and --write-gpid-file-only can be specified')

    seg_info = []
    conf_lines = options.confinfo.split(',')
    for line in conf_lines:
        conf_vals = line.split(':')
        if len(conf_vals) < 5:
            raise Exception('Invalid configuration value: %s' % conf_vals)
        if conf_vals[0] == '':
            raise Exception('Missing data directory in: %s' % conf_vals)
        try:
            if int(conf_vals[1]) < 1024:
                conf_vals[1] = int(conf_vals[1])
        except Exception as e:
            raise Exception('Invalid port in: %s' % conf_vals)
        if conf_vals[2] != 'true' and conf_vals[2] != 'false':
            raise Exception('Invalid isPrimary option in: %s' % conf_vals)
        if conf_vals[3] != 'true' and conf_vals[3] != 'false':
            raise Exception('Invalid directory validation option in: %s' % conf_vals)
        try:
            conf_vals[4] = int(conf_vals[4])
        except:
            raise Exception('Invalid dbid option in: %s' % conf_vals)
        try:
            conf_vals[5] = int(conf_vals[5])
        except:
            raise Exception('Invalid contentid option in: %s' % conf_vals)
        seg_info.append(conf_vals)

    seg_info_len = len(seg_info)
    if seg_info_len == 0:
        raise Exception('No segment configuration values found in --confinfo argument')
    elif seg_info_len < options.batch_size:
        # no need to have more threads than segments
        options.batch_size = seg_info_len

    return options, args, seg_info

try:
    (options, args, seg_info) = parseargs()

    logger = gplog.setup_tool_logging(EXECNAME, unix.getLocalHostname(), unix.getUserName(),
                                      logdir=options.logfileDirectory)

    # if options.verbose:
    #     gplog.enable_verbose_logging()

    logger.info("Starting gp_rewind with args: %s" % ' '.join(sys.argv[1:]))

    pool = WorkerPool(numWorkers=options.batch_size)

    for seg in seg_info:
        sourceHostname = seg[0]
        sourcePort = int(seg[1])
        targetdbid = int(seg[2])
        targetHostname = seg[3]
        targetdatadir = seg[4]
        targetPort = int(seg[5])
        progressFile = seg[6]

        cmd = PgConfigureRewind( name = 'Rewind segment directory'
                             , cmdstr = ' '.join(sys.argv)
                             , logger = logger
                             , sourceHostname = sourceHostname
                             , sourcePort = sourcePort
                             , targetdbid = targetdbid
                             , targetHostname = targetHostname
                             , targetdatadir = targetdatadir
                             , targetPort = targetPort
                             , progressFile  = progressFile
                             )
        pool.addCommand(cmd)

    pool.join()

    if options.validationOnly:
        errors = []
        for item in pool.getCompletedItems():
            if not item.get_results().wasSuccessful:
                errors.append(str(item.get_results().stderr).replace("\n", " "))

        if errors:
            print("\n".join(errors), file=sys.stderr)
            sys.exit(1)
        else: sys.exit(0)
    else:
        try:
            pool.check_results()
        except Exception as e:
            if options.verbose:
                logger.exception(e)
            logger.error(e)
            print(e, file=sys.stderr)
            sys.exit(1)

    sys.exit(0)

except Exception as msg:
    logger.error(msg)
    print(msg, file=sys.stderr)
    sys.exit(1)

finally:
    if pool:
        pool.haltWork()
