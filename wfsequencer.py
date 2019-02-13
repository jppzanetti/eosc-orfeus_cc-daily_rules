#! /usr/bin/env python
"""
#
#
#
  
"""
import os
import sys
import json
import collections


class sequencer(object):
    """Implements and runs the rule sequence on a file.

    Attributes
    ----------
    config : `dict`
        The configuration options loaded from the config file. See config.json.
    ruleMap : `dict`
        The rule mapping loaded from the file. See ruleMap.json.
    log : `logging.Logger`
        The WFCatalog Logger object.
    irods : `irodsmanager.irodsDAO`
        Connection to the iRODS managing the archive.
    mongo : `mongomanager.MongoDAO`
        Connection to the Mongo DB storing the metadata.
    WFCollector : `wfcollector.WFCatalogCollector`
        WF Catalog collector to compute metadata.
    dublinCore : `wfdublincore.dublinCore`
        Dublin Core metadata processor.
    digitObjProperty : `dict`
        The properties of the digital object being processed.
        - ``file``: Full file path (`str`).
        - ``filename``: Filename (`str`).
        - ``dirname``: Full path of the directory that contains the file (`str`).
        - ``collname``: Full name of the collection (`str`).
        - ``object_path``: Full logical path of the data object (`str`).
        - ``target_path``: Full logical path of the replica (`str`).
        - ``start_time``: Date of record (`datetime`).
        - ``datastations``: The catalog of station coordinates
                            given by `dublinCore.getDataStations` (`dict`).
    """

    def __init__(self, config, log, irods, mongo, WFcollector, dublinCore):

        print("sequencer ")

        # load ruleMap from file
        cfg_dir = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(cfg_dir, config['RULEMAP_FILE']), "r") as rlmp:
            self.ruleMap = json.load(rlmp)

        self.config = config
        self.log = log
        self.irods = irods
        self.mongo = mongo
        self.WFcollector = WFcollector
        self.dublinCore = dublinCore

    #..................................... iREG_INGESTION - 
    #
    # Exec Proc: Register Digital objects into iRODS
    #
    def register(self):

        self.log.info("iREG on iRODS of : "+self.digitObjProperty['file'])
        try:
            self.irods.doRegister( self.digitObjProperty['dirname'], self.digitObjProperty['collname'], self.digitObjProperty['filename'])
        except Exception as ex:
            self.log.error("Could not execute a doRegister ")
            self.log.error(ex)
            pass


    def put(self):
        """Defines an ingestion policy that puts a new digital object into iRODS.

        The file is only put if it is not already registered in iRODS
        or if it is registered with a different checksum. This policy
        registers the file's SHA256 checksum in iRODS and purges the
        cache.
        """

        self.log.info("iPUT on iRODS of : "+self.digitObjProperty['file'])
        try:
            self.irods.doPut(self.digitObjProperty['dirname'],
                             self.digitObjProperty['collname'],
                             self.digitObjProperty['filename'],
                             purge_cache=True,
                             register_checksum=True,
                             check='updated')
        except Exception as ex:
            self.log.error("Could not execute a doPut ")
            self.log.error(ex)
            pass


    def testRule(self):
        """Loads and executes the parameterless iRODS rule in the file pointed
        by TEST_RULE in the rule map."""
        rule_path =  self.ruleMap['RULE_PATHS']['TEST_RULE']
        self.log.info("exec TEST rule  on  : "+self.digitObjProperty['file'])

        try:
            myvalue = self.irods._ruleExec(rule_path)
        except Exception as ex:
            self.log.error("Could not execute a rule")
            self.log.error(ex)
            pass


    #..................................... PID - 
    #
    # Exec Rule:  Make a PID and register into EPIC
    #
    def PidRule(self):
        
        # rule execution w params (called w rule-body, params, and output -must-)
        self.log.info("call PID rule  on  : "+self.digitObjProperty['file'])

        # make a pid
        retValue = self.irods.rulePIDsingle( self.digitObjProperty['object_path'], self.ruleMap['RULE_PATHS']['PID'])
        #print (retValue)
        
        #return retValue


    #..................................... REPLICATION -  
    #
    # Exec Rule: DO a Remote Replica 
    #
    def ReplicationRule(self):
        
        self.log.info("call REPLICATION rule  on self.digitObjProperty['file'] : "+self.digitObjProperty['file'])

        self.log.info("call REP rule  object_path : "+self.digitObjProperty['object_path'])
        self.log.info("call REP rule  target_path : "+self.digitObjProperty['target_path'])

        # make a replica
        retValue = self.irods.ruleReplication(self.digitObjProperty['object_path'], self.digitObjProperty['target_path'], self.ruleMap['RULE_PATHS']['REPLICA'])

        #return retValue


    #..................................... REGISTRATION_REPLICA -  
    #
    # Exec Rule: Registration of Remote PID into local ICAT
    #
    def RegistrationRule(self):
        
        self.log.info("call REGISTRATION_REPLICA rule  on  : "+self.digitObjProperty['file'])

        # make a registration
        retValue = self.irods.ruleRegistration( self.digitObjProperty['object_path'], self.digitObjProperty['target_path'], self.ruleMap['RULE_PATHS']['REGISTER'])

        #return retValue


    #..................................... DUBLINCORE_META - 
    #
    # Exec Proc: Store DublinCore metadata into mongo WF_CATALOG
    #           
    def DublinCoreMeta(self):
        
        self.log.info("call process DUBLIN CORE meta of : "+self.digitObjProperty['file'])
        try:
            self.dublinCore.processDCmeta(self.mongo,
                                          self.irods,
                                          self.digitObjProperty['collname'],
                                          self.digitObjProperty['start_time'],
                                          self.digitObjProperty['file'],
                                          self.digitObjProperty['datastations'])
            self.log.info(" DUBLIN CORE for digitalObject: "+self.digitObjProperty['object_path']+" is: OK" )
        except Exception as ex:
            self.log.error("Could not process DublinCore metadata")
            self.log.error(ex)
            pass        


    #..................................... WFCATALOG_META -
    #
    # Exec Proc: Store WF_CATALOG metadata into mongo WF_CATALOG
    #            
    def WFCatalogMeta(self):
        
        self.log.info("called collect WF CATALOG METADATA of : "+self.digitObjProperty['file'])
        try:
            self.WFcollector.collectMetadata(self.digitObjProperty['file'])
            self.log.info(" WF METADATA for digitalObject: "+self.digitObjProperty['object_path']+" is: OK" )
        except Exception as ex:
            self.log.error("Could not compute WF metadata")
            self.log.error(ex)
            pass          
        

    def purgeTemp(self):
        """Defines the policy to delete the file from the temporary archive."""

        self.log.info("Purging file: "+self.digitObjProperty['file'])
        try:
            self.irods.purgeTempFile(self.digitObjProperty['dirname'],
                                     self.digitObjProperty['collname'],
                                     self.digitObjProperty['filename'],
                                     7,
                                     if_registered=True)
        except Exception as ex:
            self.log.error("Could not execute a purgeTemp")
            self.log.error(ex)
            pass


    def doSequence(self, digitObjProperty):
        """Runs the sequence defined by the rule map on the file given by digitObjProperty."""

        # load current property
        self.digitObjProperty = digitObjProperty
        self.log.info("\n" + " --- --- START SEQUENCE FOR : " + self.digitObjProperty['file'])

        # Log info for each file processed
        self.log.info("collname: " + digitObjProperty["collname"])
        self.log.info("dirname: "+ digitObjProperty["dirname"])
        self.log.info("filename: "+ digitObjProperty["filename"])

        # for each step apply rule
        for step in self.ruleMap['SEQUENCE']:
            try:
                self.log.info("Applying rule: " + self.ruleMap['RULE_MAP'][step])
                getattr(self, self.ruleMap['RULE_MAP'][step])()
            except Exception as ex:
                self.log.error("Sequence error, could not execute rule: "+self.steps_definition[step])
                self.log.error(ex)
                pass
                    

