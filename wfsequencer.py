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


#
# implement rules sequence
#
class sequencer(object):

    # init

    def __init__(self, config, log, irods, mongo, WFcollector, dublinCore):

        print("sequencer ")
        #
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


    #..................................... iPUT_INGESTION - 
    #
    # Exec Proc: Put Digital objects into iRODS
    #
    def put(self):

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


    #..................................... TEST_RULE - 
    #
    # Exec Rule: test rule execution w/o params (called directly self.digitObjProperty['file'].r)
    #
    def testRule(self):
        rule_path =  self.ruleMap['RULE_PATHS']['TEST_RULE']
        self.log.info("exec TEST rule  on  : "+self.digitObjProperty['file'])

        try:
            myvalue = self.irods._ruleExec(rule_path)
        except Exception as ex:
            self.log.error("Could not execute a rule")
            self.log.error(ex)
            pass
        
        #return myvalue    


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
            
            self.dublinCore.processDCmeta(self.mongo, self.irods, self.digitObjProperty['collname'], self.digitObjProperty['start_time'], self.digitObjProperty['file'], self.digitObjProperty['datastations'])
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
        """Runs the sequence defined in the rule map on the file given by digitObjProperty."""

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
                    

