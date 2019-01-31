#! /usr/bin/env python
"""
#
#
#
  
"""

import os
import json
import textwrap

import irods
from irods.session import iRODSSession
import irods.exception as ex
from irods.column import Criterion
from irods.data_object import chunks
import irods.test.helpers as helpers
import irods.keywords as kw
from irods.meta import iRODSMeta
from irods.models import (DataObject, Collection, Resource, User, DataObjectMeta,CollectionMeta, ResourceMeta, UserMeta)
from irods.results import ResultSet
from irods.rule import Rule
from irods.meta import iRODSMetaCollection
from irods.exception import CollectionDoesNotExist


#
#  data access object class for irods
#
class irodsDAO():


    def __init__(self, config, log):

        print("irods ")
        self.session = None
        self.log = log
        self.config = config

    #
    # irods Connection
    #
    def _irodsConnect(self):

        if self.session:
            return

        self.log.info("try connection to irods")
        # Make iRODS connection
        self.session = iRODSSession(host=str(self.config['IRODS']['HOST']), port=str(self.config['IRODS']['PORT']), user=str(self.config['IRODS']['USER']), password=str(self.config['IRODS']['PWD']), zone=str(self.config['IRODS']['ZONE']))
        self.log.info("done")

    #
    # irods ingestion iREG
    #
    def doRegister(self, dirname, collname, filename):   

        self._irodsConnect()

        obj_file = os.path.join(dirname, filename)
        obj_path = '{collname}/{filename}'.format(**locals())
        self._checkCollExsist(collname)
        
        # register file in test collection
        self.log.info("check obj_file : "+obj_file)
        self.log.info("check obj_path : "+obj_path)

        self.log.info("check or create a collection recursively : "+collname)
        try:
            self.session.data_objects.register(obj_file, obj_path)
            self.log.info("file registered! : "+obj_path)
        except Exception as ex:
            self.log.error("Could not register a file_obj  ")         
            self.log.error(ex)
            pass

        # confirm object presence
        #obj = self.session.data_objects.get(obj_path)
        
        #print("registred!")
        #print (obj)
       
    #
    # irods ingestion iPUT
    #
    def doPut(self, dirname, collname, filename):   

        self._irodsConnect()

        obj_file = os.path.join(dirname, filename)
        obj_path = '{collname}/{filename}'.format(**locals())
        self._checkCollExsist(collname)
        
        # register file in test collection
        self.log.info("check obj_file : "+obj_file)
        self.log.info("check obj_path : "+obj_path)

        self.log.info("check or create a collection recursively : "+collname)
        try:
            self.session.data_objects.put(obj_file, obj_path)
            self.log.info("file put! : "+obj_path)
        except Exception as ex:
            self.log.error("Could not put a file_obj  ")         
            self.log.error(ex)
            pass


    #
    # Execute a demo rule
    #
    def _ruleExec(self, rule_file_path):

        #self._irodsConnect()

        # run  rule
        myrule = Rule(self.session, rule_file_path)
        ruleout = myrule.execute()

        return ruleout

    #
    # Check if a collection exsist
    #
    def _checkCollExsist(self, collname):

        self.log.info("check or create a collection recursively : "+collname)
        try:
            self.session.collections.create(collname, recurse=True)
            self.log.info("collection created! : "+collname)
        except Exception as ex:
            self.log.error("Could not create a collection recursively ")
            self.log.error(ex)
            pass

    #
    # get Digital Object
    #
    def getObject(self, obj_path):

        self._irodsConnect()

        return self.session.data_objects.get(obj_path)

    #
    # irods Rule Execution: PID creation  (PID)
    #
    def rulePIDsingle(self, object_path):
           
        self.log.info("exec PID SINGLE rule inside irods ")

        # rule parameters
        input_params = {  # extra quotes for string literals
            "*path": '"{object_path}"'.format(**locals()),
            '*pid' : '"null"'.format(**locals()),
            '*parent_pid':'"none"'.format(**locals()),
            '*ror':'"none"'.format(**locals()),
            '*fio':'"none"'.format(**locals()),
            '*fixed':'"False"'.format(**locals())
        }

        # rule Output
        output = 'ruleExecOut'

        #
        # @TODO: move writeLine from stdout to rodsLog - serverLog
        #

        # rule body
        rule_body = textwrap.dedent('''\
                                    eudatPidSingleCheck2 {

                                      EUDATSearchPID(*path, *existing_pid)
                                      if (*existing_pid == "empty") {
                                        EUDATCreatePID(*parent_pid, *path, *ror, *fio, *fixed, *newPID);
                                        writeLine("serverLog","PID-new: *newPID");
                                      }
                                      else {
                                        writeLine("serverLog","PID-existing: *existing_pid");
                                      }
                                    }
                                    
                                ''')

        # prep  rule
        myrule = Rule(self.session, body=rule_body, params=input_params, output=output )
        
        # exec rule
        try:
            myrule.execute()
             # check that metadata is there
            returnedMeta = self.session.metadata.get(DataObject, object_path)
        except Exception as ex:
            print("Could not execute a rule for PID ")
            print(ex)
            pass

        return returnedMeta 

    #
    # irods Rule Execution: REPLICATION   (REP)
    #
    def ruleReplication(self, object_path, target_path):
          
        self.log.info("exec Replication rule inside irods ")
        returnedMeta = {}

       # rule parameters
        input_params = {  # extra quotes for string literals
            "*source": '"{object_path}"'.format(**locals()),
            '*destination' : '"{target_path}"'.format(**locals())
        }

        # rule Output
        output = 'ruleExecOut'

        # rule body
        rule_body = textwrap.dedent('''\
                                    Replication {
                                                *registered = "true";
                                                *recursive = "true";
                                                *status = EUDATReplication(*source, *destination, *registered, *recursive, *response);
                                                if (*status) {
                                                    writeLine("stdout", "Replica *source on *destination Success!");
                                                }
                                                else {
                                                    writeLine("stdout", "Replica *source on *destination Failed: *response");
                                                }
                                            }
                                    
                                ''')

        # prep  rule
        myrule = Rule(self.session, body=rule_body, params=input_params, output=output )
        
        # exec rule
        try:
            myrule.execute()
             # check that metadata is there
            returnedMeta = self.session.metadata.get(DataObject, object_path)
        except Exception as ex:
            print("Could not execute a rule for REPLICATION ")
            print(ex)
            pass

        return returnedMeta  

    #
    # irods Rule Execution: REMOTE REPLICA REGISTRATION  (RRR)
    #
    def ruleRegistration(self, object_path, target_path):
               
        self.log.info("exec  Registration Reote replicated object inside irods ")

        # rule parameters   
        input_params = {  # extra quotes for string literals
            "*source": '"{object_path}"'.format(**locals()),
            '*destination' : '"{target_path}"'.format(**locals())
        }

        # rule Output
        output = 'ruleExecOut'

        # rule body
        rule_body = textwrap.dedent('''\
                                    RegRule {
                                            *notification = 1
                                            EUDATPIDRegistration(*source, *destination, *notification, *response);
                                            writeLine("stdout","EUDAT registration: *response");
                                        }
                                ''')

        # prep  rule
        myrule = Rule(self.session, body=rule_body, params=input_params, output=output )
        
        # exec rule
        try:
            myrule.execute()
             # check that metadata is there
            returnedMeta = self.session.metadata.get(DataObject, object_path)
        except Exception as ex:
            print("Could not execute a rule for REGISTRATION ")
            print(ex)
            pass

        return returnedMeta         
