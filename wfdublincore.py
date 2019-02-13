#! /usr/bin/env python
"""
#
#
#
  
"""

import os
import json
import logging
import uuid
import csv
import http.client
import datetime

from irods.exception import (DataObjectDoesNotExist, CollectionDoesNotExist)

#
# class for process Dublin Core
#
class dublinCore():

    def __init__(self, config, log ):

        print("dublinCore ")
        self.log = log
        self.config = config

    def processDCmeta(self, mongo, irods, collname, start_time, file, datastations):
        """Process Dublin Core metadata for the given data object.

        If the object is not registered in the archive, or the
        metadata for it is already present in the database, does
        nothing.

        Parameters
        ----------
        mongo : `mongomanager.MongoDAO`
            Connection to the Mongo DB storing the metadata.
        irods : `irodsmanager.irodsDAO`
            Connection to the iRODS managing the archive.
        collname : `str`
            Full collection name.
        start_time : `datetime`
            Record date.
        file : `str`
            Full file path in the file system.
        datastations : `dict`
            A catalog of station coordinates given by `dublinCore.getDataStations`.
        """

        fileStart = datetime.datetime.now()

        self.log.info("Starting processing DC for file %s", file)

        try:
            my_doc = self._createDataObject(mongo, irods, collname, start_time, file, datastations)
            if my_doc is not None:
                mongo._storeWFDataObject(my_doc)

        except Exception as ex:
            self.log.error("Could not compute DublinCore metadata")
            self.log.error(ex, exc_info=True)
            pass

        self.log.info("Completed processing DC for file in %s" % (datetime.datetime.now() - fileStart))


    #
    # retrieve data stations via webservices
    #
    def getDataStations(self):
        
        self.mystations = {}
        
        for net in self.config["DUBLIN_CORE"]["AUTH_NETWORKS"]:
            #mynet = str(self.config["DUBLIN_CORE"]["AUTH_NETWORKS"])
            self.mystations[net] = self._getDataStation(net)
        

        return  self.mystations

    #
    # retrieve data stations via webservices
    #
    def _getDataStation(self, mynet):
        
        mystations = {}
        
        #mynet = str(self.config["DUBLIN_CORE"]["AUTH_NETWORKS"])
        
        query = self.config['DUBLIN_CORE']['STATION_ENDPOINT']+"network="+mynet+"&format=text"
        conn = http.client.HTTPConnection(self.config['DUBLIN_CORE']['HTTP_CONNECTION'], 80)

        conn.connect()
        conn.request("GET", query)
        response = conn.getresponse()
        dataStations = response.read().decode('utf-8')
        conn.close() 
        
        try:
            self.log.info("start retrieve data stations")
            for e in dataStations.split("\n"):
                mystations[e.split('|')[1]]={"lat":e.split('|')[2],"lon":e.split('|')[3]}
               
        except Exception as ex:
            self.log.info("end of data stations found")
            pass
        
        return mystations    

    #
    # process Dublin Core Meta and build a Json Doc for Mongo
    #
    def _createDataObject(self, mongo, irods, collname, start_time, file, datastations):       

        currentCursor  = mongo.getFileDataObject(file)
        if currentCursor.count() != 0:
            self.log.info(file + ": DC metadata already present!")
            return None

        dirname, filename = os.path.split(file)
        obj_path = '{collname}/{filename}'.format(**locals())
        try:
            obj = irods.getObject(obj_path)
            PID = obj.metadata.get_one("PID").value
            iPath = obj.path

        except (DataObjectDoesNotExist, CollectionDoesNotExist):
            self.log.error("Data object not registered in iRODS: " + os.path.basename(file))
            return None

        except KeyError as ex:
            # failure PID check
            self.log.error("PID missing on: " + os.path.basename(file) + " : insert fake pid")
            PID = '11099/*********fakePid**********'
            iPath = obj_path
            pass


        #Lat-lon-elevation
        sta = os.path.basename(file).split('.')[1]
        net = os.path.basename(file).split('.')[0]
        self.log.info(sta)
        georef = datastations[net][sta]  
        elevation= 0
        self.log.info(georef)
        #Time-window
        # TODO: get better start and end Time
        #start_time = wfcollector._getDateFromFile(file)
        end_time = start_time + datetime.timedelta(days=1)
        
        
        # Create data object literal with additional metadata
        # '_cls' : 'commons.models.mongo.wf_do' is needed from pymodm into HTTP-API
        self.log.info("prep document")
        document = {
          '_cls' : 'eudat.models.mongo.wf_do',  
          'fileId': os.path.basename(file),
          'dc_identifier': PID,
          'dc_title': 'INGV_Repository',
          'dc_subject': 'mSEED, waveform, quality',
          'dc_creator': self.config['ARCHIVE'],
          'dc_contributor': 'network operator',
          'dc_publisher': self.config['ARCHIVE'],
          'dc_type': 'seismic waveform',
          'dc_format': 'MSEED',
          'dc_date': datetime.datetime.now(),
          'dc_coverage_x': float(georef['lat']),
          'dc_coverage_y': float(georef['lon']),
          'dc_coverage_z': float(elevation),
          'dc_coverage_t_min': start_time,
          'dc_coverage_t_max': end_time,
          'dcterms_available': datetime.datetime.now(),
          'dcterms_dateAccepted': datetime.datetime.now(),
          'dc_rights': 'open access',
          'dcterms_isPartOf': 'wfmetadata_catalog',
          'irods_path':iPath
          
        }

        return document




