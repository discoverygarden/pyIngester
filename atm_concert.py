#!/usr/bin/env python2.6
import logging
from FedoraWrapper import FedoraWrapper

class Concert():
    fedora_wrapper = None
    
    def __init__(self, element, prefix="test"):
        logging.getLogger('ingest.XMLHandler.atm_concert').debug('Start')
        self.dbid = element.get('id_concierto', default='None')
        if self.dbid == 'None':
            raise Exception('Didn\'t find \'id_concierto\' attribute in concert ' + 
                'element!  Continuing to next...')
        else:
            self.element = element
            #Don't actually want to get the 
            if Concert.fedora_wrapper == None:
                Concert.fedora_wrapper =  FedoraWrapper()
            #self.fedora_object = FedoraWrapper().getNextObject(prefix)
    
    def process(self, logger):
        logger.info('Starting to ingest: %s', self.dbid)
        #TODO: processing
        logger.info('Done ingesting: %s', self.dbid)

