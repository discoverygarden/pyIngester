#!/usr/bin/env python2.6

import logging, sys
from fcrepo.connection import Connection
from fcrepo.client import FedoraClient as Client
                    
class FedoraWrapper:
    connection = None
    client = None
    def __init__(self):
        '''Setup the connection if need be.'''
        if FedoraWrapper.connection == None:
            #TODO:  Get these settings from the config...
            FedoraWrapper.connection = Connection('http://localhost:8080/fedora', 
                username='fedoraAdmin',
                password='fedoraAdmin')
        if FedoraWrapper.client == None:
            FedoraWrapper.client = Client(FedoraWrapper.connection)
    
    def getNextObject(self, prefix = 'test'):
        '''Get an object with the given prefix'''
        pid = FedoraWrapper.client.getNextPid(unicode(prefix))
        #Create the object--initially inactive
        return FedoraWrapper.client.createObject(pid, state=u'I')
        
