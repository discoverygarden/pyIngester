#!/usr/bin/env python2.6

import logging, sys
from fcrepo.connection import Connection
from fcrepo.client import FedoraClient as Client
                    
class FedoraWrapper:
    #Static variables, so only one connection and client should exist at any time.
    connection = None
    client = None
    
    @staticmethod
    def init():
        '''Setup the connection if need be.'''
        if FedoraWrapper.connection == None:
            #TODO:  Get these settings from the config...
            FedoraWrapper.connection = Connection('http://localhost:8080/fedora', 
                username='fedoraAdmin',
                password='fedoraAdmin',
                persistent=False)
        if FedoraWrapper.client == None:
            FedoraWrapper.client = Client(FedoraWrapper.connection)
    
    @staticmethod
    def destroy():
        if FedoraWrapper.client != None:
            del FedoraWrapper.client
        if FedoraWrapper.connection != None:
            FedoraWrapper.connection.close()
            del FedoraWrapper.connection
    
    @staticmethod
    def getNextObject(prefix = 'test', label='an object'):
        '''Get an object with the given prefix--Initially created inactive
        Make Active after adding additional DSs in Microservices!
        '''
        FedoraWrapper.init()
        pid = FedoraWrapper.client.getNextPID(unicode(prefix))
        #Create the object--initially inactive
        return FedoraWrapper.client.createObject(pid, label=unicode(label), state=u'I')

