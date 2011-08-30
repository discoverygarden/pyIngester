#!/usr/bin/env python2.6

import logging, sys
from fcrepo.connection import Connection
from fcrepo.client import FedoraClient as Client
from atm_object import atm_object as ao
from islandoraUtils.metadata import fedora_relationships as FR
                    
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

    @staticmethod
    def getPid(uri='fedora:', predicate=None, obj=None, tuples=None, default=None):
        '''Select Should fail with a KeyError if a matching object is not found an no default is given
        'tuples' may be a list of tuples containing a uri, predicate, and object which are AND'd together
        '''
        FedoraWrapper.init()
        filter = list()
        if tuples:
            for i_uri, i_predicate, i_obj in tuples:
                filter.append('$obj <%(uri)s%(predicate)s> %(obj)s' % {
                        'uri': i_uri, 
                        'predicate': i_predicate, 
                        'obj': i_obj
                    }
                )
        else:
            filter.append('$obj <%(uri)s%(predicate)s> %(obj)s' % {
                    'uri': uri, 
                    'predicate': predicate, 
                    'obj': obj
                }
            )
                
        if filter:
            query = '\
select $obj from <#ri> \
where %s\
minus $obj <fedora-model:state> <fedora-model:Deleted>' % ' and '.join(filter)
            #print query
            try:
                pid = ''
                for result in FedoraWrapper.client.searchTriples(query=query, lang='itql'):
                    print '%s' % result
                    
                    if 'obj' in result:
                        pid = result['obj']['value']
                        break
                if not pid:
                    raise KeyError('Result not in found?')
                else:
                    prefix = 'info:fedora/'
                    return pid[len(prefix):]
            except KeyError as e:
                if not default:
                    raise e
                else:
                    return default

    
    @staticmethod
    def addRelationshipWithoutDup(rel, fedora=None, rels_ext=None):
        '''
        'rel': a 2-tuple containing containing a rels_predicate and a rels_object, in that order.
        'fedora': a fcrepo FedoraObject (could probably use some testing...)
        'rels_ext': a islandoraUtils rels_ext object
        
        Only one of 'fedora' and 'rels_ext' is required.  If both are given, only rels_ext will be used, whatever differences that might cause.
        
        XXX: Should probably get the list of namespaces in a better manner, so as not to require the import of atm_object
        '''
        if rels_ext:
            pass
        elif fedora:
            rels_ext = FR.rels_ext(obj=fedora, namespaces=ao.NS.values())
        else:
            raise Exception('Either fedora or rels_ext must be provided!')
        pred, obj = rel
        if len(rels_ext.getRelationships(predicate=pred, object=obj)) == 0:
            rels_ext.addRelationship(predicate=pred, object=obj)
            
        return rels_ext

    @staticmethod
    def addRelationshipsWithoutDup(rels, fedora=None, rels_ext=None):
        if rels_ext:
            pass
        elif fedora:
            rels_ext = FR.rels_ext(obj=fedora, namespaces=ao.NS.values())
        else:
            raise Exception('Either fedora or rels_ext must be provided!')
            
        for rel in rels:
            FedoraWrapper.addRelationshipWithoutDup(rel, rels_ext=rels_ext)
            
        return rels_ext