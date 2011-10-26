#!/usr/bin/env python2.6


from islandoraUtils.fedoraLib import update_hashed_datastream_without_dup as UD
import os
import subprocess

def update_datastream(obj, dsid, filename, label='', mimeType='', controlGroup='M', tries=3, checksumType='SHA-1', checksum=None):
    '''
    Wrap it, so as to be able to sleep for an amount of time beforehand, to try to get rid of the timestamp issues.
    NOTE:  This dedup stuff doesn't really work on the EAC-CPF, EAC-CPF includes a current timestamp:  Therefore, it will always change.
    '''
    sleep(5)
    if filename.endswith('.mp3'):
        logfilename = '%s.mp3val.log' % filename
        if not os.path.exists(logfilename) or os.path.getmtime(logfilename) < os.path.getmtime(filename):
            command = ['mp3val', '-f', '-nb', '-t', '-l%s' % logfilename, filename]
            subprocess.call(command)
    return UD(obj=obj, dsid=dsid, filename=filename, label=label, mimeType=mimeType, controlGroup=controlGroup, tries=tries, checksumType=checksumType, checksum=checksum)
    
import logging, sys
from fcrepo.connection import Connection
from fcrepo.client import FedoraClient as Client
from atm_object import atm_object as ao
from islandoraUtils.metadata import fedora_relationships as FR
from time import sleep

class FedoraWrapper:
    #Static variables, so only one connection and client should exist at any time.
    connection = None
    client = None
    logger = logging.getLogger('pyIngester.fjm.FedoraWrapper')
    
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
        #FIXME (major):  Make objects be created as 'Inactive'...  Bloody timelines.
        obj = FedoraWrapper.client.createObject(pid, label=unicode(label), state=u'A')
        
        #FedoraWrapper.logger.debug(FedoraWrapper.client.getObjectProfile(pid))
        
        return obj

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
            except KeyError, e:
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
    
    @staticmethod
    def correlateDBEntry(predicate, idpred):
        '''
        This function is used to add relations involving PIDs to objects, based on relations to literals which were added during the original ingest.
        For example, in the original ingest, 'performances' are added with a relation to the score db id 'fjm-db:basedOn', which the scores are have relations to their DB id 'fjm-db:scoreID'.  This function uses a query which matches the two literals, and adds the relation 'atm-rel:basedOn' (note:  same predicate, different namespace) to the performance,  which relates directly to the score whose ID matched.
        
        NOTE:  SPARQL is bloody amazing.  That is all...
            (query description:
                1.  add prefixes,
                2.  select the object and subject of the relationship to resolve, based on matching the ID
                3.  optionally select any already existing relationships
                4.  keep results where step 3 returned nothing, or those where the selected $sub is not equal to anything found in step 3.)
        TODO (minor): I can see this being a little slow, as it is called fairly often...  Some method to streamline this might be nice, or to call it less frequently?...  Anyway.
        '''
        FedoraWrapper.init()
        for result in FedoraWrapper.client.searchTriples(query='\
PREFIX atm-rel: <%(atm-rel)s> \
PREFIX fjm-db: <%(fjm-db)s> \
SELECT $obj $sub \
FROM <#ri> \
WHERE { \
    $obj fjm-db:%(predicate)s $id . \
    $sub fjm-db:%(idpred)s $id . \
    OPTIONAL {$obj atm-rel:%(predicate)s $pid} . \
    FILTER(!bound($pid) || $sub != $pid) \
}' % {
        'fjm-db': ao.NS['fjm-db'].uri,
        'atm-rel': ao.NS['atm-rel'].uri,
        'predicate': predicate,
        'idpred': idpred
    }, lang='sparql', limit='1000000'):
            FedoraWrapper.addRelationshipWithoutDup((
                    FR.rels_predicate(alias='atm-rel', predicate=predicate),
                    FR.rels_object(result['sub']['value'].rpartition('/')[2], FR.rels_object.PID)
                ), fedora=FedoraWrapper.client.getObject(result['obj']['value'].rpartition('/')[2])).update()
