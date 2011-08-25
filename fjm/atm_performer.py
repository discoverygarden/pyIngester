
import logging
from islandoraUtils.metadata import fedora_relationships as FR, eaccpf as CPF
from islandoraUtils import fedoraLib as FL
from atm_person import Person
from FedoraWrapper import FedoraWrapper
from tempfile import NamedTemporaryFile
import fcrepo

class Performer(Person):
    def __init__(self, file_path, element, prefix=None):
        super(Performer, self).__init__(file_path, element, prefix, loggerName='ingest.XMLHandler.atm_performer')
        
        self.dbid = element.get('id')
        self.name = {
            'forename': self.element.findtext('nombre'), 
            'surname': self.element.findtext('apellidos')
        }
        self.norm_name = self.normalized_name()
    
    def _sanityTest(self):
        if self.dbid == None:
            raise Exception('Didn\'t find "id" attribute in %(tag)s element on line %(line)s of %(file)s Continuing to next...' % {'tag': self.element.tag,'line': self.element.sourceline, 'file': self.file_path})
    
    def process(self):
        logger = self.logger
        logger.info('Starting to ingest: Performer %s' % self.dbid)
        
        try:
            logger.info('Checking to see if %s already exists in Fedora' % self.norm_name)
            pid = self[self.norm_name]
            logger.info('Found %(pid)s' % {'pid': pid})
            if pid:
                logger.warning('%(name)s already exists as pid %(pid)s! Overwriting DC DS!' % {'name': self.norm_name, 'pid': pid})
                self.performer = FedoraWrapper.client.getObject(pid)
            else:
                msg = 'Something went horribly wrong!  Found a pid (%(pid)s), but couldn\'t access it...' % {'pid': pid}
                logger.error(msg)
                raise Exception(msg)
        except KeyError:
            try:
                logger.debug('Not known by name, checking by performerID')
                pid = FedoraWrapper.getPid(uri=Performer.NS['fjm-db'].uri, predicate='performerID', obj="'%s'" % self.dbid)
                logger.info('Found %(pid)s' % {'pid': pid})
                if pid:
                    logger.warning('%(name)s already exists as pid %(pid)s! Overwriting DC DS!' % {'name': self.norm_name, 'pid': pid})
                    self.performer = FedoraWrapper.client.getObject(pid)
                else:
                    msg = 'Something went horribly wrong!  Found a pid (%(pid)s), but couldn\'t access it...' % {'pid': pid}
                    logger.error(msg)
                    raise Exception(msg)
            except KeyError:
                logger.info('Doesn\'t exist: creating a new Fedora Object')
                self.performer = FedoraWrapper.getNextObject(self.prefix, label='%s' % self.norm_name)
        #except Exception, e:
        #    print e

        rels_ext = FR.rels_ext(self.performer, namespaces=Performer.NS.values())
        model = {
            'pred':  FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
            'object': FR.rels_object('atm:personCModel', FR.rels_object.PID)
        }
        db = {
            'pred': FR.rels_predicate(alias='fjm-db', predicate='performerID'),
            'object': FR.rels_object(self.dbid, FR.rels_object.LITERAL)
        }
        
        #TODO:  It might be nice to generalize this somehow?  (Adding a relationship only if it doesn't already exist...  That is, the predicate and object is the same as any current relationship)
        if len(rels_ext.getRelationships(predicate=model['pred'], object=model['object'])) == 0:
            rels_ext.addRelationship(model['pred'], model['object'])
        if len(rels_ext.getRelationships(predicate=db['pred'], object=db['object'])) == 0:
            rels_ext.addRelationship(db['pred'], db['object'])
        rels_ext.update()
            
        #Yay Pythonic-ness?  Try to get an existing EAC-CPF, or create one if none is found
        try:
            eaccpf = CPF.EACCPF(self.performer.pid, xml=self.performer['EAC-CPF'].getContent().read())
            event_type="modified"
        except fcrepo.connection.FedoraConnectionException, e:
            if e.httpcode == 404:
                eaccpf = CPF.EACCPF(self.performer.pid)
                event_type="created"
            else:
                raise e
        eaccpf.add_maintenance_event(type=event_type, time="now", agent_type="machine", agent="atm_performer.py")
        eaccpf.add_XML_source(caption='XML from database dump', xml=self.element)
        eaccpf.add_name_entry(name=self.name)
        
        
        #Use the fcrepo implementation, as we're just passing a string of XML...
        self.performer.addDataStream(dsid='EAC-CPF', body='%s' % eaccpf, mimeType=unicode("text/xml"))
        dc = self.performer['DC']
        dc['title'] = [self.norm_name]
        dc.setContent()
        
        self[self.norm_name] = self.performer.pid
