
import logging
from islandoraUtils.metadata import fedora_relationships as FR, eaccpf as CPF
from islandoraUtils import fedoraLib as FL
from atm_person import Person
from FedoraWrapper import FedoraWrapper
from tempfile import NamedTemporaryFile
import fcrepo
import os.path as path

class Composer(Person):
    def __init__(self, file_path, element, prefix=Person.PREFIX):
        super(Composer, self).__init__(file_path, element, prefix, loggerName='ingest.XMLHandler.atm_composer')
        
        self.dbid = element.get('id')
        self.name = {
            'forename': self.element.findtext('Nombre'), 
            'surname': self.element.findtext('Apellidos')
        }
        self.photo = self.element.findtext('foto')
        self.bio = self.element.find('Biografia')
        self.norm_name = self.normalized_name()
    
    def _sanityTest(self):
        if self.dbid == None:
            raise Exception('Didn\'t find "id" attribute in %(tag)s element on line %(line)s of %(file)s Continuing to next...' % {'tag': self.element.tag,'line': self.element.sourceline, 'file': self.file_path})
    
    def process(self):
        logger = self.logger
        logger.info('Starting to ingest: %(class)s %(id)s' % {'class': type(self), 'id': self.dbid})
        
        try:
            logger.info('Checking to see if %s already exists in Fedora' % self.norm_name)
            pid = self[self.norm_name]
            logger.info('Found %(pid)s' % {'pid': pid})
            if pid:
                logger.warning('%(name)s already exists as pid %(pid)s! Overwriting DC DS!' % {'name': self.norm_name, 'pid': pid})
                self.composer = FedoraWrapper.client.getObject(pid)
            else:
                msg = 'Something went horribly wrong!  Found a pid (%(pid)s), but couldn\'t access it...' % {'pid': pid}
                logger.error(msg)
                raise Exception(msg)
        except KeyError:
            try:
                logger.debug('Not known by name, checking by composerID')
                pid = FedoraWrapper.getPid(uri=Composer.NS['fjm-db'].uri, predicate='composerID', obj="'%s'" % self.dbid)
                logger.info('Found %(pid)s' % {'pid': pid})
                if pid:
                    logger.warning('%(name)s already exists as pid %(pid)s! Overwriting DC DS!' % {'name': self.norm_name, 'pid': pid})
                    self.composer = FedoraWrapper.client.getObject(pid)
                else:
                    msg = 'Something went horribly wrong!  Found a pid (%(pid)s), but couldn\'t access it...' % {'pid': pid}
                    logger.error(msg)
                    raise Exception(msg)
            except KeyError:
                logger.info('Doesn\'t exist: creating a new Fedora Object')
                self.composer = FedoraWrapper.getNextObject(self.prefix, label='Composer %s' % self.dbid)

        rels_ext = FR.rels_ext(self.composer, namespaces=Composer.NS.values())
        rels = [
            (
                FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                FR.rels_object('atm:personCModel', FR.rels_object.PID)
            ),
            (
                FR.rels_predicate(alias='fjm-db', predicate='composerID'),
                FR.rels_object(self.dbid, FR.rels_object.LITERAL)
            )
        ]
        
        FedoraWrapper.addRelationshipsWithoutDup(rels, rels_ext=rels_ext).update()
        FedoraWrapper.correlateDBEntry('composedBy', 'composerID')
        
        #Yay Pythonic-ness?  Try to get an existing EAC-CPF, or create one if none is found
        try:
            eaccpf = CPF.EACCPF(self.composer.pid, xml=self.composer['EAC-CPF'].getContent().read())
            event_type="modified"
        except fcrepo.connection.FedoraConnectionException, e:
            if e.httpcode == 404:
                eaccpf = CPF.EACCPF(self.composer.pid)
                event_type="created"
            else:
                raise e
        eaccpf.add_maintenance_event(type=event_type, time="now", agent_type="machine", agent="atm_composer.py")
        eaccpf.add_XML_source(caption='XML from database dump', xml=self.element)
        eaccpf.add_name_entry(name=self.name)
        if len(self.bio) == 0:
            eaccpf.add_bio(bio=self.bio.text)
        else:
            eaccpf.add_bio(bio=self.bio)
        
        #Try to ensure that the image does not exist somehow?
        if self.photo:
            try:
                pid = FedoraWrapper.getPid(uri=Composer.NS['fjm-db'].uri, predicate='composerImageID', obj="'%s'" % self.dbid)
                photo = FedoraWrapper.client.getObject(pid)
            except KeyError:
                photo = FedoraWrapper.getNextObject(self.prefix, label='Image of composer %s' % self.dbid)

            p_rels_ext = FR.rels_ext(photo, namespaces=Composer.NS.values())
            
            photopath = self.getPath(self.photo)
            #TODO (very minor):  Might be a good idea to check whether or not the photo we're uploading is the same as that which is already in Fedora (use the hash or sommat?)
            if path.exists(photopath):
                FL.update_datastream(obj=photo, dsid='JPG', filename=photopath, mimeType='image/jpeg')
            else:
                logger.warning('Image %s specified, but does not seem to exist!' % self.photo)
            
            rels = [
                (
                    FR.rels_predicate(alias='fjm-db', predicate='composerImageID'),
                    FR.rels_object(self.dbid, FR.rels_object.LITERAL)
                ),
                (
                    FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                    FR.rels_object('atm:imageCModel', FR.rels_object.PID)
                ),
                (
                    FR.rels_predicate(alias='atm-rel', predicate='isImageOf'),
                    FR.rels_object(self.composer.pid, FR.rels_object.PID)
                ),
                (
                    FR.rels_predicate(alias='atm-rel', predicate='isIconOf'),
                    FR.rels_object(self.composer.pid, FR.rels_object.PID)
                )
            ]
            
            FedoraWrapper.addRelationshipsWithoutDup(rels, rels_ext=p_rels_ext).update()
            
            
            
            
        
        #Use the fcrepo implementation, as we're just passing a string of XML...
        self.composer.addDataStream(dsid='EAC-CPF', body='%s' % eaccpf, mimeType=unicode("text/xml"))
        dc = self.composer['DC']
        dc['title'] = [self.norm_name]
        dc.setContent()
        
        self[self.norm_name] = self.composer.pid
