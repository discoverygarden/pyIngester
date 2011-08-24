
import logging
from islandoraUtils.metadata import fedora_relationships as FR, eaccpf as CPF
from islandoraUtils import fedoraLib as FL
from atm_object import atm_object as ao
from FedoraWrapper import FedoraWrapper
from tempfile import NamedTemporaryFile

class Performer(ao):
    def __init__(self, file_path, element, prefix=ao.PREFIX):
        super(Score, self).__init__(file_path, element, prefix, loggerName='ingest.XMLHandler.atm_player')
        
        self.dbid = element.get('id')
    
    def _sanityTest(self):
        if self.dbid == None:
            raise Exception('Didn\'t find "id" attribute in %(tag)s element on line %(line)s of %(file)s Continuing to next...' % {'tab': self.element.tag,'line': self.element.sourceline, 'file': self.file_path})
    
    def process(self):
        logger = self.logger
        logger.info('Starting to ingest: Groupo %s' % self.dbid)
        
        try:
            pid = FedoraWrapper.getPid(uri=ao.NS['fjm-db'].uri, predicate='groupID', obj="'%s'" % self.dbid)
            if pid:
                logger.warning('Group %(id)s already exists as pid %(pid)s! Overwriting DC DS!' % {'id': self.dbid, 'pid': pid})
                group = FedoraWrapper.client.getObject(pid)
            else:
                raise Exception('Something went horribly wrong!  Found a pid, but couldn\'t access it...')
        except KeyError:
            group = FedoraWrapper.getNextObject(self.prefix, label='Group %s' % self.dbid)
            rels_ext = FR.rels_ext(group, namespaces=ao.NS.values())
            rels_ext.addRelationship(
                FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                FR.rels_object('atm:groupCModel', FR.rels_object.PID))
            rels_ext.addRelationship(
                FR.rels_predicate(alias='fjm-db', predicate='groupID'),
                FR.rels_object(self.dbid, FR.rels_object.LITERAL))
            rels_ext.update()
            
            eaccpf = CPF.EACCPF(group.pid)
            eaccpf.add_XML_source(caption='XML from database dump', xml=self.element)
            eaccpf.add_name_entry(name={'forename': self.element.findtext('nombre'), 'surname': self.element.findtext('apellidos')})
            eaccpf.add_maintenance_event(type="created", time="now", agent_type="machine", agent="atm_player.py")
            with NamedTemporaryFile(bufsize=64*2**10, delete=True) as temp:
                temp.write('%s' % eaccpf)
                FL.update_datastream(obj=group, dsid="EAC-CPF", label='EAC-CPF Metadata', filename=temp.name, mimeType="text/xml")
        finally:
            dc = group['DC']
            dc['type'] = [unicode('Collection')]
            dc['title'] = [self.element.findtext('groupo').strip()]
            dc.setContent()
            