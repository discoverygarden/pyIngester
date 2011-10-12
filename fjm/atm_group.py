
import logging
from islandoraUtils.metadata import fedora_relationships as FR
from atm_object import atm_object as ao
from FedoraWrapper import FedoraWrapper

class Group(ao):
    def __init__(self, file_path, element, prefix=ao.PREFIX):
        super(Group, self).__init__(file_path, element, prefix, loggerName='ingest.XMLHandler.atm_group')
        
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
        rels = [
            (
                FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                FR.rels_object('atm:groupCModel', FR.rels_object.PID)
            ),
            (
                FR.rels_predicate(alias='fjm-db', predicate='groupID'),
                FR.rels_object(self.dbid, FR.rels_object.LITERAL)
            )
        ]
        
        FedoraWrapper.addRelationshipsWithoutDup(rels, rels_ext=rels_ext).update()

        dc = dict()
        dc['type'] = [unicode('Collection')]
        dc['title'] = [self.element.findtext('grupo').strip()]
        Group.save_dc(group, dc)
        
        FedoraWrapper.correlateDBEntry('group', 'groupID')
        group.state = unicode('A')
            