import logging
import os.path as path
from islandoraUtils.metadata import fedora_relationships as FR

class atm_object(object):
    PREFIX="atm-test"
    NS={
        'fjm-db': FR.rels_namespace('fjm-db', 'http://digital.march.es/db#'),
        'fjm-titn': FR.rels_namespace('fjm-titn', 'http://digital.march.es/titn#'),
        #'atm': FR.rels_namespace('atm', 'http://digital.march.es/atmusica/fedora/rdf'),
        'atm-rel': FR.rels_namespace('atm-rel', 'http://www.example.org/dummy#'),
        'fedora-model': FR.rels_namespace('fedora-model', 'info:fedora/fedora-system:def/model#'),
        'atm': FR.rels_namespace('atm', 'fedora:atm:'),
        'fedora-view': FR.rels_namespace('fedora-view', 'info:fedora/fedora-system:def/view#'),
        'fedora-rels-ext': FR.rels_namespace('fedora-rels-ext', 'info:fedora/fedora-system:def/relations-external#')
    }
    
    #atm:{person,concert,program,group}CModel should be active after initial population 
    RELATIONSHIPS={
        'atm:performerCModel': [
            ('atm-rel:player', 'AnY'),
            ('atm-rel:instrument', 'AnY'),
            ('atm-rel:group', 'AnY')
        ],
        'atm:performanceCModel': [
            ('atm-rel:basedOn', 'AnY')
        ],
        'atm:scoreCModel': [
            ('atm-rel:composedBy', 'AnY')
        ],
        'atm:imageCModel': [
            ('fedora-view:disseminates', 'fedora:')
        ]
    }

    def __init__(self, file_path, element, prefix=PREFIX, loggerName='ingest.XMLHandler.atm_object'):
        self.logger = logging.getLogger(loggerName)
        self.logger.debug('Start')
        
        self.prefix = prefix
        self.path = path.dirname(path.abspath(file_path))
        self.file_name = file_path
        self.element = element
        
    def getPath(self, filename):
        if filename and path.isabs(filename):  #Make the path relative...
            filename = ".%(filename)s" % {'filename': filename} 
        elif filename:
            pass
        else:
            raise AttributeError('filename is not set')
            
        return path.normpath(path.join(self.path, filename))
        
    @staticmethod
    def normalize_name(nameparts):
        '''
        Explode each item in the iterable 'nameparts', implode with a single space separating tokens, and apply Title Casing
        '''
        name = list()
        for namepart in nameparts:
            try:
                name.extend(namepart.split())
            except:
                logging.getLogger('ingest.atm_object.normalize_name').error("Bad object input: %s", namepart)
        return ' '.join(name).title()