import logging
import os.path as path
from islandoraUtils.metadata import fedora_relationships as FR
from tempfile import NamedTemporaryFile as NTF
from islandoraUtils.fedoraLib import update_datastream

try:
    from lxml import etree
    logging.debug("running with lxml.etree")
except ImportError:
    try:
        # Python 2.5
        import xml.etree.cElementTree as etree
        logging.debug("running with cElementTree on Python 2.5+")
    except ImportError:
        try:
            # Python 2.5
            import xml.etree.ElementTree as etree
            logging.debug("running with ElementTree on Python 2.5+")
        except ImportError:
            try:
                # normal cElementTree install
                import cElementTree as etree
                logging.debug("running with cElementTree")
            except ImportError:
                try:
                    # normal ElementTree install
                    import elementtree.ElementTree as etree
                    logging.debug("running with ElementTree")
                except ImportError:
                    message = "Failed to import ElementTree from any known place"
                    logging.critical(message)
                    raise ImportError(message)


class atm_object(object):
    PREFIX="atm"
    NS={
        'fjm-db': FR.rels_namespace('fjm-db', 'http://digital.march.es/db#'),
        'fjm-titn': FR.rels_namespace('fjm-titn', 'http://digital.march.es/titn#'),
        #'atm': FR.rels_namespace('atm', 'http://digital.march.es/atmusica/fedora/rdf'),
        'atm-rel': FR.rels_namespace('atm-rel', 'http://digital.march.es/atmusica#'),
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
        Explode each item in the iterable 'nameparts', implode with a single space separating tokens
        '''
        name = list()
        for namepart in nameparts:
            try:
                name.extend(namepart.split())
            except:
                logging.getLogger('ingest.atm_object.normalize_name').error("Bad object input: %s", namepart)
        return ' '.join(name)

    @staticmethod
    def save_dc(fobj, dc_dict):
        nsmap = {'dc': 'http://purl.org/dc/elements/1.1/',
                     'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/'}
        dc_dict['identifier'] = [fobj.pid]
        doc = etree.Element('{%s}dc' % nsmap['oai_dc'], nsmap=nsmap)
        for key, values in dc_dict.items():
            for value in values:
                etree.SubElement(doc, '{%s}%s' % (nsmap['dc'], key)).text = value
        return atm_object.save_etree(fobj, doc, 'DC', 'Dublin Core', controlGroup='M', hash='SHA-1')
            
    @staticmethod
    def save_etree(fobj, element, dsid, label, mimeType='text/xml', controlGroup='M', hash='SHA-1'):
        with NTF() as temp:
            etree.ElementTree(element=element).write(temp, encoding="UTF-8", pretty_print=True)
            temp.flush()
            
            return update_datastream(fobj, dsid, temp.name, label=label, mimeType=mimeType, controlGroup=controlGroup, checksumType=hash)
