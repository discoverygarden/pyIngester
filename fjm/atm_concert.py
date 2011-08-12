#!/usr/bin/env python2.6
import logging
from FedoraWrapper import FedoraWrapper
import os.path as path
from ObjectBuilder import ObjectBuilder
from islandoraUtils import fedoraLib as FL, fedora_relationships as FR
import tempfile as TF

PREFIX="test"
NS=[
    FR.rels_namespace('fjm-db', 'http://digital.march.es/db#'),
    FR.rels_namespace('fjm-titn', 'http://digital.march.es/titn#'),
    #FR.rels_namespace('atm', 'http://digital.march.es/atmusica/fedora/rdf'),
    FR.rels_namespace('atm-rel', 'http://www.example.org/dummy#'),
    FR.rels_namespace('fedora-model', 'info:fedora/fedora-system:def/model#')
]

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
                    loging.critical(message)
                    raise ImportError(message)

class Concert:
    def __init__(self, file_path, element, prefix=PREFIX):
        self.logger = logging.getLogger('ingest.XMLHandler.atm_concert')
        self.logger.debug('Start')
        
        #Sanity test
        self.dbid = element.get('id_concierto')
        if self.dbid == None:
            raise Exception('Didn\'t find "id_concierto" attribute in concert ' + 
                'element!  Continuing to next...')
        else:
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
    
    def __processConcert(self):
        logger = logging.getLogger('ingest.atm_concert.Concert.__processConcert')
        #Get an object (with a new PID)
        logger.debug('Getting FedoraObject...')
        concert = FedoraWrapper.getNextObject(prefix=self.prefix, label="concert %s" % self.dbid)
        
        #Dump the DB XML for the concert element into a tempfile before ingesting.
        #XXX: Had to set the bufsize to 32K, as the default of 4K (based on OS,
        #   really) resulted in the xml being truncated...  Doesn't grow, for 
        #   some reason.
        logger.info('Adding CustomXML datastream')
        with TF.NamedTemporaryFile(bufsize=32*1024,delete=True) as temp:
            etree.ElementTree(self.element).write(file=temp, 
                pretty_print=True, encoding='utf-8')
            temp.flush()
            if FL.update_datastream(obj=concert, dsid='CustomXML', 
                filename=temp.name, mimeType="text/xml"):
                logger.info('CustomXML added successfully')
            else:
                logger.error('Error while adding CustomXML!')
        
        #Ingest the WAV (if it exists...)
        WAV = self.getPath(self.element.findtext('Grabacion/wav'))
        if WAV:
            if path.exists(WAV):
                if FL.update_datastream(obj=concert, dsid='WAV', filename=WAV,
                    label='WAV', mimeType="audio/x-wav"):
                    logger.info('Added WAV datastream: ')
                else:
                    logger.error('Error adding WAV!')
            else:
                logger.warning('WAV file specified (%s), but does not exist!', WAV)
        else:
            logger.warning('No WAV found at %s!  Skipping...', WAV)
        
        #Ingest the MARCXML
        FL.update_datastream(obj=concert, dsid='MARCXML', mimeType="application/xml",
            filename=path.join(path.dirname(WAV), self.dbid + '.xml'))
        
        #Add relations to concert object
        rels_ext = FR.rels_ext(obj=concert, namespaces=NS)
        rels_ext.addRelationship(
            FR.rels_predicate(alias='fjm-db', predicate='concertID'),
            FR.rels_object(self.dbid, FR.rels_object.LITERAL))
        rels_ext.addRelationship(
            FR.rels_predicate(alias='fedora', predicate='isMemberOfCollection'),
            FR.rels_object('atm:concertCollection', FR.rels_object.PID))
        rels_ext.addRelationship(
            FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
            FR.rels_object('atm:concertCModel', FR.rels_object.PID))
        
        #Write 'out' rels_ext
        rels_ext.update()
        
        self.concert_obj = concert
        
    def __processProgram(self):
        #Get a Fedora Object for the program
        program = FedoraWrapper.getNextObject(self.prefix, 
        label='Program for concert %(dbid)s' % {'dbid': self.dbid})
        
        p_el = self.element.find('programa')
        #Add the PDF to the program object
        filename = self.getPath(p_el.findtext('ruta'))
        
        FL.update_datastream(obj=program, dsid='PDF', 
            filename=filename,
            mimeType='application/pdf'
        )
        
        #TODO: Add into RELS-EXT?
        titn = p_el.findtext('titn')
        
        #Add the MARCXML to the object...
        FL.update_datastream(obj=program, dsid='MARCXML', 
            filename=path.join(path.dirname(filename), self.dbid + '.xml'),
            mimeType='application/xml')
        
        #Create the RELS-EXT datastream
        rels_ext = FR.rels_ext(obj=program, namespaces=NS)
        rels_ext.addRelationship(
            FR.rels_predicate(alias='fedora', predicate='isMemberOf'),
            FR.rels_object(self.concert_obj.pid, FR.rels_object.PID))
        rels_ext.addRelationship(
            FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
            FR.rels_object('atm:programCModel', FR.rels_object.PID))
        rels_ext.update()
    
    def __processPerformance(self, p_el):
        logger = logging.getLogger()
        p_dict = {
            'piece': p_el.get('id_obra'), 
            'concert': self.dbid, 
            'order': p_el.findtext('Posicion')
        }
        
        performance = FedoraWrapper.getNextObject(self.prefix, label='Performance of %(piece)s in %(concert)s' % p_dict)
        
        #Add MP3 to performance (if there is one to add)
        p_mp3 = p_el.findtext('mp3_Obra')
        if p_mp3:
            mp3_path = self.getPath(p_mp3)
            if path.exists(mp3_path):
                FL.update_datastream(obj=performance, dsid='MP3', 
                    filename=mp3_path, mimeType='audio/mpeg')
            else:
                logger.warning('MP3 entry for performance of %(piece)s in concert %(concert)s, but the file does not exist!' % p_dict)
        else:
            logger.debug('No performance MP3 for %(concert)s/%(piece)s' % p_dict)
        
        #Add relationships
        #1  - To concert
        #2  - To score
        #3  - To CM
        #4  - Position in concert
        rels_ext = FR.rels_ext(obj=performance, namespaces=NS)
        rels_ext.addRelationship(
            FR.rels_predicate(alias='fedora', predicate='isMemberOf'),
            FR.rels_object(self.concert_obj.pid, FR.rels_object.PID))
        rels_ext.addRelationship(
            FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
            FR.rels_object('atm:performanceCModel', FR.rels_object.PID))
        rels_ext.addRelationship(
            FR.rels_predicate(alias='atm_rel', predicate='concertOrder'),
            FR.rels_object(p_dict['order'], FR.rels_object.LITERAL))
        #TODO:  Check if score exists and relate to it; otherwise (as is 
        #   currently happening), create a literal against the database id of 
        #   the score
        q_dict = {
            'uri': NS['fjm-db'],
            'predicate': 'scoreID',
            'id': p_dict['piece']
        }
        result = performance.client.searchTriples(
            'select $score from <#ri>' +
            'where $score <%(uri)s%(predicate)s> \'%(id)s\'' % q_dict
        )
        try:
            uri = result['score']['value']
            prefix = 'info:fedora/'
            if uri.startswith(prefix):
                uri = result[len(prefix):]
            rels_ext.addRelationship(
                FR.rels_predicate(alias='atm_rel', predicate='basedOn'),
                FR.rels_object(, FR.rels_object.PID))
        except KeyError:
            rels_ext.addRelationship(
                FR.rels_predicate(alias='fjm-db', predicate='basedOn'),
                FR.rels_object(p_dict['piece'], FR.rels_object.LITERAL))
        rels_ext.update()
        
        #Create objects for any movements within the piece
        for m_el in p_el.findall('Movimiento/Movimiento'):
            m_dict = {
                'concert': p_dict['concert'],
                'piece': p_dict['piece']
                'id': m_el.get('id'),
                'corder': p_dict['order'],
                'porder': m_el.get('posicion'),
                'name': m_el.findtext('NOMBRE'),
                'MP3': m_el.findtext('mp3_Movimiento')
            }
            
            #Sanity test
            if m_dict['order']:
                #Get a Fedora Object for this movement
                mov = FedoraWrapper.getNextObject(self.prefix, 
                    label='Movement: %(concert)s/%(piece)s/%(id)s' % m_dict)
                
                #Get DC and set the title if we have a name.
                if m_dict['name']:
                    mov_dc = mov.get('DC')
                    mov_dc.set('title', m_dict['name'])
                    mov_dc.setContent()
                
                #Set the three required relations:
                #1 - To the performance
                #2 - To the content model
                #3 - The order this movement occurs within the piece
                m_rels_ext = FR.rels_ext(obj=mov, namespaces=NS)
                m_rels_ext.addRelationship(
                    FR.rels_predicate(alias='fedora', predicate='isMemberOf'),
                    FR.rels_object(performance.pid, FR.rels_object.PID))
                m_rels_ext.addRelationship(
                    FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                    FR.rels_object('atm:movementCModel', FR.rels_object.PID))
                m_rels_ext.addRelationship(
                    FR.rels_predicate(alias='atm_rel', predicate='pieceOrder'),
                    FR.rels_object(m_dict['order'], FR.rels_object.LITERAL))
                m_rels_ext.update()
                
                #Add the MP3 (if it exists)
                if m_dict['MP3']:
                    mp3_path = self.getPath(m_dict['MP3'])
                    if path.exists(mp3_path):
                        FL.update_datastream(obj=mov, dsid='MP3', 
                            filename=mp3_path, mimeType='audio/mpeg')
                    else:
                        logger.warning("MP3 entry for movement %(id)s in performance %(piece)s in %(concert)s" % m_dict)
                else:
                    logger.debug('No movement MP3 for %(concert)s/%(piece)s/%(id)s' % m_dict)
            else:
                logger.error('Movement %(concert)s/%(piece)s/%(id)s does not have a position!')
                
    
    def process(self):
        logger = self.logger
        logger.info('Starting to ingest: %s', self.dbid)
        #TODO: processing
        #TODO:  Create concert object...
        #TODO:  Ensure this concert does not already exist in Fedora before
        #   getting the object for a new one.
        self.__processConcert()
        
        
        #TODO: Create program object...
        self.__processProgram()
           
        #TODO: Create performer objects
        #TODO: Create performance objects
        for el in self.element.findall('Obras/Obra'):
            self.__processPerformance(el)

        logger.info('Done ingesting: %s', self.dbid)
        
def __init__():
    pass

