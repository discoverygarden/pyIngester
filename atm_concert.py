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
        #   some reason
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
        scoreID = p_el.get('id_obra')
        p_dict = {'piece': scoreID, 'concert': self.dbid}
        performance = FedoraWrapper.getNextObject(self.prefix, label='Performance of %(piece)s in %(concert)s' % p_dict)
        
        #Add MP3 to performance (if there is one to add)
        p_mp3 = p_el.find('mp3_Obra')
        if p_mp3:
            mp3_path = self.getPath(p_mp3)
            if path.exists(mp3_path):
                FL.update_datastream(obj=performance, dsid='MP3', 
                    filename=mp3_path, mimeType='audio/mpeg')
            else:
                logger.warning('MP3 entry for performance of %(piece)s in concert %(concert)s, but the file does not exist!' % p_dict)
        else:
            logger.debug('No performance MP3 for %(concert)s/%(piece)s' % p_dict)
        
        
    
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
        self.performances = dict()
        for el in self.element.findall('Obras/Obra'):
            self.__processPerformance(el)
            position = el.findtext('Posicion')
            perf = dict()
            
            perf['id'] = el.get('id_obra')
            mp3 = el.findtext('mp3_Obra')
            if mp3 != None:
                perf['mp3'] = self.getPath(mp3)
                
            perfs = dict()
            perfs['performers'] = dict()
            for p_el in el.findall('Interpretes/Interprete'):
                person = dict()
                try:
                    person_id = p_el.get('id')
                    if person_id == None:
                        raise Exception('Person doesn\'t have a id in %(filename)s at line %(line)i' % {'filename': self.file_name, 'line': p_el.sourceline})
                    
                    group = p_el.get('id_groupo')
                    if group != None:
                        person['group'] = group
                        
                    insts = set()
                    for inst_el in p_el.findall('Instrumentos/Instrumento'):
                        inst = inst_el.get('id')
                        if inst != None: insts.add(inst)
                    if insts: 
                        person['insts'] = insts
                    else:
                        raise Exception('No instrument in %(filename)s at line %(line)i' % 
                            {"filename": self.file_name, "line": inst_el.sourceline})
                except Exception, e:
                    logger.warning('%s', e)
                else:
                    perfs['performers'][person_id] = person
                    
            perf['movements'] = dict()
            for mov_el in el.findall('Movimientos/Movimiento'):
                mov = dict()
                mov['position'] = mov_el.get('posicion')
                mov['id'] = mov_el.get('id')
                mov['title'] = mov_el.findtext('NOMBRE')
                mov_mp3 = mov_el.findtext('mp3_Movimiento')
                if mov_mp3 != None:
                    mov['mp3'] = self.getPath(mov_mp3)
                perf['movements'][mov['position']] = mov
            self.performances[position] = perf
        
        logger.debug('Performances: %s', self.performances)
        logger.info('Done ingesting: %s', self.dbid)

