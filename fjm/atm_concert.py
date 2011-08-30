#!/usr/bin/env python2.6
import logging
from FedoraWrapper import FedoraWrapper
from islandoraUtils import fedoraLib as FL
from islandoraUtils.metadata import fedora_relationships as FR
import os.path as path
from atm_object import atm_object as ao
import tempfile as TF

#Import ElementTree from somewhere
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

class Concert(ao):
    def __init__(self, file_path, element, prefix=ao.PREFIX):
        super(Concert, self).__init__(file_path, element, prefix, loggerName='ingest.XMLHandler.Concert')
        
        #Sanity test
        self.dbid = element.get('id_concierto')
        self._sanityTest()
    
    def _sanityTest(self):
        if self.dbid == None:
            raise Exception('Didn\'t find id attribute in %(tag)s element on line %(line)s of %(file)s Continuing to next...' % {'tab': self.element.tag,'line': self.element.sourceline, 'file': self.file_path})
    
    def __processConcert(self):
        logger = logging.getLogger('ingest.atm_concert.Concert.__processConcert')
        #Get the/an object
        try:
            pid = FedoraWrapper.getPid(uri=Concert.NS['fjm-db'].uri, predicate='concertID', obj="'%s'" % self.dbid)
            if pid:
                logger.warning('Concert %s found as %s.  Overwriting DSs!' % (self.dbid, pid))
                concert = FedoraWrapper.client.getObject(pid)
        except KeyError:
            concert = FedoraWrapper.getNextObject(prefix=self.prefix, label="concert %s" % self.dbid)
        
        #Dump the DB XML for the concert element into a tempfile before ingesting.
        #XXX: Had to set the bufsize to 32K, as the default of 4K (based on OS,
        #   really) resulted in the xml being truncated...  Doesn't grow, for 
        #   some reason.
        logger.info('Adding CustomXML datastream')
        with TF.NamedTemporaryFile(bufsize=32*1024, delete=True) as temp:
            etree.ElementTree(self.element).write(file=temp, 
                pretty_print=True, encoding='utf-8')
            temp.flush()
            if FL.update_datastream(obj=concert, dsid='CustomXML', 
                filename=temp.name, mimeType="text/xml"):
                logger.info('CustomXML added successfully')
            else:
                logger.error('Error while adding CustomXML!')
        
        #Ingest the WAV (if it exists...)
        WAV = self.element.findtext('Grabacion/wav')
        if WAV:
            WAV = self.getPath(WAV)
            if path.exists(WAV):
                FL.update_datastream(obj=concert, dsid='WAV', filename=WAV, 
                    label='WAV', mimeType="audio/x-wav")
            else:
                logger.warning('WAV file specified (%s), but does not exist!', WAV)
        else:
            logger.warning('No WAV found at %s!  Skipping...', WAV)
        
        #Ingest the MARCXML...  FIXME: Maybe this might not make sense to attempt, if there's no WAV?
        MARC = path.join(path.dirname(WAV), '%s.xml' % self.dbid)
        if path.exists(MARC):
            FL.update_datastream(obj=concert, dsid='MARCXML', mimeType="application/xml", filename=MARC)
            logger.debug('Added %s', MARC)
        else:
            logger.debug('Couldn\'t find MARCXML at %s', MARC)
        
        #Add relations to concert object
        rels_ext = FR.rels_ext(obj=concert, namespaces=ao.NS.values())
        rels = [
            #Don't know that this one is necessary...  Oh well...
            (
                FR.rels_predicate(alias='fjm-db', predicate='concertID'),
                FR.rels_object(self.dbid, FR.rels_object.LITERAL)
            ),
            (
                FR.rels_predicate(alias='fedora', predicate='isMemberOfCollection'),
                FR.rels_object('atm:concertCollection', FR.rels_object.PID)
            ),
            (
                FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                FR.rels_object('atm:concertCModel', FR.rels_object.PID)
            )
        ]
        
        #Write 'out' rels_ext
        FedoraWrapper.addRelationshipsWithoutDup(rels, rels_ext=rels_ext).update()
        
        desc = self.element.findtext('Descripcion')
        dc = concert['DC']
        dc['type'] = [unicode('Event')]
        if desc:
            dc['description'] = [unicode(desc)]
        dc.setContent()
        
        self.concert_obj = concert
        
    #TODO:  Deal with the bloody authors...  In the two (at least (Related to individual pieces, or the program as a whole)) different forms they are given.
    def __processProgram(self):
        p_el = self.element.find('programa')
        #Add the PDF to the program object
        filename = self.getPath(p_el.findtext('ruta'))
        
        if len(p_el) != 0:
            try:
                pid = FedoraWrapper.getPid(uri=Concert.NS['fjm-db'].uri, predicate='programConcertID', obj="'%s'" % self.dbid)
                program = FedoraWrapper.client.getObject(pid)
            except KeyError:
                #Get a Fedora Object for the program
                program = FedoraWrapper.getNextObject(self.prefix, 
                label='Program for concert %(dbid)s' % {'dbid': self.dbid})
        
            FL.update_datastream(obj=program, dsid='PDF', 
                filename=filename,
                mimeType='application/pdf'
            )
            
            #Add the MARCXML to the object...
            FL.update_datastream(obj=program, dsid='MARCXML', 
                filename=path.join(path.dirname(filename), self.dbid + '.xml'),
                mimeType='application/xml')
            
            #Create the RELS-EXT datastream
            rels_ext = FR.rels_ext(obj=program, namespaces=ao.NS.values())
            rels = [
                (
                    FR.rels_predicate(alias='fedora', predicate='isMemberOf'),
                    FR.rels_object(self.concert_obj.pid, FR.rels_object.PID)
                ),
                (
                    FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                    FR.rels_object('atm:programCModel', FR.rels_object.PID)
                ),
                (
                    FR.rels_predicate(alias='fjm-db', predicate='programConcertID'),
                    FR.rels_object(self.dbid, FR.rels_object.LITERAL)
                )
            ]
                
            #Get and add the titn to the RELS-EXT (might this make more sense in DC?...  It'd still end up in the triplestore...)
            titn = p_el.findtext('titn')
            if titn:
                rels.append(
                    (
                        FR.rels_predicate(alias='fjm-titn', predicate='program'),
                        FR.rels_object(titn, FR.rels_object.LITERAL)
                    )
                )
                
            for rel in rels:
                FedoraWrapper.addRelationshipWithoutDup(rel, rels_ext=rels_ext)
            rels_ext.update()
            
            #Update DC
            dc = program['DC']
            dc['type'] = [unicode('Text')]
            dc.setContent()
    
    def __processPerformance(self, p_el):
        logger = logging.getLogger('ingest.atm_concert.Concert.__processPerformance')
        p_dict = {
            'piece': p_el.get('id_obra'), 
            'concert': self.dbid, 
            'order': p_el.findtext('Posicion')
        }
        
        #TODO:  Bloody well deduplicate (ensure that this object does not already exist in Fedora)
        try:
            pid = FedoraWrapper.getPid(tuples=[
                (Concert.NS['fjm-db'].uri, 'basedOn', "'%s'" % p_dict['piece']), #Not sure if this is really necessary with the other two conditions...
                ('fedora-rels-ext:', 'isMemberOf', "<fedora:%s>" % self.concert_obj.pid), #To ensure that the performance actually belongs to this concert...
                (Concert.NS['atm-rel'].uri, 'concertOrder', "'%s'" % p_dict['order']) #To eliminate the confusion if the same piece is played twice in the same concert.
            ])
            if pid:
                performance = FedoraWrapper.client.getObject(pid)
        except KeyError:
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
        rels_ext = FR.rels_ext(obj=performance, namespaces=ao.NS.values())
        rels = [
            (
                FR.rels_predicate(alias='fedora', predicate='isMemberOf'),
                FR.rels_object(self.concert_obj.pid, FR.rels_object.PID)
            ),
            (
                FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                FR.rels_object('atm:performanceCModel', FR.rels_object.PID)
            ),
            (
                FR.rels_predicate(alias='atm-rel', predicate='concertOrder'),
                FR.rels_object(p_dict['order'], FR.rels_object.LITERAL)
            ),
            (
                FR.rels_predicate(alias='fjm-db', predicate='basedOn'),
                FR.rels_object(p_dict['piece'], FR.rels_object.LITERAL)
            )
        ]
        
        FedoraWrapper.addRelationshipsWithoutDup(rels, rels_ext=rels_ext)
        
        rels_ext.update()
        
        #Create objects for any movements within the piece
        for m_el in p_el.findall('Movimiento/Movimiento'):
            m_dict = {
                'concert': p_dict['concert'],
                'piece': p_dict['piece'],
                'id': m_el.get('id'),
                'corder': p_dict['order'],
                'porder': m_el.get('posicion'),
                'name': m_el.findtext('NOMBRE'),
                'MP3': m_el.findtext('mp3_Movimiento'),
                'line': m_el.sourceline,
                'file': self.file_name
            }
            
            #Sanity test
            if m_dict['order']:
                #Get a Fedora Object for this movement
                try:
                    pid = FedoraWrapper.getPid(tuples=[
                        ('fedora:', 'isMemberOf', performance.pid),
                        ('fedora-model:', 'hasModel', 'atm:movementCModel'),
                        (Concert.NS['atm-rel'].uri, 'pieceOrder', m_dict['porder'])
                    ])
                    mov = FedoraWrapper.client.getObject(pid)
                except KeyError:
                    mov = FedoraWrapper.getNextObject(self.prefix, label='Movement: %(concert)s/%(piece)s/%(id)s' % m_dict)
                
                #Get DC and set the title if we have a name.
                mov_dc = mov['DC']
                mov_dc['type'] = [unicode('Event')]
                if m_dict['name']:
                    mov_dc['title'] = [unicode(m_dict['name'])]
                mov_dc.setContent()
                
                #Set the three required relations:
                #1 - To the performance
                #2 - To the content model
                #3 - The order this movement occurs within the piece
                m_rels_ext = FR.rels_ext(obj=mov, namespaces=NS.values())
                m_rels = [
                    (
                        FR.rels_predicate(alias='fedora', predicate='isMemberOf'),
                        FR.rels_object(performance.pid, FR.rels_object.PID)
                    ),
                    (
                        FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                        FR.rels_object('atm:movementCModel', FR.rels_object.PID)
                    ),
                    (
                        FR.rels_predicate(alias='atm-rel', predicate='pieceOrder'),
                        FR.rels_object(m_dict['order'], FR.rels_object.LITERAL)
                    )
                ]
                
                FedoraWrapper.addRelationshipsWithoutDup(m_rels, rels_ext=m_rels_ext)
                m_rels_ext.update()
                
                #Add the MP3 (if it exists)
                if m_dict['MP3']:
                    mp3_path = self.getPath(m_dict['MP3'])
                    if path.exists(mp3_path):
                        FL.update_datastream(obj=mov, dsid='MP3', 
                            filename=mp3_path, mimeType='audio/mpeg')
                    else:
                        logger.warning("MP3 entry for movement %(id)s in performance of %(piece)s in %(concert)s on line %(line)s of %(file)s" % m_dict)
                else:
                    logger.debug('No movement MP3 for %(concert)s/%(piece)s/%(id)s on line %(line)s of %(file)s' % m_dict)
            else:
                logger.error('Movement %(concert)s/%(piece)s/%(id)s does not have a position near line %(line)s of %(file)s!' % m_dict)
        #Done with movements
                
        #Create objects for the performers.
        for per_el in p_el.findall('Interpretes/Interprete'):
            perf = {
                'id': per_el.get('id'),
                'group': per_el.get('id_grupo', default=None),
                'line': per_el.sourceline,
                'file': self.file_name
            }
            perf.update(p_dict)
                
            if perf['id']:
                rels = [
                    #Relate performer to CModel
                    (
                        FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                        FR.rels_object('atm:performerCModel', FR.rels_object.PID)
                    ),
                    #Relate performer to performance
                    (
                        FR.rels_predicate(alias='atm-rel', predicate='performance'),
                        FR.rels_object(performance.pid, FR.rels_object.PID)
                    ),
                    #Relate perfomer to their 'person' entry
                    (
                        FR.rels_predicate(alias='fjm-db', predicate='player'),
                        FR.rels_object(perf['id'], FR.rels_object.LITERAL)
                    )
                ]
                
                try:
                    t_list = list()
                    for pred, obj in rels:
                        if obj.type == FR.rels_object.LITERAL:
                            t_obj = "'%s'" % obj
                        else:
                            t_obj = "<fedora:%s>" % obj
                        t_list.append(("%s" % Concert.NS[pred.alias].uri, "%s" % pred.predicate, "%s" % t_obj))
                        
                    pid = FedoraWrapper.getPid(tuples=t_list)
                    if pid:
                        performer = FedoraWrapper.client.getObject(pid)
                except KeyError:
                    performer = FedoraWrapper.getNextObject(prefix = self.prefix, label = 'Performer: %(concert)s/%(piece)s/%(id)s in group %(group)s' % perf)
                    
                #Relate the performer to the listed group (or 'unaffiliated, if none)
                if perf['group'] != None:
                    rels.append(
                        (
                            FR.rels_predicate(alias='fjm-db', predicate='group'),
                            FR.rels_object(perf['group'], FR.rels_object.LITERAL)
                        )
                    )
                else:
                    rels.append(
                        (
                            FR.rels_predicate(alias='atm-rel', predicate='group'),
                            FR.rels_object('atm:unaffiliatedPerfomer', FR.rels_object.PID)
                        )
                    )
                        
                for i_el in per_el.findall('Instrumentos/Instrumento'):
                    rels.append(
                        (
                            FR.rels_predicate(alias='fjm-db', predicate='instrument'),
                            FR.rels_object(i_el.get('id'), FR.rels_object.LITERAL)
                        )
                    )
                    
                FedoraWrapper.addRelationshipsWithoutDup(rels, fedora=performer).update()
            else:
                logger.error("Performer on line %(line)s of %(file)s does not have an ID!" % perf)
    
    #NOTE: Currently ignoring "Foto_principal" element, where it occurs.
    def __processImages(self, iEl):
        logger = logging.getLogger('ingest.atm_concert.Concert.__processImages')
        firstImage = True
        for el in iEl.findall('Foto'):
            i_dict = {
                'id': el.get('id', default=None),
                'path': el.findtext('ruta'),
                'description': el.findtext('pie'),
                'line': el.sourceline
            }
            if i_dict['id'] and i_dict['path'] and path.exists(self.getPath(i_dict['path'])):
                #Get or create an new image containing the new object.
                try:
                    image = FedoraWrapper.client.getObject(FedoraWrapper.getPid(uri=ao.NS['fjm-db'].uri, predicate='imageID', obj="'%(id)s'" % i_dict))
                except KeyError:
                    image = FedoraWrapper.getNextObject(self.prefix, label='Image: %(id)s' % i_dict)
                    
                #FIXME:  Detect Mimetype, and create image accordingly?
                FL.update_datastream(obj=image, dsid="JPG", filename=self.getPath(i_dict['path']), mimeType="image/jpeg")
                    
                i_rels_ext = FR.rels_ext(obj=image, namespaces=ao.NS.values())
                    
                rels = [
                    (
                        FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                        FR.rels_object('atm:imageCModel', FR.rels_object.PID)
                    ),
                    (
                        FR.rels_predicate(alias='fjm-db', predicate='imageID'),
                        FR.rels_object(el.get('id'), FR.rels_object.LITERAL)
                    ),
                    #Relate the image to the concert as a general image...
                    (
                        FR.rels_predicate(alias='atm-rel', predicate='isImageOf'),
                        FR.rels_object(self.concert_obj.pid, FR.rels_object.PID)
                    )
                ]
                
                #Set the first image as the "primary" (Used for thumbnails)
                if firstImage:
                    firstImage = False
                    rels.append(
                        (
                            FR.rels_predicate(alias='atm-rel', predicate='isIconOf'),
                            FR.rels_object(self.concert_obj.pid, FR.rels_object.PID)
                        )
                    )
                
                #Update and commit the rels_ext
                FedoraWrapper.addRelationshipsWithoutDup(rels, rels_ext=i_rels_ext).update()
                
                dc = image['DC']
                dc['type'] = [unicode('StillImage')]
                #Add a description, based on the 'pie' (if it exists, and there isn't already on for the image...), and don't clobber any existing description...
                if i_dict['description'] and 'description' not in dc:
                    dc['description'] = [unicode('%(description)s' % i_dict)]
                dc.setContent()
            else:
                logger.warning('No ID or invalid path for image at line: %(line)s' % i_dict)
                break
    
    def __processConferences(self, cEl):
        for el in self.element.findall('Evento_Asociado'):
            e_dict = {
                'id': el.get('id'),
                'type': el.findtext('Tipo'),
                'description': el.findtext('descripcion'),
                'mp3_path': el.findtext('ruta'),
                'concert': self.dbid
            }
            
            if e_dict['id']:
                try:
                    pid = FedoraWrapper.getPid(uri=Concert.NS['fjm-db'].uri, predicate="lectureID", object="'%(id)s'" % e_dict)
                    conference = FedoraWrapper.client.getObject(pid)
                except KeyError:
                    conference = FedoraWrapper.getNextObject(self.prefix, label="%(type)s %(id)s in %(concert)s" % e_dict)
                    
                c_rels_ext = FR.rels_ext(obj=conference, namespaces=ao.NS.values())
                
                rels = [
                    (
                        FR.rels_predicate(alias='fedora', predicate='isMemberOf'),
                        FR.rels_object(self.concert_obj.pid, FR.rels_object.PID)
                    ),
                    (
                        FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                        FR.rels_object('atm:lectureCModel', FR.rels_object.PID)
                    ),
                    (
                        FR.rels_predicate(alias='fjm-db', predicate='lectureID'),
                        FR.rels_object(e_dict['id'], FR.rels_object.LITERAL)
                    )
                ]
                

                FedoraWrapper.addRelationshipsWithoutDup(rels, rels_ext=c_rels_ext)
                
                c_rels_ext.update()
                
                FL.update_datastream(obj=conference, dsid='MP3', filename=self.getPath(e_dict['mp3_path']), mimeType="audio/mpeg")
                
                dc = conference['DC']
                dc['type'] = [unicode('Sound')]
                dc['description'] = [unicode(e_dict['description'])]
                dc['subject'] = [unicode(e_dict['type'])]
                dc.setContent()
                
                    
    def process(self):
        logger = self.logger
        logger.info('Starting to ingest: %s', self.dbid)

        # Create concert object...
        # Ensure this concert does not already exist in Fedora before
        #   getting the object for a new one.
        self.__processConcert()
        
        #Create program object...
        self.__processProgram()
           
        #Create performance(s) and performer(s)
        for el in self.element.findall('Obras/Obra'):
            self.__processPerformance(el)
            #pass
            
        #Add photos
        self.__processImages(self.element.find('Fotos'))
           
        #Add lectures and stuff...
        self.__processConferences(self.element.find('Eventos_Asociados'))

        logger.info('Done ingesting: %s', self.dbid)
        
def __init__():
    pass

