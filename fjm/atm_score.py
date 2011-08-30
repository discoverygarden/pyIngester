
import logging
from islandoraUtils import fedoraLib as FL
from islandoraUtils.metadata import fedora_relationships as FR
from atm_object import atm_object as ao
from FedoraWrapper import FedoraWrapper
import os.path as path

class Score(ao):
    def __init__(self, file_path, element, prefix=ao.PREFIX):
        super(Score, self).__init__(file_path, element, prefix, loggerName='ingest.XMLHandler.atm_score')
        
        self.dbid = element.get('id')
    
    def _sanityTest(self):
        if self.dbid == None:
            raise Exception('Didn\'t find "id" attribute in obra element on line %(line)s of %(file)s Continuing to next...' % {'line': self.element.sourceline, 'file': self.file_path})
    
    def process(self):
        logger = self.logger
        logger.info('Starting to ingest: Score %s' % self.dbid)
        
        try:
            pid = FedoraWrapper.getPid(uri=ao.NS['fjm-db'].uri, predicate='scoreID', obj="'%s'" % self.dbid)
            if pid:
                logger.warning('Score %(id)s already exists as pid %(pid)s! Overwriting PDF and DC DSs!' % {'id': self.dbid, 'pid': pid})
                score = FedoraWrapper.client.getObject(pid)
            else:
                raise Exception('Something went horribly wrong!  Found a pid, but couldn\'t access it...')
        except KeyError:
            score = FedoraWrapper.getNextObject(self.prefix, label='Score %s' % self.dbid)
            
        rels_ext = FR.rels_ext(score, namespaces=ao.NS.values())
        rels = [
            (
                FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                FR.rels_object('atm:scoreCModel', FR.rels_object.PID)
            ),
            (
                FR.rels_predicate(alias='fjm-db', predicate='scoreID'),
                FR.rels_object(self.dbid, FR.rels_object.LITERAL)
            )
        ]
        
        titn = self.element.findtext('titn_partitura')
        if titn:
            rels.append(
                (
                    FR.rels_predicate(alias='fjm-titn', predicate='score'),
                    FR.rels_object(titn, FR.rels_object.LITERAL)
                )
            )
        #FIXME:  'Direction' of composer relation...  Should I go from the score to the composer, or (as I think I do in my hand-made objects) from the composer to the score...  Or should I make the relationships go in both directions?
        composer = self.element.findtext('ID_COMPOSITOR')
        if composer:
            rels.append(
                (
                    FR.rels_predicate(alias='fjm-db', predicate='composedBy'),
                    FR.rels_object(composer, FR.rels_object.LITERAL)
                )
            )
            
        for rel in rels:
            FedoraWrapper.addRelationshipWithoutDup(rel, rels_ext=rels_ext)
        rels_ext.update()
        
        filename = self.element.findtext('Ruta_Partitura')
        if filename:
            fn = self.getPath(filename)
            if path.exists(fn):
                FL.update_datastream(obj=score, dsid='PDF', label="Score PDF", filename=fn, mimeType='application/pdf')
            else:
                logger.error('PDF specified for score %(id)s, but file does not seem to exist!' % {'id': self.dbid})
                
            marc = self.getPath(path.join(path.dirname(filename), '%s.xml' % self.dbid))
            if path.exists(marc):
                FL.update_datastream(obj=score, dsid='MARCXML', label="MARC XML", filename=marc, mimeType='application/xml')
        else:
            logger.info('No PDF for %s', self.dbid)
            
        
        
        dc = score['DC']
        dc['type'] = [unicode('StillImage')]
        dc['title'] = [self.element.findtext('TITULO')]
        dc.setContent()
            