
import csv
from os import path
from atm_object import atm_object as ao
from FedoraWrapper import FedoraWrapper, update_datastream
#from islandoraUtils.fedoraLib import update_hashed_datastream_without_dup as update_datastream
from islandoraUtils.metadata.eaccpf import EACCPF as eaccpf
from islandoraUtils.metadata import fedora_relationships as FR
from islandoraUtils.xmlib import import_etree
etree = import_etree() 

class Authority(ao):
    handler = ('CSVHandler', 'CSVHandler')
    #element should be a list of values, as provided by csv.reader...
    def __init__(self, file_path, element, prefix='ir-test-again', loggerName='ingest.FileHandler.atm_authority'):
        super(Authority, self).__init__(file_path, element, prefix, loggerName)
        
    def process(self):
        #self.logger.debug('Received line: %s' % self.element)
        
        info = dict()
        for part, value in zip(['forename', 'surname', 'birth_date', 'death_date', 'alt_forename', 'ceacs_member', 'academic_page', 'ceacs_arrival', 'ceacs_depart', 'phd_date', 'photo'], self.element):
            val = unicode(value.strip(), 'UTF-8', 'replace')
            if val or part in ['birth_date', 'death_date']:
                info[part] = val
        if 'photo' not in info:
            return
        #self.logger.debug('info dictionary: %s' % info)
        
        info['full_name'] = "%(surname)s, %(forename)s" % info
        auth_record = FedoraWrapper.getNextObject(prefix=self.prefix, label=info['full_name'].encode('ascii', 'replace'))
        
        cpf = eaccpf(auth_record.pid)
        
        cpf.add_maintenance_event(agent="Adam Vessey, via ir_authority.py")
        cpf.add_bin_source(caption='Row in Excel spreadsheet', obj=str(self.element))
        
        cpf.add_name_entry(name={
            'forename': info['forename'],
            'surname': info['surname']
        })
        if 'alt_forename' in info:
            cpf.add_name_entry(name={
                'forename': info['alt_forename'],
                'surname': info['surname']
            }, role='alternative')
        
        cl = list()
        rel = list()
        
        if 'phd_date' in info:
            cl.append({
                'date': info['phd_date'],
                'event': 'Achieved PhD'
            })
            
        if 'ceacs_arrival' in info and 'ceacs_depart' in info:
            rel = {
                'dateRange': {
                    'fromDate': info['ceacs_arrival'],
                    'toDate': info['ceacs_depart']
                },
                'event': 'CEACS membership'
            }
        elif 'ceacs_arrival' in info:
            rel = {
                'dateRange': {
                    'fromDate': info['ceacs_arrival']
                },
                'event': 'CEACS membership'
            }
        elif 'ceacs_depart' in info:
            rel = {
                'dateRange': {
                    'toDate': info['ceacs_depart']
                },
                'event': 'CEACS membership'
            }
            
        if cl:
            cpf.add_chron_list(cl)
            
        if rel:
            cpf.add_relation(type='cpfRelation', url='http://digital.march.es/ceacs', elements=rel)
        
        if 'academic_page' in info:
            cpf.add_relation(type="resourceRelation", url=info['academic_page'], elements={'descriptiveNote': 'Academic page'})
        
        cpf.add_exist_dates(info['birth_date'], info['death_date'])

        #print(cpf)
        Authority.save_etree(auth_record, cpf.element, 'EAC-CPF', 'EAC-CPF record', controlGroup='X', hash='DISABLED')
        rels = FR.rels_ext(obj=auth_record, namespaces=Authority.NS.values())
        rels.addRelationship(['fedora-model', 'hasModel'], ['ir:authorityCModel', 'pid'])
        rels.update()
        
        #Add image (with relationship to object?).
        if 'photo' in info:
            photo_path = self.getPath(info['photo'])
            if path.exists(photo_path):
                #Create the object...
                photo = FedoraWrapper.getNextObject(self.prefix, label=('Photo of %s' % info['full_name']).encode('ascii', 'replace'))
                
                #... add the datastream ...
                update_datastream(photo, 'JPG', filename=photo_path, label='Original image', checksumType='SHA-1', mimeType='image/jpeg')
                
                #... and relate the object.
                NSs = Authority.NS
                NSs['ir-rel'] = FR.rels_namespace('ir-rel', 'http://digital.march.es/ceacs#')
                p_rels = FR.rels_ext(photo, namespaces=NSs.values())
                
                p_rels.addRelationship(['fedora-model', 'hasModel'], ['ir:photoCModel', 'pid'])
                p_rels.addRelationship(['ir-rel', 'iconOf'], [auth_record.pid, 'pid'])
                p_rels.update()
            else:
                self.logger.warning('photo: %s specified, but %s does not exist!' % (info['photo'], photo_path))
        else:
            self.logger.debug('No photo specified.')
            