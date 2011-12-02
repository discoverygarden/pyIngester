
import csv
from os import path
from atm_object import atm_object as ao
from FedoraWrapper import FedoraWrapper
from islandoraUtils.fedoraLib import update_hashed_datastream_without_dup as update_datastream
from islandoraUtils.metadata.eaccpf import EACCPF as eaccpf
from islandoraUtils.metadata import fedora_relationships as FR
from islandoraUtils.xmlib import import_etree
etree = import_etree() 

class Authority(ao):
    handler = ('CSVHandler', 'CSVHandler')
    #element should be a list of values, as provided by csv.reader...
    def __init__(self, file_path, element, prefix='ir-test', loggerName='ingest.FileHandler.atm_authority'):
        super(Authority, self).__init__(file_path, element, prefix, loggerName)
        
    def process(self):
        #Crazy bit of unpacking...
        self.logger.debug('Received line: %s' % self.element)
        
        info = dict()
        for part, value in zip(['full_name', 'lib_o_cong', 'forename', 'surname', 'birth_date', 'death_date', 'title', 'dirty_alt_forename', 'alt_forename', 'ceacs_member', 'academic_page', 'ceacs_arrival', 'ceacs_depart', 'phd_date', 'photo', 'other'], self.element):
            val = unicode(value, 'UTF-8').strip()
            if val or part in ['birth_date', 'death_date']:
                info[part] = val
        self.logger.debug('info dictionary: %s' % info)
        
        #auth_record = FedoraWrapper.getNextObject(prefix=self.prefix, label=unicode(info['full_name']))
        auth_record = object()
        auth_record.pid = 'test'
        
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
        Authority.save_etree(auth_record, cpf.element, 'EAC-CPF', 'EAC-CPF record')
        rels = FR.rels_ext(auth_record, namespaces=Authority.NSMAP)
        rels.addRelationship(['fedora-model', 'hasModel'], 'ir:authorityCModel')
        rels.update()
        
        #Add image (with relationship to object?).
        if 'photo' in info:
            photo_path = self.getPath(info['path'])
            if path.exists(photo_path):
                #Create the object...
                photo = FedoraWrapper.getNextObject(self.prefix, 'Photo of %s' % info['full_name'])
                
                #... add the datastream ...
                update_datastream(photo, 'JPG', 'Original image', checksumType='SHA-1', mimeType='image/jpeg')
                
                #... and relate the object.
                NSs = Authority.NSMAP
                NSs['ir-rel'] = 'http://digital.march.es/ceacs#'
                p_rels = FR.rels_ext(photo, namespaces=NSs)
                
                p_rels.addRelationship(['fedora-model', 'hasModel'],'ir:photoCModel')
                p_rels.addRelationship(['ir-rel', 'iconOf'], auth_record.pid)
                p_rels.update()
            else:
                self.logger.warning('photo: %s specified, but %s does not exist!' % (info['photo'], photo_path))
        else:
            self.logger.debug('No photo specified.')
            