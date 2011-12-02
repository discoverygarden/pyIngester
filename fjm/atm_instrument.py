 
from atm_object import atm_object as ao
from islandoraUtils.metadata import fedora_relationships as FR
from FedoraWrapper import FedoraWrapper

class Instrument(ao):
    INSTRUMENT_CLASS = None
    INSTRUMENT = None
    pattern = 'Instrumento[@id]'
    handler = ('XMLHandler', 'XMLHandler')
    def __init__(self, file_path, element, prefix=ao.PREFIX):
        super(Instrument, self).__init__(file_path, element, prefix, loggerName='ingest.XMLHandler.atm_instrument')
        FedoraWrapper.init()
        
        self.dbid = self.element.get('id')
        self.instrumentName = Instrument.normalize_name([self.element.findtext('nombre')])
        self.classID = self.element.findtext('id_tipo_instrumento')
        self.instrumentClass = Instrument.normalize_name([self.element.findtext('tipoInstrumento')])
        
    def process(self):
        try:
            pid = Instrument.__getClasses()[self.classID]
            instrumentClass = FedoraWrapper.client.getObject(pid)
        except KeyError:
            instrumentClass = FedoraWrapper.getNextObject(self.prefix, label='Instrument class %s' % self.classID)
            Instrument.__addInstrumentClass(self.classID, instrumentClass.pid)
            c_rels = [
                (
                    FR.rels_predicate(alias='fjm-db', predicate='instrumentClassID'),
                    FR.rels_object(self.classID, FR.rels_object.LITERAL)
                ),
                (
                    FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                    FR.rels_object('atm:instrumentClassCModel', FR.rels_object.PID)
                )
            ]
            FedoraWrapper.addRelationshipsWithoutDup(c_rels, fedora=instrumentClass).update()
            dc = dict()
            dc['title'] = [self.instrumentClass]
            Instrument.save_dc(instrumentClass, dc)
        instrumentClass.state = unicode('A')
            
        try:
            pid = Instrument.__getInstruments()[self.instrumentName]
            instrument = FedoraWrapper.client.getObject(pid)
        except KeyError:
            instrument = FedoraWrapper.getNextObject(self.prefix, label='Instrument %s' % self.dbid)
            Instrument.__addInstrument(self.instrumentName, instrument.pid)
            dc = dict()
            dc['title'] = [self.instrumentName]
            Instrument.save_dc(instrument, dc)
        i_rels = [
            (
                FR.rels_predicate(alias='fjm-db', predicate='instrumentID'),
                FR.rels_object(self.dbid, FR.rels_object.LITERAL)
            ),
            (
                FR.rels_predicate(alias='fedora-model', predicate='hasModel'),
                FR.rels_object('atm:instrumentCModel', FR.rels_object.PID)
            ),
            (
                FR.rels_predicate(alias='fedora-rels-ext', predicate='isMemberOf'),
                FR.rels_object(instrumentClass.pid, FR.rels_object.PID)
            )
        ]
        FedoraWrapper.addRelationshipsWithoutDup(i_rels, fedora=instrument).update()
        FedoraWrapper.correlateDBEntry('instrument', 'instrumentID')
        instrument.state = unicode('A')
    
    @staticmethod
    def __getInstruments():
        if Instrument.INSTRUMENT == None:
            Instrument.INSTRUMENT = dict()
            for result in FedoraWrapper.client.searchTriples(query='\
select $obj $name from <#ri> \
where $obj <dc:title> $name \
and $obj <fedora-model:hasModel> <fedora:atm:instrumentCModel>', lang='itql', limit='1000000'):
                Instrument.__addInstrument(result['name']['value'], result['obj']['value'])
            print Instrument.INSTRUMENT
        return Instrument.INSTRUMENT
        
    @staticmethod
    def __addInstrument(key, value):
        fedora = u'info:fedora/'
        if value.startswith(fedora):
            val = value[len(fedora):]
        else:
            val = value
        Instrument.__getInstruments()[key] = val
        
    @staticmethod
    def __getClasses():
        if Instrument.INSTRUMENT_CLASS == None:
            Instrument.INSTRUMENT_CLASS = dict()
            for result in FedoraWrapper.client.searchTriples(query='\
PREFIX fjm-db: <http://digital.march.es/db#> \
PREFIX fedora-model: <info:fedora/fedora-system:def/model#> \
SELECT $obj $name \
FROM <#ri> \
WHERE {\
$obj fjm-db:instrumentClassID $name . \
$obj fedora-model:hasModel <info:fedora/atm:instrumentClassCModel> \
}', lang='sparql', limit='1000000'):
                Instrument.__addInstrumentClass(result['name']['value'], result['obj']['value'])
            print Instrument.INSTRUMENT_CLASS
        return Instrument.INSTRUMENT_CLASS
        
    @staticmethod
    def __addInstrumentClass(key, value):
        fedora = u'info:fedora/'
        if value.startswith(fedora):
            val = value[len(fedora):]
        else:
            val = value
        Instrument.__getClasses()[key] = val
