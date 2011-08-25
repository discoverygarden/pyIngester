
from atm_object import atm_object as ao
from FedoraWrapper import FedoraWrapper

class Person(ao):
    #A dictionary of normalized names to PIDs
    PEOPLE = None
    
    def __init__(self, file_path, element, prefix, loggerName):
        super(Person, self).__init__(file_path, element, prefix, loggerName)
        FedoraWrapper.init()
        
    def normalized_name(self):
        fl = self.name['forename'].split()
        ll = self.name['surname'].split()
        fl.extend(ll)
        return " ".join(fl).title()
        
    @staticmethod
    def __people():
        
        if Person.PEOPLE == None:
            Person.PEOPLE = dict()
            for result in FedoraWrapper.client.searchTriples(query='\
select $obj $name from <#ri> \
where $obj <dc:title> $name \
and $obj <fedora-model:hasModel> <fedora:atm:personCModel>', lang='itql', limit='1000000'):
                Person.__addPerson(result['name']['value'], result['obj']['value'])
        print Person.PEOPLE
        return Person.PEOPLE

    @staticmethod
    def __addPerson(key, value):
        fedora = u'info:fedora/'
        if value.startswith(fedora):
            val = value[len(fedora):]
        else:
            val = value
        Person.__people()[key] = val
        
    def __setitem__(self, key, value):
        Person.__addPerson(key, value)
        
    def __getitem__(self, key):
        return Person.__people()[key]
