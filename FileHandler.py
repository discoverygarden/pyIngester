#!/usr/bin/env python2.6
import logging
import sys
import types

class FileHandler:
    def __init__(self):
        pass
    
    @staticmethod
    def str_to_class(module, field):
        try:
            identifier = getattr(module, field)
        except AttributeError:
            raise NameError("%s doesn't exist in %s" % (field, module))
        else:
            if isinstance(identifier, (types.ClassType, types.TypeType)):
                return identifier
            else:
                raise TypeError("%s is not a class." % field) 
    
    @staticmethod
    def process(f, settings):
        '''Matches the provided pattern line-by-line, as a regex'''
        logger = logging.getLogger('ingest.FileHandler')
        with open(f) as file:
            for line in file:
                if re.match(settings['pattern'], line):
                    #TODO: make it instantiate the class given in the dictionary 'settings'
                    pass
                else:
                    logger.info("Did not match regex: %s", line)
                
                
    

