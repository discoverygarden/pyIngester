#!/usr/bin/env python2.6
import logging
import sys
import types
import csv

class CSVHandler:
    def __init__(self):
        pass
    
    @staticmethod
    def str_to_class(module, field):
        '''
        Acquired from the 'net somewhere...  I feel kinda bad that I don't
        actually remember the source...  Don't think I really use this, in any case.
        '''
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
        '''
        Matches the provided pattern line-by-line, as a regex
        Classes which are used by this handler should have constructors which accept two parameters, and a method: 
        *   'file_path' - the path to the file being processed (for including in log output, really...), and
        *   'line' - the line whose contents need to be parsed to do that what we need to do in...
        *   'process()' - do the actual processing.
        '''
        logger = logging.getLogger('ingest.CSVHandler')
        logger.debug('Entered CSVHandler.process()')
        count = 0
        with open(f, 'rb') as file:
            for line in csv.reader(file):
                try:
                    #Create an instance of the class, passing in the path to the file being processed, and the line to be processed.
                    toProcess = settings['class'](file_path=f, element=line)
                except KeyError as e:
                    logger.debug('Bad key: %s...  Object builder of this type probably doesn\'t exist yet', 'class')
                except TypeError as e:
                    logger.error('%s', settings)
                    logger.error('%s (this type probably doesn\'t exist yet)', e)
                else:
                    if count > 0: #To ignore the first/header line.
                      toProcess.process()
                    count += 1
                    del toProcess
                
                
    

