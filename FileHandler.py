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
        '''
        Matches the provided pattern line-by-line, as a regex
        Classes which are used by this handler should have constructors which accept two parameters, and a method: 
        *   'file_path' - the path to the file being processed (for including in log output, really...), and
        *   'line' - the line whose contents need to be parsed to do that what we need to do in...
        *   'process()' - do the actual processing.
        '''
        logger = logging.getLogger('ingest.FileHandler')
        with open(f) as file:
            for line in file:
                if re.match(settings['pattern'], line):
                    try:
                        #Create an instance of the class, passing in the path to the file being processed, and the line to be processed.
                        toProcess = settings['class'](file_path=f, line=line)
                    except KeyError as e:
                        logger.debug('Bad key: %s...  Object builder of this type probably doesn\'t exist yet', 'class')
                    #    pass
                    except TypeError as e:
                        logger.error('%s', settings)
                        logger.error('%s (this type probably doesn\'t exist yet)', e)
                    #    pass
                    else:
                        toProcess.process()
                        del toProcess
                else:
                    logger.debug("Did not match regex: %s", line)
                
                
    

