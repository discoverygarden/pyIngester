#!/usr/bin/env python2.6
import logging
from FileHandler import FileHandler
import sys

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

class XMLHandler(FileHandler):
    def __init__(self):
        self.super()
        
    @staticmethod
    def process(f, settings):
        '''
        Creates an object of settings['className'] based on matches to 
        settings['pattern'] in the provided filename
        NOTE:  Due to how ElementTree works, the 'root' node is 
        skipped, so in my case, where I'm looking for 
        'Conciertos/Concierto', I should use the pattern 'Concierto' 
        '''
        logger = logging.getLogger('ingest.XMLHandler')
        
        try:
            xml = etree.parse(f)
            logger.info('Processing %s with pattern %s', f, settings['class'].pattern)
            for item in xml.findall(settings['class'].pattern):
                try:
                    toProcess = settings['class'](file_path=f, element=item)
                except KeyError as e:
                    logger.debug('Bad key: %s...  Object builder of this type probably doesn\'t exist yet', 'class')
                except TypeError as e:
                    logger.error('%s', settings)
                    logger.error('%s (this type probably doesn\'t exist yet)', e)
                else:
                    toProcess.process()
                    del toProcess
                        
        except etree.ParseError:
            print('Error while parsing: %s' % f)
          
