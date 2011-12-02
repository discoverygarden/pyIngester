#!/usr/bin/env python2.6
import logging
logging.basicConfig(filename='ingest.log', level=logging.DEBUG,
    datefmt='%Y/%m/%d %H:%M:%S',
    format='%(asctime)s  %(name)s:%(levelname)s: %(message)s')
logging.debug('Logger initialized')
import signal
from optparse import OptionParser, OptionGroup
import os
import ConfigParser
import sys
from FileHandler import FileHandler
from fjm.FedoraWrapper import FedoraWrapper


#Defaults...
#FIXME:  Make the config file name be absolute (currently looks in the directory from which this script is executed.)
CONFIG_FILE_NAME = os.path.join(os.path.dirname(__file__), 'ingest.conf')
FILE_LIST = []
SELECTOR = ''
DIR_LIST = []
DEF_EXTS = ['.xml']

def main():
    logger = logging.getLogger('ingest')  
    
    logger.debug('Creating OptionsParser')
    optionp = OptionParser(description='Create (and add) Fedora Objects ' + 
        'according to XPaths/patterns')
    optionp.add_option('-C', '--config-file', type='string', dest='configfile', 
        metavar='FILE', default=CONFIG_FILE_NAME, 
        help='Path to the configuration file.')
        
    rifle = OptionGroup(optionp, 'Narrow', 'Pick individual files')
    rifle.add_option('-f', '--file', dest='files', action='append', 
        default=FILE_LIST, metavar='FILE', help='individual xml file(s) to ' + 
        'attempt to import [0, *)')
    rifle.add_option('-s', '--select', dest='selector', action='store', 
        default=SELECTOR, metavar='SELECTOR', help='Only attempt to import ' + 
        'elements which match the given selector (actual usage depends on ' + 
        'filetype) [0, 1]')
        
    shotgun = OptionGroup(optionp, 'Wide', 'Try entire directories')
    shotgun.add_option('-d', '--dir', '--directory', dest='dirs', 
        default=DIR_LIST, action='append', metavar='DIR', 
        help='director{y|ies} to attempt to import [0, *)')
    shotgun.add_option('-e', '--extension', dest='exts', action='append',
        metavar='EXT', help='filename extension(s) to look for in specified ' + 
        'directories [0, *)', default=DEF_EXTS)
    shotgun.add_option('-r', '-R', '--recursive', dest='recurse', default=False,
        action='store_true', help='add files recursively (in sub-directories)')
    optionp.add_option_group(rifle)
    optionp.add_option_group(shotgun)
    (options, args) = optionp.parse_args()
    logger.debug('Done with OptionsParser')
    
    logger.debug('Creating ConfigParser')
    try:
        configp = ConfigParser.SafeConfigParser()
        conffile = os.path.abspath(options.configfile)
        configp.read(conffile)
        
        if options.selector:
            section = [('custom', options.selector)]
        else:
            section = configp.items('Mappings')
        #TODO (minor: naming):  Change "module" to "handler" or similar...
        mapTo = ['classPkg', 'className']
        mappings = dict()
        for sect, values in section:
            mappings[sect] = dict(zip(mapTo, values.split(',')))
            logger.debug("%s", mappings[sect])
            
            try:
                __import__(mappings[sect]['classPkg'])
                mappings[sect]['class'] = getattr(sys.modules[mappings[sect]['classPkg']],
                    mappings[sect]['className'])
            except ImportError as e:
                logger.error(e)
            else:
                try:
                    __import__(mappings[sect]['class'].handler[0])
                    mappings[sect]['module'] = getattr(sys.modules[mappings[sect]['class'].handler[0]],
                        mappings[sect]['class'].handler[1])
                except ImportError as e:
                    logger.error(e)
            
            #Hmmm...  The selector should work in a manner similar to below, but not quite...  
            #   Really, I'm thinking it may be necessary just to reproduce a configuration line
            #   instead of some other syntax...  The configuration should be simplified, though.
            #if options.selector:
            #    mappings[sect]['pattern'] = options.selector
            
    except (ConfigParser.Error, ValueError), e:
        logger.warning('Error reading config file: %s ->  Continuing merrily.', conffile)
        logger.exception('%s' % e)
        logger.debug('%s' % section)
    logger.debug('Done with ConfigParser')  
    
    logger.debug("Mappings: %s", mappings)
    
    logger.debug("Options: %s", options)
    for o in options.dirs:
        try:
            logger.debug('Traversing dir: ' + o)
            if options.recurse:
                for root, dirs, files in os.walk(o):
                    for f in files:
                        name = os.path.join(root, f)
                        if options.files.count(name) > 0:
                            logger.warning('Already have: %s', f);
                        else:                          
                            options.files.append(name)
                            logger.debug('Add to \'files\' list: %s', name) 
            else:
                for f in os.listdir(o):
                    if not os.path.isdir(f):
                        options.files.append(os.path.join(o, f))
                        logger.debug('Add to \'files\' list: %s', f)
            logger.debug('Done adding files from: %s', o)
        except os.error:
            logger.warning('Not a directory!: %s', o)

    FedoraWrapper.init()
    for f in options.files:
        if f.endswith(tuple(options.exts)):
            #Perform processing on this file
            logger.debug('Process file: %s', f)
            
            for recordtype in mappings:
                mappings[recordtype]['module'].process(f, mappings[recordtype])
    FedoraWrapper.destroy()

def shutdown_handler(signum, frame):
    type = 'unknown'
    if signum == signal.SIGINT:
        type = 'interrupt'
    elif signum == signal.SIGTERM:
        type = 'terminate'
    
    logging.critical('%s signal received!  Shutting down!', type)
    
    #perform nice shutdown
    # close connection to Fedora and the like
    logging.shutdown()
    
    #NOTE: signum should not be zero (which normally indicates success)
    # INT = 2 and TERM = 15, so we should be good (On POSIX, anyway)
    sys.exit(signum)

if __name__=="__main__":
    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    
    try:
        main()
    except Exception as e:
        logging.exception(e)
    finally:
        logging.shutdown()

