#
# Configration model
#
# This model provides set_value() and get_value() to update SUT configuration option and get the configuation option value.
#


import ConfigParser
import os
from logger import logger

VERIFY_CONFIG = os.path.abspath(os.path.join(os.path.dirname(__file__), 'verify_config.cfg'))
# print VERIFY_CONFIG
def set_value(section,option,value):
    if not section or not option or not value:
        logger.error(r'input value error,section {0},option {1},value {2}'.format(section,option,value))
        return False
    conf = ConfigParser.ConfigParser()
    try:
        conf.read(VERIFY_CONFIG)
        if not conf.has_section(section):
            logger.error(r'not have section {0}'.format(section))
            return False
        conf.set(section,option,value)
        with open(VERIFY_CONFIG,'w') as f:
            conf.write(f)
            logger.debug(r'set_value success end')
        return True
    except Exception:
        logger.error(r'set_value raise error')
        return False
def get_value(section,option):
    '''
    This API is used to get the configuration options value.

    :param: section: SUT_config.cfg section
    :return: The input SUT configuration option value.
    :dependency: None
    :black box equivalent class: section and option are valid;
                                 section is invalid --> return RET_INVALID_INPUT;
                                 option is invalid --> return RET_INVALID_INPUT.
    '''

    # Reference code
    # conf = ConfigParser.ConfigParser()
    # conf.read('SUT_config.cfg')
    # value = conf.get(section,option)  # May raise NoOptionError or NoSectionError exception. Capture them and return RET_INVALID_INPUT.
    # return value
    if not section or not option:
        logger.error(r'input value error,section {0},option {1}'.format(section,option))
        return False
    conf = ConfigParser.ConfigParser()
    try:
        conf.read(VERIFY_CONFIG)
        return conf.get(section, option)
    except Exception:
        logger.error(r'get_value raise error')
        return False