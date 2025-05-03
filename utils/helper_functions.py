"""
This module contains various helper functions which
are used throughout the project.
"""

from HoymilesZeroExport import logger


def cast_to_int(pValueToCast):
    try:
        result = int(pValueToCast)
        return result
    except:
        logger.warning(f'Unable to directly cast to integer: {pValueToCast}, trying via conversion to float...')
        result = 0
    try:
        result = int(float(pValueToCast))
        return result
    except:
        logger.warning(f'Unable to cast to at all: {pValueToCast}')
        raise


def get_number_array(pExcludedPanels):
    lclExcludedPanelsList = pExcludedPanels.split(',')
    result = []
    for number_str in lclExcludedPanelsList:
        if number_str == '':
            continue
        number = int(number_str.strip())
        result.append(number)
    return result
