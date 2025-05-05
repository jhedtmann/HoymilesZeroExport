"""
This module contains various helper functions which
are used throughout the project.
"""

import logging


logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

def cast_to_int(p_value_to_cast):
    try:
        result = int(p_value_to_cast)
        return result
    except:
        logger.warning(f'Unable to directly cast to integer: {p_value_to_cast}, trying via conversion to float...')
        result = 0
    try:
        result = int(float(p_value_to_cast))
        return result
    except:
        logger.warning(f'Unable to cast to at all: {p_value_to_cast}')
        raise


def get_number_array(p_excluded_panels):
    lcl_excluded_panels_list = p_excluded_panels.split(',')
    result = []
    for number_str in lcl_excluded_panels_list:
        if number_str == '':
            continue
        number = int(number_str.strip())
        result.append(number)
    return result

def extract_json_value(data, path):
    from jsonpath_ng import parse
    jsonpath_expr = parse(path)
    match = jsonpath_expr.find(data)
    if match:
        return int(float(match[0].value))
    else:
        raise ValueError("No match found for the JSON path")
