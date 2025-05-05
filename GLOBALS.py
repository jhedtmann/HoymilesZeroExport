# This file keeps the large number of global variables in one place and
# accessible to all modules without creating circular dependencies.
# Ideally, there should not be any globals; this shall be a task for another day...

import logging

from requests import Session

session = Session()

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

SERIAL_NUMBER = []
ENABLED = []
NAME = []
TEMPERATURE = []
HOY_MAX_WATT = []
HOY_INVERTER_WATT = []
CURRENT_LIMIT = []
AVAILABLE = []
LASTLIMITACKNOWLEDGED = []
HOY_BATTERY_GOOD_VOLTAGE = []
HOY_COMPENSATE_WATT_FACTOR = []
HOY_BATTERY_MODE = []
HOY_BATTERY_THRESHOLD_OFF_LIMIT_IN_V = []
HOY_BATTERY_THRESHOLD_REDUCE_LIMIT_IN_V = []
HOY_BATTERY_THRESHOLD_NORMAL_LIMIT_IN_V = []
HOY_BATTERY_THRESHOLD_ON_LIMIT_IN_V = []
HOY_BATTERY_IGNORE_PANELS = []
HOY_PANEL_VOLTAGE_LIST = []
HOY_PANEL_MIN_VOLTAGE_HISTORY_LIST = []
HOY_BATTERY_AVERAGE_CNT = []
