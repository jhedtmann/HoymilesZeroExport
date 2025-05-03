# HoymilesZeroExport - https://github.com/reserve85/HoymilesZeroExport
# Copyright (C) 2023, Tobias Kraft

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = "Tobias Kraft"
__contributors__ = "Jörg Hedtmann, <df3ei@db0kk.org>"
__version__ = "1.112"

import argparse
import logging
import os
import sys
import time

from configparser import ConfigParser
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from requests.adapters import HTTPAdapter
from requests.sessions import Session
from urllib3.util import Retry

from configuration.config_providers import ConfigFileConfigProvider, MqttHandler, ConfigProviderChain
from utils.helper_functions import cast_to_int

session = Session()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='Override configuration file path')
args = parser.parse_args()

try:
    config = ConfigParser()

    baseconfig = str(Path.joinpath(Path(__file__).parent.resolve(), "HoymilesZeroExport_Config.ini"))
    if args.config:
        config.read([baseconfig, args.config])
    else:
        config.read(baseconfig)

    ENABLE_LOG_TO_FILE = config.getboolean('COMMON', 'ENABLE_LOG_TO_FILE')
    LOG_BACKUP_COUNT = config.getint('COMMON', 'LOG_BACKUP_COUNT')
except Exception as e:
    logger.info('Error on reading ENABLE_LOG_TO_FILE, set it to DISABLED')
    ENABLE_LOG_TO_FILE = False
    if hasattr(e, 'message'):
        logger.error(e.message)
    else:
        logger.error(e)

if ENABLE_LOG_TO_FILE:
    if not os.path.exists(Path.joinpath(Path(__file__).parent.resolve(), 'log')):
        os.makedirs(Path.joinpath(Path(__file__).parent.resolve(), 'log'))

    rotating_file_handler = TimedRotatingFileHandler(
        filename=Path.joinpath(Path.joinpath(Path(__file__).parent.resolve(), 'log'),'log'),
        when='midnight',
        interval=2,
        backupCount=LOG_BACKUP_COUNT)

    formatter = logging.Formatter(
        '%(asctime)s %(levelname)-8s %(message)s')
    rotating_file_handler.setFormatter(formatter)
    logger.addHandler(rotating_file_handler)

logger.info('Log write to file: %s', ENABLE_LOG_TO_FILE)
logger.info('Python Version: ' + sys.version)

try:
    assert sys.version_info >= (3,8)
except:
    logger.info('Error: your Python version is too old, this script requires version 3.8 or newer. Please update your Python.')
    sys.exit()

def set_limit(p_limit):
    try:
        if not hasattr(set_limit, "LastLimit"):
            set_limit.LastLimit = cast_to_int(0)
        if not hasattr(set_limit, "LastLimitAck"):
            set_limit.LastLimitAck = bool(False)
        if (set_limit.LastLimit == cast_to_int(p_limit)) and set_limit.LastLimitAck:
            logger.info("Inverterlimit was already accepted at %s Watt", cast_to_int(p_limit))
            cross_check_limit()
            return
        if (set_limit.LastLimit == cast_to_int(p_limit)) and not set_limit.LastLimitAck:
            logger.info("Inverterlimit %s Watt was previously not accepted by at least one inverter, trying again...", cast_to_int(p_limit))

        logger.info("setting new limit to %s Watt", cast_to_int(p_limit))
        set_limit.LastLimit = cast_to_int(p_limit)
        set_limit.LastLimitAck = True

        min_watt_all_inverters = get_min_watt_from_all_inverters()
        if (cast_to_int(p_limit) <= min_watt_all_inverters):
            p_limit = min_watt_all_inverters # set only minWatt for every inv.
            publish_global_state("limit", min_watt_all_inverters)
        else:
            publish_global_state("limit", cast_to_int(p_limit))

        remaining_limit = cast_to_int(p_limit)

        remaining_limit -= get_min_watt_from_all_inverters()

        # Handle non-battery inverters first
        if remaining_limit >= get_max_watt_from_all_non_battery_inverters() - get_min_watt_from_all_non_battery_inverters():
            non_battery_inverters_limit = get_max_watt_from_all_non_battery_inverters() - get_min_watt_from_all_non_battery_inverters()
        else:
            non_battery_inverters_limit = remaining_limit

        for i in range(INVERTER_COUNT):
            if not AVAILABLE[i] or HOY_BATTERY_MODE[i]:
                continue

            # Calculate proportional limit for non-battery inverters
            new_limit = cast_to_int(non_battery_inverters_limit * (HOY_MAX_WATT[i] - get_min_watt(i)) / (get_max_watt_from_all_non_battery_inverters() - get_min_watt_from_all_non_battery_inverters()))

            new_limit += get_min_watt(i)

            # Apply the calculated limit to the inverter
            new_limit = apply_limits_to_setpoint_inverter(i, new_limit)
            if HOY_COMPENSATE_WATT_FACTOR[i] != 1:
                logger.info('Ahoy: Inverter "%s": compensate Limit from %s Watt to %s Watt', NAME[i], cast_to_int(new_limit), cast_to_int(new_limit * HOY_COMPENSATE_WATT_FACTOR[i]))
                new_limit = cast_to_int(new_limit * HOY_COMPENSATE_WATT_FACTOR[i])
                new_limit = apply_limits_to_max_inverter_limits(i, new_limit)

            if (new_limit == cast_to_int(CURRENT_LIMIT[i])) and LASTLIMITACKNOWLEDGED[i]:
                logger.info('Inverter "%s": Already at %s Watt', NAME[i], cast_to_int(new_limit))
                continue

            LASTLIMITACKNOWLEDGED[i] = True

            publish_inverter_state(i, "limit", new_limit)
            DTU.set_limit(i, new_limit)
            if not DTU.wait_for_ack(i, SET_LIMIT_TIMEOUT_SECONDS):
                set_limit.LastLimitAck = False
                LASTLIMITACKNOWLEDGED[i] = False

        # Adjust the remaining limit based on what was assigned to non-battery inverters
        remaining_limit -= non_battery_inverters_limit

        # Then handle battery inverters based on priority
        for j in range(1, 6):
            batteryMaxWattSamePrio = get_max_watt_from_all_battery_inverters_same_prio(j)
            if batteryMaxWattSamePrio <= 0:
                continue

            if remaining_limit >= batteryMaxWattSamePrio - get_min_watt_from_all_battery_inverters_with_same_priority(j):
                LimitPrio = batteryMaxWattSamePrio - get_min_watt_from_all_battery_inverters_with_same_priority(j)
            else:
                LimitPrio = remaining_limit

            for i in range(INVERTER_COUNT):
                if (not HOY_BATTERY_MODE[i]):
                    continue
                if (not AVAILABLE[i]) or (not HOY_BATTERY_GOOD_VOLTAGE[i]):
                    continue
                if CONFIG_PROVIDER.get_battery_priority(i) != j:
                    continue

                # Calculate proportional limit for battery inverters
                new_limit = cast_to_int(LimitPrio * (HOY_MAX_WATT[i] - get_min_watt(i)) / (get_max_watt_from_all_battery_inverters_same_prio(j) - get_min_watt_from_all_battery_inverters_with_same_priority(j)))
                new_limit += get_min_watt(i)

                new_limit = apply_limits_to_setpoint_inverter(i, new_limit)
                if HOY_COMPENSATE_WATT_FACTOR[i] != 1:
                    logger.info('Ahoy: Inverter "%s": compensate Limit from %s Watt to %s Watt', NAME[i], cast_to_int(new_limit), cast_to_int(new_limit * HOY_COMPENSATE_WATT_FACTOR[i]))
                    new_limit = cast_to_int(new_limit * HOY_COMPENSATE_WATT_FACTOR[i])
                    new_limit = apply_limits_to_max_inverter_limits(i, new_limit)

                if (new_limit == cast_to_int(CURRENT_LIMIT[i])) and LASTLIMITACKNOWLEDGED[i]:
                    logger.info('Inverter "%s": Already at %s Watt', NAME[i], cast_to_int(new_limit))
                    continue

                LASTLIMITACKNOWLEDGED[i] = True

                publish_inverter_state(i, "limit", new_limit)
                DTU.set_limit(i, new_limit)
                if not DTU.wait_for_ack(i, SET_LIMIT_TIMEOUT_SECONDS):
                    set_limit.LastLimitAck = False
                    LASTLIMITACKNOWLEDGED[i] = False

            remaining_limit -= LimitPrio
    except:
        logger.error("Exception at SetLimit")
        set_limit.LastLimitAck = False
        raise

def reset_inverter_data(pInverterId):
    attributes_to_delete = [
        "LastLimit",
        "LastLimitAck",
    ]
    array_attributes_to_delete = [
        {"LastPowerStatus": False},
        {"SamePowerStatusCnt": 0},
    ]
    target_objects = [
        set_limit,
        get_hoymiles_panel_min_voltage,
    ]
    for target_object in target_objects:
        for attribute in attributes_to_delete:
            if hasattr(target_object, attribute):
                delattr(target_object, attribute)
        for array_attribute in array_attributes_to_delete:
            for key, value in array_attribute.items():
                if hasattr(target_object, key):
                    target_object[key][pInverterId] = value

    LASTLIMITACKNOWLEDGED[pInverterId] = False
    HOY_PANEL_MIN_VOLTAGE_HISTORY_LIST[pInverterId] = []
    CURRENT_LIMIT[pInverterId] = -1
    HOY_BATTERY_GOOD_VOLTAGE[pInverterId] = True
    TEMPERATURE[pInverterId] = str('--- degC')

def get_hoymiles_available():
    try:
        GetHoymilesAvailable = False
        for i in range(INVERTER_COUNT):
            try:
                WasAvail = AVAILABLE[i]
                AVAILABLE[i] = ENABLED[i] and DTU.get_available(i)
                if AVAILABLE[i]:
                    GetHoymilesAvailable = True
                    if not WasAvail:
                        reset_inverter_data(i)
                        get_hoymiles_info()
            except Exception as e:
                AVAILABLE[i] = False
                logger.error("Exception at GetHoymilesAvailable, Inverter %s (%s) not reachable", i, NAME[i])
                if hasattr(e, 'message'):
                    logger.error(e.message)
                else:
                    logger.error(e)
        return GetHoymilesAvailable
    except:
        logger.error('Exception at GetHoymilesAvailable')
        raise

def get_hoymiles_info():
    try:
        for i in range(INVERTER_COUNT):
            try:
                if not AVAILABLE[i]:
                    continue
                DTU.get_info(i)
            except Exception as e:
                logger.error('Exception at GetHoymilesInfo, Inverter "%s" not reachable', NAME[i])
                if hasattr(e, 'message'):
                    logger.error(e.message)
                else:
                    logger.error(e)
    except:
        logger.error("Exception at GetHoymilesInfo")
        raise

def get_hoymiles_panel_min_voltage(pInverterId):
    try:
        if not AVAILABLE[pInverterId]:
            return 0
        
        HOY_PANEL_MIN_VOLTAGE_HISTORY_LIST[pInverterId].append(DTU.get_panel_min_voltage(pInverterId))
        
        # calculate mean over last x values
        if len(HOY_PANEL_MIN_VOLTAGE_HISTORY_LIST[pInverterId]) > HOY_BATTERY_AVERAGE_CNT[pInverterId]:
            HOY_PANEL_MIN_VOLTAGE_HISTORY_LIST[pInverterId].pop(0)
        from statistics import mean
        
        logger.info('Average min-panel voltage, inverter "%s": %s Volt',NAME[pInverterId], mean(HOY_PANEL_MIN_VOLTAGE_HISTORY_LIST[pInverterId]))
        return mean(HOY_PANEL_MIN_VOLTAGE_HISTORY_LIST[pInverterId])
    except:
        logger.error("Exception at GetHoymilesPanelMinVoltage, Inverter %s not reachable", pInverterId)
        raise

def set_hoymiles_power_status(pInverterId, pActive):
    logger.debug(f'{pInverterId} is now {pActive}')
    try:
        if not AVAILABLE[pInverterId]:
            return
        if SET_POWERSTATUS_CNT > 0:
            if not hasattr(set_hoymiles_power_status, "LastPowerStatus"):
                set_hoymiles_power_status.last_power_status = []
                set_hoymiles_power_status.last_power_status = [False for i in range(INVERTER_COUNT)]
            if not hasattr(set_hoymiles_power_status, "SamePowerStatusCnt"):
                set_hoymiles_power_status.SamePowerStatusCnt = []
                set_hoymiles_power_status.SamePowerStatusCnt = [0 for i in range(INVERTER_COUNT)]
            if set_hoymiles_power_status.last_power_status[pInverterId] == pActive:
                set_hoymiles_power_status.SamePowerStatusCnt[pInverterId] = set_hoymiles_power_status.SamePowerStatusCnt[pInverterId] + 1
            else:
                set_hoymiles_power_status.last_power_status[pInverterId] = pActive
                set_hoymiles_power_status.SamePowerStatusCnt[pInverterId] = 0
            if set_hoymiles_power_status.SamePowerStatusCnt[pInverterId] > SET_POWERSTATUS_CNT:
                if pActive:
                    logger.info("Retry Counter exceeded: Inverter PowerStatus already ON")
                else:
                    logger.info("Retry Counter exceeded: Inverter PowerStatus already OFF")
                return
        DTU.set_power_status(pInverterId, pActive)
        time.sleep(SET_POWER_STATUS_DELAY_IN_SECONDS)
    except:
        logger.error("Exception at SetHoymilesPowerStatus")
        raise

def get_check_battery():
    try:
        result = False
        for i in range(INVERTER_COUNT):
            try:
                if not AVAILABLE[i]:
                    continue
                if not HOY_BATTERY_MODE[i]:
                    result = True
                    continue
                minVoltage = get_hoymiles_panel_min_voltage(i)

                if minVoltage <= HOY_BATTERY_THRESHOLD_OFF_LIMIT_IN_V[i]:
                    set_hoymiles_power_status(i, False)
                    HOY_BATTERY_GOOD_VOLTAGE[i] = False
                    HOY_MAX_WATT[i] = CONFIG_PROVIDER.get_reduce_wattage(i)

                elif minVoltage <= HOY_BATTERY_THRESHOLD_REDUCE_LIMIT_IN_V[i]:
                    if HOY_MAX_WATT[i] != CONFIG_PROVIDER.get_reduce_wattage(i):
                        HOY_MAX_WATT[i] = CONFIG_PROVIDER.get_reduce_wattage(i)
                        set_limit.LastLimit = -1

                elif minVoltage >= HOY_BATTERY_THRESHOLD_ON_LIMIT_IN_V[i]:
                    set_hoymiles_power_status(i, True)
                    if not HOY_BATTERY_GOOD_VOLTAGE[i]:
                        DTU.set_limit(i, get_min_watt(i))
                        DTU.wait_for_ack(i, SET_LIMIT_TIMEOUT_SECONDS)
                        set_limit.LastLimit = -1
                    HOY_BATTERY_GOOD_VOLTAGE[i] = True
                    if (minVoltage >= HOY_BATTERY_THRESHOLD_NORMAL_LIMIT_IN_V[i]) and (HOY_MAX_WATT[i] != CONFIG_PROVIDER.get_normal_wattage(i)):
                        HOY_MAX_WATT[i] = CONFIG_PROVIDER.get_normal_wattage(i)
                        set_limit.LastLimit = -1

                elif minVoltage >= HOY_BATTERY_THRESHOLD_NORMAL_LIMIT_IN_V[i]:
                    if HOY_MAX_WATT[i] != CONFIG_PROVIDER.get_normal_wattage(i):
                        HOY_MAX_WATT[i] = CONFIG_PROVIDER.get_normal_wattage(i)
                        set_limit.LastLimit = -1

                if HOY_BATTERY_GOOD_VOLTAGE[i]:
                    result = True
            except:
                logger.error("Exception at CheckBattery, Inverter %s not reachable", i)
        return result
    except:
        logger.error("Exception at CheckBattery")
        raise

def get_hoymiles_temperature():
    try:
        for i in range(INVERTER_COUNT):
            try:
                DTU.get_temperature(i)
            except:
                logger.error("Exception at GetHoymilesTemperature, Inverter %s not reachable", i)
    except:
        logger.error("Exception at GetHoymilesTemperature")
        raise

def get_hoymiles_actual_power():
    try:
        try:
            Watts = abs(INTERMEDIATE_POWERMETER.get_powermeter_watts())
            logger.info(f"intermediate meter {INTERMEDIATE_POWERMETER.__class__.__name__}: {Watts} Watt")
            return Watts
        except Exception as e:
            logger.error("Exception at GetHoymilesActualPower")
            if hasattr(e, 'message'):
                logger.error(e.message)
            else:
                logger.error(e)
            logger.error("try reading actual power from DTU:")
            Watts = DTU.get_powermeter_watts()
            logger.info(f"intermediate meter {DTU.__class__.__name__}: {Watts} Watt")
    except:
        logger.error("Exception at GetHoymilesActualPower")
        if SET_INVERTER_TO_MIN_ON_POWERMETER_ERROR:
            set_limit(0)
        raise

def get_powermeter_watts():
    try:
        Watts = POWERMETER.get_powermeter_watts()
        logger.info(f"metering {POWERMETER.__class__.__name__}: {Watts} Watt")
        return Watts
    except:
        logger.error("Exception at GetPowermeterWatts")
        if SET_INVERTER_TO_MIN_ON_POWERMETER_ERROR:
            set_limit(0)        
        raise

def get_min_watt(pInverter: int):
    min_watt_percent = CONFIG_PROVIDER.get_min_wattage_in_percent(pInverter)
    return int(HOY_INVERTER_WATT[pInverter] * min_watt_percent / 100)

def cut_limit_to_production(pSetpoint):
    if pSetpoint != get_max_watt_from_all_inverters():
        ActualPower = get_hoymiles_actual_power()
        # prevent the setpoint from running away...
        if pSetpoint > ActualPower + (get_max_watt_from_all_inverters() * MAX_DIFFERENCE_BETWEEN_LIMIT_AND_OUTPUTPOWER / 100):
            pSetpoint = cast_to_int(ActualPower + (get_max_watt_from_all_inverters() * MAX_DIFFERENCE_BETWEEN_LIMIT_AND_OUTPUTPOWER / 100))
            logger.info('Cut limit to %s Watt, limit was higher than %s percent of live-production', cast_to_int(pSetpoint), MAX_DIFFERENCE_BETWEEN_LIMIT_AND_OUTPUTPOWER)
    return cast_to_int(pSetpoint)

def check_and_apply_upper_and_lower_limits(pSetpoint):
    if pSetpoint > get_max_watt_from_all_inverters():
        pSetpoint = get_max_watt_from_all_inverters()
    if pSetpoint < get_min_watt_from_all_inverters():
        pSetpoint = get_min_watt_from_all_inverters()
    return pSetpoint

def apply_limits_to_setpoint_inverter(pInverter, pSetpoint):
    if pSetpoint > HOY_MAX_WATT[pInverter]:
        pSetpoint = HOY_MAX_WATT[pInverter]
    if pSetpoint < get_min_watt(pInverter):
        pSetpoint = get_min_watt(pInverter)
    return pSetpoint

def apply_limits_to_max_inverter_limits(pInverter, pSetpoint):
    if pSetpoint > HOY_INVERTER_WATT[pInverter]:
        pSetpoint = HOY_INVERTER_WATT[pInverter]
    if pSetpoint < get_min_watt(pInverter):
        pSetpoint = get_min_watt(pInverter)
    return pSetpoint

def cross_check_limit():
    try:
        for i in range(INVERTER_COUNT):
            if AVAILABLE[i]:
                DTULimitInW = DTU.get_actual_limit_in_w(i)
                LimitMax = float(CURRENT_LIMIT[i] + HOY_INVERTER_WATT[i] * 0.05)
                LimitMin = float(CURRENT_LIMIT[i] - HOY_INVERTER_WATT[i] * 0.05)
                if not (min(LimitMax, LimitMin) < DTULimitInW < max(LimitMax, LimitMin)):
                    logger.info('CrossCheckLimit: DTU ( %s ) <> SetLimit ( %s ). Resend limit to DTU', "{:.1f}".format(DTULimitInW), "{:.1f}".format(CURRENT_LIMIT[i]))
                    DTU.set_limit(i, CURRENT_LIMIT[i])
    except:
        logger.error("Exception at CrossCheckLimit")
        raise

def get_max_watt_from_all_inverters():
    # Max possible Watts, can be reduced on battery mode
    maxWatt = 0
    for i in range(INVERTER_COUNT):
        if (not AVAILABLE[i]) or (not HOY_BATTERY_GOOD_VOLTAGE[i]):
            continue
        maxWatt = maxWatt + HOY_MAX_WATT[i]
    return maxWatt

def get_max_watt_from_all_battery_inverters_same_prio(pPriority):
    return sum(
        HOY_MAX_WATT[i] for i in range(INVERTER_COUNT)
        if AVAILABLE[i] and HOY_BATTERY_GOOD_VOLTAGE[i] and HOY_BATTERY_MODE[i] and CONFIG_PROVIDER.get_battery_priority(i) == pPriority
    )

def get_max_inverter_watt_from_all_inverters():
    # Max possible Watts (physically) - Inverter Specification!
    maxWatt = 0
    for i in range(INVERTER_COUNT):
        if (not AVAILABLE[i]) or (not HOY_BATTERY_GOOD_VOLTAGE[i]):
            continue
        maxWatt = maxWatt + HOY_INVERTER_WATT[i]
    return maxWatt

def get_max_watt_from_all_non_battery_inverters():
    return sum(
        HOY_MAX_WATT[i] for i in range(INVERTER_COUNT)
        if AVAILABLE[i] and not HOY_BATTERY_MODE[i] and HOY_BATTERY_GOOD_VOLTAGE[i]
    )

def get_min_watt_from_all_inverters():
    minWatt = 0
    for i in range(INVERTER_COUNT):
        if (not AVAILABLE[i]) or (not HOY_BATTERY_GOOD_VOLTAGE[i]):
            continue
        minWatt = minWatt + get_min_watt(i)
    return minWatt

def get_min_watt_from_all_non_battery_inverters():
    minWatt = 0
    for i in range(INVERTER_COUNT):
        if (not AVAILABLE[i]) or (HOY_BATTERY_MODE[i]) or (not HOY_BATTERY_GOOD_VOLTAGE[i]):
            continue
        minWatt = minWatt + get_min_watt(i)
    return minWatt

def get_min_watt_from_all_battery_inverters():
    minWatt = 0
    for i in range(INVERTER_COUNT):
        if (not AVAILABLE[i]) or (not HOY_BATTERY_MODE[i]) or (not HOY_BATTERY_GOOD_VOLTAGE[i]):
            continue
        minWatt = minWatt + get_min_watt(i)
    return minWatt  

def get_min_watt_from_all_battery_inverters_with_same_priority(pPriority):
    minWatt = 0
    for i in range(INVERTER_COUNT):
        if (not AVAILABLE[i]) or (not HOY_BATTERY_MODE[i]) or (not HOY_BATTERY_GOOD_VOLTAGE[i]) or (CONFIG_PROVIDER.get_battery_priority(i) != pPriority):
            continue
        minWatt = minWatt + get_min_watt(i)
    return minWatt  

def publish_config_state():
    if MQTT is None:
        return
    MQTT.publish_state("on_grid_usage_jump_to_limit_percent", CONFIG_PROVIDER.on_grid_usage_jump_to_limit_percent())
    MQTT.publish_state("on_grid_feed_fast_limit_decrease", CONFIG_PROVIDER.on_grid_feed_fast_limit_decrease())
    MQTT.publish_state("powermeter_target_point", CONFIG_PROVIDER.get_powermeter_target_point())
    MQTT.publish_state("powermeter_max_point", CONFIG_PROVIDER.get_powermeter_max_point())
    MQTT.publish_state("powermeter_min_point", CONFIG_PROVIDER.get_powermeter_min_point())
    MQTT.publish_state("powermeter_tolerance", CONFIG_PROVIDER.get_powermeter_tolerance())
    MQTT.publish_state("inverter_count", INVERTER_COUNT)
    for i in range(INVERTER_COUNT):
        MQTT.publish_inverter_state(i, "min_watt_in_percent", CONFIG_PROVIDER.get_min_wattage_in_percent(i))
        MQTT.publish_inverter_state(i, "normal_watt", CONFIG_PROVIDER.get_normal_wattage(i))
        MQTT.publish_inverter_state(i, "reduce_watt", CONFIG_PROVIDER.get_reduce_wattage(i))
        MQTT.publish_inverter_state(i, "battery_priority", CONFIG_PROVIDER.get_battery_priority(i))

def publish_global_state(state_name, state_value):
    if MQTT is None:
        return
    MQTT.publish_state(state_name, state_value)

def publish_inverter_state(inverter_idx, state_name, state_value):
    if MQTT is None:
        return
    MQTT.publish_inverter_state(inverter_idx, state_name, state_value)

def extract_json_value(data, path):
    from jsonpath_ng import parse
    jsonpath_expr = parse(path)
    match = jsonpath_expr.find(data)
    if match:
        return int(float(match[0].value))
    else:
        raise ValueError("No match found for the JSON path")

def CreatePowermeter() -> Powermeter:
    shelly_ip = config.get('SHELLY', 'SHELLY_IP')
    shelly_user = config.get('SHELLY', 'SHELLY_USER')
    shelly_pass = config.get('SHELLY', 'SHELLY_PASS')
    shelly_emeterindex = config.get('SHELLY', 'EMETER_INDEX')
    if config.getboolean('SELECT_POWERMETER', 'USE_SHELLY_EM'):
        return ShellyEM(shelly_ip, shelly_user, shelly_pass, shelly_emeterindex)
    elif config.getboolean('SELECT_POWERMETER', 'USE_SHELLY_3EM'):
        return Shelly3EM(shelly_ip, shelly_user, shelly_pass, shelly_emeterindex)
    elif config.getboolean('SELECT_POWERMETER', 'USE_SHELLY_3EM_PRO'):
        return Shelly3EMPro(shelly_ip, shelly_user, shelly_pass, shelly_emeterindex)
    elif config.getboolean('SELECT_POWERMETER', 'USE_TASMOTA'):
        return Tasmota(
            config.get('TASMOTA', 'TASMOTA_IP'),
            config.get('TASMOTA', 'TASMOTA_USER'),
            config.get('TASMOTA', 'TASMOTA_PASS'),
            config.get('TASMOTA', 'TASMOTA_JSON_STATUS'),
            config.get('TASMOTA', 'TASMOTA_JSON_PAYLOAD_MQTT_PREFIX'),
            config.get('TASMOTA', 'TASMOTA_JSON_POWER_MQTT_LABEL'),
            config.get('TASMOTA', 'TASMOTA_JSON_POWER_INPUT_MQTT_LABEL'),
            config.get('TASMOTA', 'TASMOTA_JSON_POWER_OUTPUT_MQTT_LABEL'),
            config.getboolean('TASMOTA', 'TASMOTA_JSON_POWER_CALCULATE', fallback=False)
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_SHRDZM'):
        return Shrdzm(
            config.get('SHRDZM', 'SHRDZM_IP'),
            config.get('SHRDZM', 'SHRDZM_USER'),
            config.get('SHRDZM', 'SHRDZM_PASS')
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_EMLOG'):
        return Emlog(
            config.get('EMLOG', 'EMLOG_IP'),
            config.get('EMLOG', 'EMLOG_METERINDEX'),
            config.getboolean('EMLOG', 'EMLOG_JSON_POWER_CALCULATE', fallback=False)
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_IOBROKER'):
        return IoBroker(
            config.get('IOBROKER', 'IOBROKER_IP'),
            config.get('IOBROKER', 'IOBROKER_PORT'),
            config.get('IOBROKER', 'IOBROKER_CURRENT_POWER_ALIAS'),
            config.getboolean('IOBROKER', 'IOBROKER_POWER_CALCULATE'),
            config.get('IOBROKER', 'IOBROKER_POWER_INPUT_ALIAS'),
            config.get('IOBROKER', 'IOBROKER_POWER_OUTPUT_ALIAS')
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_HOMEASSISTANT'):
        return HomeAssistant(
            config.get('HOMEASSISTANT', 'HA_IP'),
            config.get('HOMEASSISTANT', 'HA_PORT'),
            config.getboolean('HOMEASSISTANT', 'HA_HTTPS', fallback=False),
            config.get('HOMEASSISTANT', 'HA_ACCESSTOKEN'),
            config.get('HOMEASSISTANT', 'HA_CURRENT_POWER_ENTITY'),
            config.getboolean('HOMEASSISTANT', 'HA_POWER_CALCULATE'),
            config.get('HOMEASSISTANT', 'HA_POWER_INPUT_ALIAS'),
            config.get('HOMEASSISTANT', 'HA_POWER_OUTPUT_ALIAS')
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_VZLOGGER'):
        return VZLogger(
            config.get('VZLOGGER', 'VZL_IP'),
            config.get('VZLOGGER', 'VZL_PORT'),
            config.get('VZLOGGER', 'VZL_UUID')
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_SCRIPT'):
        return Script(
            config.get('SCRIPT', 'SCRIPT_FILE'),
            config.get('SCRIPT', 'SCRIPT_IP'),
            config.get('SCRIPT', 'SCRIPT_USER'),
            config.get('SCRIPT', 'SCRIPT_PASS')
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_AMIS_READER'):
        return AmisReader(
            config.get('AMIS_READER', 'AMIS_READER_IP')
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_MQTT'):
        return MqttPowermeter(
            config.get('MQTT_POWERMETER', 'MQTT_BROKER', fallback=config.get("MQTT_CONFIG", "MQTT_BROKER", fallback=None)),
            config.getint('MQTT_POWERMETER', 'MQTT_PORT', fallback=config.getint("MQTT_CONFIG", "MQTT_PORT", fallback=1883)),
            config.get('MQTT_POWERMETER', 'MQTT_TOPIC_INCOMING'),
            config.get('MQTT_POWERMETER', 'MQTT_JSON_PATH_INCOMING', fallback=None),
            config.get('MQTT_POWERMETER', 'MQTT_TOPIC_OUTGOING', fallback=None),
            config.get('MQTT_POWERMETER', 'MQTT_JSON_PATH_OUTGOING', fallback=None),
            config.get('MQTT_POWERMETER', 'MQTT_USERNAME', fallback=config.get('MQTT_CONFIG', 'MQTT_USERNAME', fallback=None)),
            config.get('MQTT_POWERMETER', 'MQTT_PASSWORD', fallback=config.get('MQTT_CONFIG', 'MQTT_PASSWORD', fallback=None))
        )
    elif config.getboolean('SELECT_POWERMETER', 'USE_DEBUG_READER'):
        return DebugReader()    
    else:
        raise Exception("Error: no metering defined!")

def CreateIntermediatePowermeter(dtu: DTU) -> Powermeter:
    shelly_ip = config.get('INTERMEDIATE_SHELLY', 'SHELLY_IP_INTERMEDIATE')
    shelly_user = config.get('INTERMEDIATE_SHELLY', 'SHELLY_USER_INTERMEDIATE')
    shelly_pass = config.get('INTERMEDIATE_SHELLY', 'SHELLY_PASS_INTERMEDIATE')
    shelly_emeterindex = config.get('INTERMEDIATE_SHELLY', 'EMETER_INDEX')
    if config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_TASMOTA_INTERMEDIATE'):
        return Tasmota(
            config.get('INTERMEDIATE_TASMOTA', 'TASMOTA_IP_INTERMEDIATE'),
            config.get('INTERMEDIATE_TASMOTA', 'TASMOTA_USER_INTERMEDIATE'),
            config.get('INTERMEDIATE_TASMOTA', 'TASMOTA_PASS_INTERMEDIATE'),
            config.get('INTERMEDIATE_TASMOTA', 'TASMOTA_JSON_STATUS_INTERMEDIATE'),
            config.get('INTERMEDIATE_TASMOTA', 'TASMOTA_JSON_PAYLOAD_MQTT_PREFIX_INTERMEDIATE'),
            config.get('INTERMEDIATE_TASMOTA', 'TASMOTA_JSON_POWER_MQTT_LABEL_INTERMEDIATE'),
            config.get('INTERMEDIATE_TASMOTA', 'TASMOTA_JSON_POWER_INPUT_MQTT_LABEL_INTERMEDIATE', fallback=None),
            config.get('INTERMEDIATE_TASMOTA', 'TASMOTA_JSON_POWER_OUTPUT_MQTT_LABEL_INTERMEDIATE', fallback=None),
            config.getboolean('INTERMEDIATE_TASMOTA', 'TASMOTA_JSON_POWER_CALCULATE_INTERMEDIATE', fallback=False)
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_SHELLY_EM_INTERMEDIATE'):
        return ShellyEM(shelly_ip, shelly_user, shelly_pass, shelly_emeterindex)
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_SHELLY_3EM_INTERMEDIATE'):
        return Shelly3EM(shelly_ip, shelly_user, shelly_pass, shelly_emeterindex)
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_SHELLY_3EM_PRO_INTERMEDIATE'):
        return Shelly3EMPro(shelly_ip, shelly_user, shelly_pass, shelly_emeterindex)
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_SHELLY_1PM_INTERMEDIATE'):
        return Shelly1PM(shelly_ip, shelly_user, shelly_pass, shelly_emeterindex)
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_SHELLY_PLUS_1PM_INTERMEDIATE'):
        return ShellyPlus1PM(shelly_ip, shelly_user, shelly_pass, shelly_emeterindex)
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_ESPHOME_INTERMEDIATE'):
        return ESPHome(
            config.get('INTERMEDIATE_ESPHOME', 'ESPHOME_IP_INTERMEDIATE'),
            config.get('INTERMEDIATE_ESPHOME', 'ESPHOME_PORT_INTERMEDIATE', fallback='80'),
            config.get('INTERMEDIATE_ESPHOME', 'ESPHOME_DOMAIN_INTERMEDIATE'),
            config.get('INTERMEDIATE_ESPHOME', 'ESPHOME_ID_INTERMEDIATE')
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_SHRDZM_INTERMEDIATE'):
        return Shrdzm(
            config.get('INTERMEDIATE_SHRDZM', 'SHRDZM_IP_INTERMEDIATE'),
            config.get('INTERMEDIATE_SHRDZM', 'SHRDZM_USER_INTERMEDIATE'),
            config.get('INTERMEDIATE_SHRDZM', 'SHRDZM_PASS_INTERMEDIATE')
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_EMLOG_INTERMEDIATE'):
        return Emlog(
            config.get('INTERMEDIATE_EMLOG', 'EMLOG_IP_INTERMEDIATE'),
            config.get('INTERMEDIATE_EMLOG', 'EMLOG_METERINDEX_INTERMEDIATE'),
            config.getboolean('INTERMEDIATE_EMLOG', 'EMLOG_JSON_POWER_CALCULATE', fallback=False)
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_IOBROKER_INTERMEDIATE'):
        return IoBroker(
            config.get('INTERMEDIATE_IOBROKER', 'IOBROKER_IP_INTERMEDIATE'),
            config.get('INTERMEDIATE_IOBROKER', 'IOBROKER_PORT_INTERMEDIATE'),
            config.get('INTERMEDIATE_IOBROKER', 'IOBROKER_CURRENT_POWER_ALIAS_INTERMEDIATE'),
            config.getboolean('INTERMEDIATE_IOBROKER', 'IOBROKER_POWER_CALCULATE', fallback=False),
            config.get('INTERMEDIATE_IOBROKER', 'IOBROKER_POWER_INPUT_ALIAS_INTERMEDIATE', fallback=None),
            config.get('INTERMEDIATE_IOBROKER', 'IOBROKER_POWER_OUTPUT_ALIAS_INTERMEDIATE', fallback=None)
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_HOMEASSISTANT_INTERMEDIATE'):
        return HomeAssistant(
            config.get('INTERMEDIATE_HOMEASSISTANT', 'HA_IP_INTERMEDIATE'),
            config.get('INTERMEDIATE_HOMEASSISTANT', 'HA_PORT_INTERMEDIATE'),
            config.getboolean('INTERMEDIATE_HOMEASSISTANT', 'HA_HTTPS_INTERMEDIATE', fallback=False),
            config.get('INTERMEDIATE_HOMEASSISTANT', 'HA_ACCESSTOKEN_INTERMEDIATE'),
            config.get('INTERMEDIATE_HOMEASSISTANT', 'HA_CURRENT_POWER_ENTITY_INTERMEDIATE'),
            config.getboolean('INTERMEDIATE_HOMEASSISTANT', 'HA_POWER_CALCULATE_INTERMEDIATE', fallback=False),
            config.get('INTERMEDIATE_HOMEASSISTANT', 'HA_POWER_INPUT_ALIAS_INTERMEDIATE', fallback=None),
            config.get('INTERMEDIATE_HOMEASSISTANT', 'HA_POWER_OUTPUT_ALIAS_INTERMEDIATE', fallback=None)
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_VZLOGGER_INTERMEDIATE'):
        return VZLogger(
            config.get('INTERMEDIATE_VZLOGGER', 'VZL_IP_INTERMEDIATE'),
            config.get('INTERMEDIATE_VZLOGGER', 'VZL_PORT_INTERMEDIATE'),
            config.get('INTERMEDIATE_VZLOGGER', 'VZL_UUID_INTERMEDIATE')
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_SCRIPT_INTERMEDIATE'):
        return Script(
            config.get('INTERMEDIATE_SCRIPT', 'SCRIPT_FILE_INTERMEDIATE'),
            config.get('INTERMEDIATE_SCRIPT', 'SCRIPT_IP_INTERMEDIATE'),
            config.get('INTERMEDIATE_SCRIPT', 'SCRIPT_USER_INTERMEDIATE'),
            config.get('INTERMEDIATE_SCRIPT', 'SCRIPT_PASS_INTERMEDIATE')
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_MQTT_INTERMEDIATE'):
        return MqttPowermeter(
            config.get('INTERMEDIATE_MQTT', 'MQTT_BROKER', fallback=config.get("MQTT_CONFIG", "MQTT_BROKER", fallback=None)),
            config.getint('INTERMEDIATE_MQTT', 'MQTT_PORT', fallback=config.getint("MQTT_CONFIG", "MQTT_PORT", fallback=1883)),
            config.get('INTERMEDIATE_MQTT', 'MQTT_TOPIC_INCOMING'),
            config.get('INTERMEDIATE_MQTT', 'MQTT_JSON_PATH_INCOMING', fallback=None),
            config.get('INTERMEDIATE_MQTT', 'MQTT_TOPIC_OUTGOING', fallback=None),
            config.get('INTERMEDIATE_MQTT', 'MQTT_JSON_PATH_OUTGOING', fallback=None),
            config.get('INTERMEDIATE_MQTT', 'MQTT_USERNAME', fallback=config.get("MQTT_CONFIG", "MQTT_USERNAME", fallback=None)),
            config.get('INTERMEDIATE_MQTT', 'MQTT_PASSWORD', fallback=config.get("MQTT_CONFIG", "MQTT_PASSWORD", fallback=None))
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_AMIS_READER_INTERMEDIATE'):
        return AmisReader(
            config.get('INTERMEDIATE_AMIS_READER', 'AMIS_READER_IP_INTERMEDIATE')
        )
    elif config.getboolean('SELECT_INTERMEDIATE_METER', 'USE_DEBUG_READER_INTERMEDIATE'):
        return DebugReader()
    else:
        return dtu

def CreateDTU() -> DTU:
    inverter_count = config.getint('COMMON', 'INVERTER_COUNT')
    if config.getboolean('SELECT_DTU', 'USE_AHOY'):
        return AhoyDTU(
            inverter_count,
            config.get('AHOY_DTU', 'AHOY_IP'),
            config.get('AHOY_DTU', 'AHOY_PASS', fallback='')
        )
    elif config.getboolean('SELECT_DTU', 'USE_OPENDTU'):
        return OpenDTU(
            inverter_count,
            config.get('OPEN_DTU', 'OPENDTU_IP'),
            config.get('OPEN_DTU', 'OPENDTU_USER'),
            config.get('OPEN_DTU', 'OPENDTU_PASS')
        )
    elif config.getboolean('SELECT_DTU', 'USE_DEBUG'):
        return DebugDTU(
            inverter_count
        )    
    else:
        raise Exception("Error: no DTU defined!")

# ----- START -----
logger.info("Author:         %s / Script Version: %s",__author__, __version__)
logger.info("Contributor(s): %s", __contributors__)

# read config:
logger.info("read config file: " + str(Path.joinpath(Path(__file__).parent.resolve(), "HoymilesZeroExport_Config.ini")))
if args.config:
    logger.info("read additional config file: " + args.config)

VERSION = config.get('VERSION', 'VERSION')
logger.info("Config file V %s", VERSION)

MAX_RETRIES = config.getint('COMMON', 'MAX_RETRIES', fallback=3)
RETRY_STATUS_CODES = config.get('COMMON', 'RETRY_STATUS_CODES', fallback='500,502,503,504')
RETRY_BACKOFF_FACTOR = config.getfloat('COMMON', 'RETRY_BACKOFF_FACTOR', fallback=0.1)
retry = Retry(total=MAX_RETRIES,
              backoff_factor=RETRY_BACKOFF_FACTOR,
              status_forcelist=[int(status_code) for status_code in RETRY_STATUS_CODES.split(',')],
              allowed_methods={"GET", "POST"})
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

USE_AHOY = config.getboolean('SELECT_DTU', 'USE_AHOY')
USE_OPENDTU = config.getboolean('SELECT_DTU', 'USE_OPENDTU')
AHOY_IP = config.get('AHOY_DTU', 'AHOY_IP')
OPENDTU_IP = config.get('OPEN_DTU', 'OPENDTU_IP')
OPENDTU_USER = config.get('OPEN_DTU', 'OPENDTU_USER')
OPENDTU_PASS = config.get('OPEN_DTU', 'OPENDTU_PASS')
DTU = CreateDTU()
POWERMETER = CreatePowermeter()
INTERMEDIATE_POWERMETER = CreateIntermediatePowermeter(DTU)
INVERTER_COUNT = config.getint('COMMON', 'INVERTER_COUNT')
LOOP_INTERVAL_IN_SECONDS = config.getint('COMMON', 'LOOP_INTERVAL_IN_SECONDS')
SET_LIMIT_TIMEOUT_SECONDS = config.getint('COMMON', 'SET_LIMIT_TIMEOUT_SECONDS')
SET_POWER_STATUS_DELAY_IN_SECONDS = config.getint('COMMON', 'SET_POWER_STATUS_DELAY_IN_SECONDS')
POLL_INTERVAL_IN_SECONDS = config.getint('COMMON', 'POLL_INTERVAL_IN_SECONDS')
MAX_DIFFERENCE_BETWEEN_LIMIT_AND_OUTPUTPOWER = config.getint('COMMON', 'MAX_DIFFERENCE_BETWEEN_LIMIT_AND_OUTPUTPOWER')
SET_POWERSTATUS_CNT = config.getint('COMMON', 'SET_POWERSTATUS_CNT')
SLOW_APPROX_FACTOR_IN_PERCENT = config.getint('COMMON', 'SLOW_APPROX_FACTOR_IN_PERCENT')
LOG_TEMPERATURE = config.getboolean('COMMON', 'LOG_TEMPERATURE')
SET_INVERTER_TO_MIN_ON_POWERMETER_ERROR = config.getboolean('COMMON', 'SET_INVERTER_TO_MIN_ON_POWERMETER_ERROR', fallback=False)
powermeter_target_point = config.getint('CONTROL', 'POWERMETER_TARGET_POINT')
MAX_UNLIMITED_CHARGE_SOC = config.getint('CONTROL', 'MAX_UNLIMITED_CHARGE_SOC')
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
for i in range(INVERTER_COUNT):
    SERIAL_NUMBER.append(config.get('INVERTER_' + str(i + 1), 'SERIAL_NUMBER', fallback=''))
    ENABLED.append(config.getboolean('INVERTER_' + str(i + 1), 'ENABLED', fallback = True))
    NAME.append(str('yet unknown'))
    TEMPERATURE.append(str('--- degC'))
    HOY_MAX_WATT.append(config.getint('INVERTER_' + str(i + 1), 'HOY_MAX_WATT'))
    
    if (config.get('INVERTER_' + str(i + 1), 'HOY_INVERTER_WATT') != ''):
        HOY_INVERTER_WATT.append(config.getint('INVERTER_' + str(i + 1), 'HOY_INVERTER_WATT'))
    else:
        HOY_INVERTER_WATT.append(HOY_MAX_WATT[i])
        
    CURRENT_LIMIT.append(int(-1))
    AVAILABLE.append(bool(False))
    LASTLIMITACKNOWLEDGED.append(bool(False))
    HOY_BATTERY_GOOD_VOLTAGE.append(bool(True))
    HOY_BATTERY_MODE.append(config.getboolean('INVERTER_' + str(i + 1), 'HOY_BATTERY_MODE'))
    HOY_BATTERY_THRESHOLD_OFF_LIMIT_IN_V.append(config.getfloat('INVERTER_' + str(i + 1), 'HOY_BATTERY_THRESHOLD_OFF_LIMIT_IN_V'))
    HOY_BATTERY_THRESHOLD_REDUCE_LIMIT_IN_V.append(config.getfloat('INVERTER_' + str(i + 1), 'HOY_BATTERY_THRESHOLD_REDUCE_LIMIT_IN_V'))
    HOY_BATTERY_THRESHOLD_NORMAL_LIMIT_IN_V.append(config.getfloat('INVERTER_' + str(i + 1), 'HOY_BATTERY_THRESHOLD_NORMAL_LIMIT_IN_V'))
    HOY_BATTERY_THRESHOLD_ON_LIMIT_IN_V.append(config.getfloat('INVERTER_' + str(i + 1), 'HOY_BATTERY_THRESHOLD_ON_LIMIT_IN_V'))
    HOY_COMPENSATE_WATT_FACTOR.append(config.getfloat('INVERTER_' + str(i + 1), 'HOY_COMPENSATE_WATT_FACTOR'))
    HOY_BATTERY_IGNORE_PANELS.append(config.get('INVERTER_' + str(i + 1), 'HOY_BATTERY_IGNORE_PANELS'))
    HOY_PANEL_VOLTAGE_LIST.append([])
    HOY_PANEL_MIN_VOLTAGE_HISTORY_LIST.append([])
    HOY_BATTERY_AVERAGE_CNT.append(config.getint('INVERTER_' + str(i + 1), 'HOY_BATTERY_AVERAGE_CNT', fallback=1))

SLOW_APPROX_LIMIT = cast_to_int(get_max_watt_from_all_inverters() * config.getint('COMMON', 'SLOW_APPROX_LIMIT_IN_PERCENT') / 100)
CONFIG_PROVIDER = ConfigFileConfigProvider(config)
MQTT = None

if config.has_section("MQTT_CONFIG"):
    broker = config.get("MQTT_CONFIG", "MQTT_BROKER")
    port = config.getint("MQTT_CONFIG", "MQTT_PORT", fallback=1883)
    client_id = config.get("MQTT_CONFIG", "MQTT_CLIENT_ID", fallback="HoymilesZeroExport")
    username = config.get("MQTT_CONFIG", "MQTT_USERNAME", fallback=None)
    password = config.get("MQTT_CONFIG", "MQTT_PASSWORD", fallback=None)
    topic_prefix = config.get("MQTT_CONFIG", "MQTT_SET_TOPIC", fallback="zeropower")
    log_level_config_value = config.get("MQTT_CONFIG", "MQTT_LOG_LEVEL", fallback=None)
    mqtt_log_level = logging.getLevelName(log_level_config_value) if log_level_config_value else None
    MQTT = MqttHandler(broker, port, client_id, username, password, topic_prefix, mqtt_log_level)

    if mqtt_log_level is not None:
        class MqttLogHandler(logging.Handler):
            def emit(self, record):
                MQTT.publish_log_record(record)

        logger.addHandler(MqttLogHandler())

    CONFIG_PROVIDER = ConfigProviderChain([MQTT, CONFIG_PROVIDER])

try:
    logger.info("---Init---")
    new_limit_setpoint = 0
    DTU.check_min_version()
    if get_hoymiles_available():
        for i in range(INVERTER_COUNT):
            set_hoymiles_power_status(i, True)
        new_limit_setpoint = get_min_watt_from_all_inverters()
        set_limit(new_limit_setpoint)
        get_hoymiles_actual_power()
        get_check_battery()
    get_powermeter_watts()
except Exception as e:
    if hasattr(e, 'message'):
        logger.error(e.message)
    else:
        logger.error(e)
    time.sleep(LOOP_INTERVAL_IN_SECONDS)
logger.info("---Start Zero Export---")

while True:
    socResult = session.get(
        'http://192.168.37.6:8093/v1/state/growatt.0.2276541.devices.CYF6CF4005.statusData.SOC').json()
    soc = socResult['val']

    socResult = session.get(
        'http://192.168.37.6:8093/v1/state/growatt.0.2276541.devices.CYF6CF4005.historyLast.batteryTemperature').json()
    bat_temperature = socResult['val']

    json = session.get(
        'http://192.168.37.6:8093/v1/state/growatt.0.2276541.devices.CYF6CF4005.statusData.pdisCharge1').json()
    discharge_watts = json['val'] * 1000.0

    json = session.get(
        'http://192.168.37.6:8093/v1/state/growatt.0.2276541.devices.CYF6CF4005.statusData.chargePower').json()
    chargePower = json['val'] * 1000.0

    CONFIG_PROVIDER.update()
    publish_config_state()
    on_grid_usage_jump_to_limit_percent = CONFIG_PROVIDER.on_grid_usage_jump_to_limit_percent()
    on_grid_feed_fast_limit_decrease = CONFIG_PROVIDER.on_grid_feed_fast_limit_decrease()    
    powermeter_target_point = CONFIG_PROVIDER.get_powermeter_target_point()
    powermeter_max_point = CONFIG_PROVIDER.get_powermeter_max_point()
    powermeter_min_point = CONFIG_PROVIDER.get_powermeter_min_point()
    powermeter_tolerance = CONFIG_PROVIDER.get_powermeter_tolerance()
    if powermeter_max_point < (powermeter_target_point + powermeter_tolerance):
        powermeter_max_point = powermeter_target_point + powermeter_tolerance + 50
        logger.info(
            'Warning: POWERMETER_MAX_POINT < POWERMETER_TARGET_POINT + POWERMETER_TOLERANCE. Setting POWERMETER_MAX_POINT to ' + str(
                powermeter_max_point))

    try:
        hoymiles_actual_power = get_hoymiles_actual_power()
        previous_limit_setpoint = new_limit_setpoint
        if get_hoymiles_available() and get_check_battery():
            if LOG_TEMPERATURE:
                get_hoymiles_temperature()
            for x in range(cast_to_int(LOOP_INTERVAL_IN_SECONDS / POLL_INTERVAL_IN_SECONDS)):
                powermeter_watts = get_powermeter_watts()
                if powermeter_watts > powermeter_max_point:
                    if on_grid_usage_jump_to_limit_percent > 0:
                        new_limit_setpoint = cast_to_int(get_max_inverter_watt_from_all_inverters() * on_grid_usage_jump_to_limit_percent / 100)
                        if (new_limit_setpoint <= previous_limit_setpoint) and (on_grid_usage_jump_to_limit_percent != 100):
                            new_limit_setpoint = previous_limit_setpoint + powermeter_watts - powermeter_target_point
                    else:
                        new_limit_setpoint = previous_limit_setpoint + powermeter_watts - powermeter_target_point
                    new_limit_setpoint = check_and_apply_upper_and_lower_limits(new_limit_setpoint)
                    set_limit(new_limit_setpoint)
                    remaining_delay = cast_to_int((LOOP_INTERVAL_IN_SECONDS / POLL_INTERVAL_IN_SECONDS - x) * POLL_INTERVAL_IN_SECONDS)
                    if remaining_delay > 0:
                        time.sleep(remaining_delay)
                        break
                elif (powermeter_watts < powermeter_min_point) and on_grid_feed_fast_limit_decrease:
                    new_limit_setpoint = previous_limit_setpoint + powermeter_watts - powermeter_target_point
                    new_limit_setpoint = check_and_apply_upper_and_lower_limits(new_limit_setpoint)
                    set_limit(new_limit_setpoint)
                    remaining_delay = cast_to_int((LOOP_INTERVAL_IN_SECONDS / POLL_INTERVAL_IN_SECONDS - x) * POLL_INTERVAL_IN_SECONDS)
                    if remaining_delay > 0:
                        time.sleep(remaining_delay)
                        break
                else:
                    time.sleep(POLL_INTERVAL_IN_SECONDS)

            if MAX_DIFFERENCE_BETWEEN_LIMIT_AND_OUTPUTPOWER != 100:
                cut_limit = cut_limit_to_production(new_limit_setpoint)
                if cut_limit != new_limit_setpoint:
                    new_limit_setpoint = cut_limit
                    previous_limit_setpoint = new_limit_setpoint

            if powermeter_watts > powermeter_max_point:
                continue

            # producing too much power: reduce limit
            if powermeter_watts < (powermeter_target_point - powermeter_tolerance):
                if previous_limit_setpoint >= get_max_watt_from_all_inverters():
                    new_limit_setpoint = hoymiles_actual_power + powermeter_watts - powermeter_target_point
                    limit_difference = abs(hoymiles_actual_power - new_limit_setpoint)
                    if limit_difference > SLOW_APPROX_LIMIT:
                        new_limit_setpoint = new_limit_setpoint + (limit_difference * SLOW_APPROX_FACTOR_IN_PERCENT / 100)
                    if new_limit_setpoint > hoymiles_actual_power:
                        new_limit_setpoint = hoymiles_actual_power
                    logger.info("overproducing: reduce limit based on actual power")
                else:
                    new_limit_setpoint = previous_limit_setpoint + powermeter_watts - powermeter_target_point
                    # check if it is necessary to approximate to the setpoint with some more passes. this reduces overshoot
                    limit_difference = abs(previous_limit_setpoint - new_limit_setpoint)
                    if limit_difference > SLOW_APPROX_LIMIT:
                        logger.info("overproducing: reduce limit based on previous limit setpoint by approximation")
                        new_limit_setpoint = new_limit_setpoint + (limit_difference * SLOW_APPROX_FACTOR_IN_PERCENT / 100)
                    else:
                        logger.info("overproducing: reduce limit based on previous limit setpoint")

            # producing too little power: increase limit
            elif powermeter_watts > (powermeter_target_point + powermeter_tolerance):
                if previous_limit_setpoint < get_max_watt_from_all_inverters():
                    new_limit_setpoint = previous_limit_setpoint + powermeter_watts - powermeter_target_point
                    logger.info("Not enough energy producing: increasing limit")
                else:
                    logger.info("Not enough energy producing: limit already at maximum")

            # Check battery SoC and increase the limit, if necessary
            total_rated_power = get_max_watt_from_all_inverters()
            if discharge_watts > 0 and new_limit_setpoint < (total_rated_power - discharge_watts):
                new_limit_setpoint = discharge_watts * 1.1 + hoymiles_actual_power
                logger.warning(f'Increasing limit to current discharge plus margin: {new_limit_setpoint}W')
            else:
                logger.warning(f'Leaving set point at: {new_limit_setpoint}')

            # In principle, we do not need to adjust limits before the battery is full;
            # however, there might be a temperature limitation which is cared for below.
            limit_active = True
            if soc < MAX_UNLIMITED_CHARGE_SOC:
                new_limit_setpoint = powermeter_watts + powermeter_tolerance
                limit_active = False
                set_hoymiles_power_status(0, True)
                set_hoymiles_power_status(1, True)
                set_hoymiles_power_status(2, True)
            elif soc >= MAX_UNLIMITED_CHARGE_SOC and soc < 97:
                set_hoymiles_power_status(0, False)
                set_hoymiles_power_status(1, True)
                set_hoymiles_power_status(2, True)
            elif soc >= 97 and soc < 99:
                set_hoymiles_power_status(0, False)
                set_hoymiles_power_status(1, False)
                set_hoymiles_power_status(2, True)
            elif soc >= 99:
                set_hoymiles_power_status(0, False)
                set_hoymiles_power_status(1, False)
                set_hoymiles_power_status(2, False)

            # adjust the limit with battery temperature
            temperature_degradation = True
            if bat_temperature <= 0:
                new_limit_setpoint = powermeter_watts
            elif bat_temperature > 0 and bat_temperature <= 10.0:
                new_limit_setpoint = powermeter_watts + 510
            elif bat_temperature > 10 and bat_temperature <= 20.0:
                new_limit_setpoint = powermeter_watts + 2100
            elif bat_temperature > 20.0 and bat_temperature <= 25.0:
                new_limit_setpoint = powermeter_watts + 3200
            else:
                temperature_degradation = False

            # check for upper and lower limits
            new_limit_setpoint = check_and_apply_upper_and_lower_limits(new_limit_setpoint)

            # set new limit to inverter
            set_limit(new_limit_setpoint)

            # Log to console and publish to MQTT
            logger.info(f'Power Consumption  : {powermeter_watts}W')
            logger.info(f'PV Production      : {hoymiles_actual_power}W')
            logger.info(f'Battery Temperature: {bat_temperature}ºC')
            logger.info(f'Battery SoC        : {soc}%')
            logger.info(f'Total Rated Power  : {total_rated_power}W')
            logger.info(f'Inverter Limit     : {new_limit_setpoint}W')
            logger.info(f'Temp. Degradation? : {temperature_degradation}')
            logger.info(f'Limit Active?      : {limit_active}')
            if discharge_watts > 0:
                logger.info(f'Discharge Rate     : {discharge_watts}W')
            elif chargePower > 0:
                logger.info(f'Charge Rate        : {chargePower}W')

            publish_global_state('total_limit_w', new_limit_setpoint)
            publish_global_state('total_rated_power_w', total_rated_power)
            publish_global_state('battery_soc_percent', soc)
            publish_global_state('battery_cell_temperature_c', bat_temperature)
        else:
            if hasattr(set_limit, "LastLimit"):
                set_limit.LastLimit = -1
            time.sleep(LOOP_INTERVAL_IN_SECONDS)

    except Exception as e:
        if hasattr(e, 'message'):
            logger.error(e.message)
        else:
            logger.error(e)
        time.sleep(LOOP_INTERVAL_IN_SECONDS)

