from GLOBALS import *
from metering.powermeters import *
from control.dtus import *


class Factory:
    def __init__(self):
        return

    @staticmethod
    def create_powermeter() -> Powermeter:
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

    @staticmethod
    def create_intermediate_powermeter(dtu: DTU) -> Powermeter:
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

    @staticmethod
    def create_dtu() -> DTU:
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

