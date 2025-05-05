# control/dtus.py

"""
This module contains the metering classes where the implementation is a true
metering and not a control device. Subclasses that control things reside in the 'control' package.
"""
import time

from packaging import version
from requests.auth import HTTPBasicAuth

from GLOBALS import *
from metering.powermeters import Powermeter
from utils.helper_functions import cast_to_int, get_number_array


class DTU(Powermeter):
    def __init__(self, inverter_count: int):
        self.inverter_count = inverter_count

    def get_ac_power(self, p_inverter_id: int):
        raise NotImplementedError()

    def get_powermeter_watts(self):
        return sum(self.get_ac_power(p_inverter_id) for p_inverter_id in range(self.inverter_count) if
                   AVAILABLE[p_inverter_id] and HOY_BATTERY_GOOD_VOLTAGE[p_inverter_id])

    def check_min_version(self):
        raise NotImplementedError()

    def get_available(self, p_inverter_id: int):
        raise NotImplementedError()

    def get_actual_limit_in_w(self, p_inverter_id: int):
        raise NotImplementedError()

    def get_info(self, p_inverter_id: int):
        raise NotImplementedError()

    def get_temperature(self, p_inverter_id: int):
        raise NotImplementedError()

    def get_panel_min_voltage(self, p_inverter_id: int):
        raise NotImplementedError()

    def wait_for_ack(self, p_inverter_id: int, p_timeout_in_s: int):
        raise NotImplementedError()

    def set_limit(self, p_inverter_id: int, p_limit: int):
        raise NotImplementedError()

    def set_power_status(self, p_inverter_id: int, p_active: bool):
        raise NotImplementedError()


class AhoyDTU(DTU):
    def __init__(self, inverter_count: int, ip: str, password: str):
        super().__init__(inverter_count)
        self.ip = ip
        self.password = password
        self.token = ''

    def get_json(self, path):
        url = f'http://{self.ip}{path}'
        # AhoyDTU sometimes returns literal 'null' instead of a valid json, so we retry a few times
        data = None
        retry_count = 3
        while retry_count > 0 and data is None:
            data = session.get(url, timeout=10).json()
            retry_count -= 1
        return data

    def get_response_json(self, path, obj):
        url = f'http://{self.ip}{path}'
        return session.post(url, json=obj, timeout=10).json()

    def get_ac_power(self, p_inverter_id):
        parsed_data = self.get_json('/api/live')
        actual_power_index = parsed_data["ch0_fld_names"].index("P_AC")
        parsed_data = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        return cast_to_int(parsed_data["ch"][0][actual_power_index])

    def CheckMinVersion(self):
        min_version = '0.8.80'
        parsed_data = self.get_json('/api/system')
        try:
            ahoy_version = str((parsed_data["version"]))
        except:
            ahoy_version = str((parsed_data["generic"]["version"]))

        logger.info('Ahoy: Current Version: %s', ahoy_version)
        if version.parse(ahoy_version) < version.parse(min_version):
            logger.error(
                'Error: Your AHOY Version is too old! Please update at least to Version %s - you can find the newest dev-releases here: https://github.com/lumapu/ahoy/actions',
                min_version)
            quit()

    def get_available(self, p_inverter_id: int):
        parsed_data = self.get_json('/api/index')
        available = bool(parsed_data["inverter"][p_inverter_id]["is_avail"])
        logger.info('Ahoy: Inverter "%s" Available: %s', NAME[p_inverter_id], available)
        return available

    def get_actual_limit_in_w(self, p_inverter_id: int):
        parsed_data = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        limit_in_percent = float(parsed_data['power_limit_read'])
        limit_in_w = HOY_INVERTER_WATT[p_inverter_id] * limit_in_percent / 100
        return limit_in_w

    def get_info(self, p_inverter_id: int):
        parsed_data = self.get_json('/api/live')
        temp_index = parsed_data["ch0_fld_names"].index("Temp")

        parsed_data = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        SERIAL_NUMBER[p_inverter_id] = str(parsed_data['serial'])
        NAME[p_inverter_id] = str(parsed_data['name'])
        TEMPERATURE[p_inverter_id] = str(parsed_data["ch"][0][temp_index]) + ' degC'
        logger.info('Ahoy: Inverter "%s" / serial number "%s" / temperature %s', NAME[p_inverter_id],
                    SERIAL_NUMBER[p_inverter_id], TEMPERATURE[p_inverter_id])

    def get_temperature(self, p_inverter_id: int):
        parsed_data = self.get_json('/api/live')
        temp_index = parsed_data["ch0_fld_names"].index("Temp")

        parsed_data = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        TEMPERATURE[p_inverter_id] = str(parsed_data["ch"][0][temp_index]) + ' degC'
        logger.info('Ahoy: Inverter "%s" temperature: %s', NAME[p_inverter_id], TEMPERATURE[p_inverter_id])

    def get_panel_min_voltage(self, p_inverter_id: int):
        parsed_data = self.get_json('/api/live')
        panel_vdc_index = parsed_data["fld_names"].index("U_DC")

        parsed_data = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        panel_vdc = []
        excluded_panels = get_number_array(HOY_BATTERY_IGNORE_PANELS[p_inverter_id])
        for i in range(1, len(parsed_data['ch']), 1):
            if i not in excluded_panels:
                panel_vdc.append(float(parsed_data['ch'][i][panel_vdc_index]))
        min_vdc = float('inf')
        for i in range(len(panel_vdc)):
            if (min_vdc > panel_vdc[i]) and (panel_vdc[i] > 5):
                min_vdc = panel_vdc[i]
        if min_vdc == float('inf'):
            min_vdc = 0

        # save last 5 min-values in list and return the "highest" value.
        HOY_PANEL_VOLTAGE_LIST[p_inverter_id].append(min_vdc)
        if len(HOY_PANEL_VOLTAGE_LIST[p_inverter_id]) > 5:
            HOY_PANEL_VOLTAGE_LIST[p_inverter_id].pop(0)
        max_value = None
        for num in HOY_PANEL_VOLTAGE_LIST[p_inverter_id]:
            if max_value is None or num > max_value:
                max_value = num

        logger.info('Lowest panel voltage inverter "%s": %s Volt', NAME[p_inverter_id], max_value)
        return max_value

    def wait_for_ack(self, p_inverter_id: int, p_timeout_in_s: int):
        try:
            timeout = p_timeout_in_s
            timeout_start = time.time()
            ack = False
            while time.time() < timeout_start + timeout:
                time.sleep(0.5)
                ParsedData = self.get_json(f'/api/inverter/id/{p_inverter_id}')
                ack = bool(ParsedData['power_limit_ack'])
                if ack:
                    break
            if ack:
                logger.info('Ahoy: Inverter "%s": Limit acknowledged', NAME[p_inverter_id])
            else:
                logger.info('Ahoy: Inverter "%s": Limit timeout!', NAME[p_inverter_id])
            return ack
        except Exception as e:
            if hasattr(e, 'message'):
                logger.error('Ahoy: Inverter "%s" WaitForAck: "%s"', NAME[p_inverter_id], e.message)
            else:
                logger.error('Ahoy: Inverter "%s" WaitForAck: "%s"', NAME[p_inverter_id], e)
            return False

    def set_limit(self, p_inverter_id: int, p_limit: int):
        logger.info('Ahoy: Inverter "%s": setting new limit from %s Watt to %s Watt', NAME[p_inverter_id],
                    cast_to_int(CURRENT_LIMIT[p_inverter_id]), cast_to_int(p_limit))
        myobj = {'cmd': 'limit_nonpersistent_absolute', 'val': p_limit, "id": p_inverter_id, "token": self.token}
        response = self.get_response_json('/api/ctrl', myobj)
        if response["success"] == False and response["error"] == "ERR_PROTECTED":
            self.authenticate()
            self.set_limit(p_inverter_id, p_limit)
            return
        if response["success"] == False:
            raise Exception("Error: SetLimitAhoy Request error")
        CURRENT_LIMIT[p_inverter_id] = p_limit

    def set_power_status(self, p_inverter_id: int, p_active: bool):
        if p_active:
            logger.info('Ahoy: Inverter "%s": Turn on', NAME[p_inverter_id])
        else:
            logger.info('Ahoy: Inverter "%s": Turn off', NAME[p_inverter_id])
        myobj = {'cmd': 'power', 'val': cast_to_int(p_active == True), "id": p_inverter_id, "token": self.token}
        response = self.get_response_json('/api/ctrl', myobj)
        if response["success"] == False and response["error"] == "ERR_PROTECTED":
            self.authenticate()
            self.set_power_status(p_inverter_id, p_active)
            return
        if not response["success"]:
            raise Exception("Error: SetPowerStatus Request error")

    def authenticate(self):
        logger.info('Ahoy: Authenticating...')
        myobj = {'auth': self.password}
        response = self.get_response_json('/api/ctrl', myobj)
        if not response["success"]:
            raise Exception("Error: Authenticate Request error")
        self.token = response["token"]
        logger.info('Ahoy: Authenticating successful, received Token: %s', self.token)


class OpenDTU(DTU):
    def __init__(self, inverter_count: int, ip: str, user: str, password: str):
        super().__init__(inverter_count)
        self.ip = ip
        self.user = user
        self.password = password

    def get_json(self, path):
        url = f'http://{self.ip}{path}'
        return session.get(url, auth=HTTPBasicAuth(self.user, self.password), timeout=10).json()

    def get_response_json(self, path, send_str):
        url = f'http://{self.ip}{path}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        return session.post(url=url, headers=headers, data=send_str, auth=HTTPBasicAuth(self.user, self.password),
                            timeout=10).json()

    def get_ac_power(self, p_inverter_id):
        parsed_data = self.get_json(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        return cast_to_int(parsed_data['inverters'][0]['AC']['0']['Power']['v'])

    def check_min_version(self):
        min_version = 'v24.2.12'
        parsed_data = self.get_json('/api/system/status')
        open_dtu_version = str((parsed_data["git_hash"]))
        if ("-Database" in open_dtu_version):  # trim string "v24.5.27-Database"
            open_dtu_version = open_dtu_version.replace("-Database", "")
        logger.info('OpenDTU: Current Version: %s', open_dtu_version)
        if version.parse(open_dtu_version) < version.parse(min_version):
            logger.error(
                'Error: Your OpenDTU Version is too old! Please update at least to Version %s - you can find the newest dev-releases here: https://github.com/tbnobody/OpenDTU/actions',
                min_version)
            quit()

        return

    def get_available(self, p_inverter_id: int):
        parsed_data = self.get_json(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        reachable = bool(parsed_data['inverters'][0]["reachable"])
        logger.info('OpenDTU: Inverter "%s" reachable: %s', NAME[p_inverter_id], reachable)
        return reachable

    def get_actual_limit_in_w(self, p_inverter_id: int):
        parsed_data = self.get_json('/api/limit/status')
        limit_relative = float(parsed_data[SERIAL_NUMBER[p_inverter_id]]['limit_relative'])
        limit_in_w = HOY_INVERTER_WATT[p_inverter_id] * limit_relative / 100
        return limit_in_w

    def get_info(self, p_inverter_id: int):
        if SERIAL_NUMBER[p_inverter_id] == '':
            parsed_data = self.get_json('/api/livedata/status')
            SERIAL_NUMBER[p_inverter_id] = str(parsed_data['inverters'][p_inverter_id]['serial'])

        parsed_data = self.get_json(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        TEMPERATURE[p_inverter_id] = str(
            round(float((parsed_data['inverters'][0]['INV']['0']['Temperature']['v'])), 1)) + ' degC'
        NAME[p_inverter_id] = str(parsed_data['inverters'][0]['name'])
        logger.info('OpenDTU: Inverter "%s" / serial number "%s" / temperature %s', NAME[p_inverter_id],
                    SERIAL_NUMBER[p_inverter_id], TEMPERATURE[p_inverter_id])
        return

    def get_temperature(self, p_inverter_id: int):
        parsed_data = self.get_json(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        TEMPERATURE[p_inverter_id] = str(
            round(float((parsed_data['inverters'][0]['INV']['0']['Temperature']['v'])), 1)) + ' degC'
        logger.info('OpenDTU: Inverter "%s" temperature: %s', NAME[p_inverter_id], TEMPERATURE[p_inverter_id])
        return

    def get_panel_min_voltage(self, p_inverter_id: int):
        parsed_data = self.get_json(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        panel_vdc = []
        excluded_panels = get_number_array(HOY_BATTERY_IGNORE_PANELS[p_inverter_id])
        for i in range(len(parsed_data['inverters'][0]['DC'])):
            if i not in excluded_panels:
                panel_vdc.append(float(parsed_data['inverters'][0]['DC'][str(i)]['Voltage']['v']))
        min_vdc = float('inf')
        for i in range(len(panel_vdc)):
            if (min_vdc > panel_vdc[i]) and (panel_vdc[i] > 5):
                min_vdc = panel_vdc[i]
        if min_vdc == float('inf'):
            min_vdc = 0

        # save last 5 min-values in list and return the "highest" value.
        HOY_PANEL_VOLTAGE_LIST[p_inverter_id].append(min_vdc)
        if len(HOY_PANEL_VOLTAGE_LIST[p_inverter_id]) > 5:
            HOY_PANEL_VOLTAGE_LIST[p_inverter_id].pop(0)
        max_value = None
        for num in HOY_PANEL_VOLTAGE_LIST[p_inverter_id]:
            if (max_value is None or num > max_value):
                max_value = num

        return max_value

    def wait_for_ack(self, p_inverter_id: int, p_timeout_in_s: int):
        try:
            timeout = p_timeout_in_s
            timeout_start = time.time()
            ack = False
            while time.time() < timeout_start + timeout:
                time.sleep(0.5)
                parsed_data = self.get_json('/api/limit/status')
                ack = (parsed_data[SERIAL_NUMBER[p_inverter_id]]['limit_set_status'] == 'Ok')
                if ack:
                    break
            if ack:
                logger.info('OpenDTU: Inverter "%s": Limit acknowledged', NAME[p_inverter_id])
            else:
                logger.info('OpenDTU: Inverter "%s": Limit timeout!', NAME[p_inverter_id])
            return ack
        except Exception as e:
            if hasattr(e, 'message'):
                logger.error('OpenDTU: Inverter "%s" WaitForAck: "%s"', NAME[p_inverter_id], e.message)
            else:
                logger.error('OpenDTU: Inverter "%s" WaitForAck: "%s"', NAME[p_inverter_id], e)
            return False

    def set_limit(self, p_inverter_id: int, p_limit: int):
        logger.info('OpenDTU: Inverter "%s": setting new limit from %s Watt to %s Watt', NAME[p_inverter_id],
                    cast_to_int(CURRENT_LIMIT[p_inverter_id]), cast_to_int(p_limit))
        rel_limit = cast_to_int(p_limit / HOY_INVERTER_WATT[p_inverter_id] * 100)
        my_send_str = f'''data={{"serial":"{SERIAL_NUMBER[p_inverter_id]}", "limit_type":1, "limit_value":{rel_limit}}}'''
        response = self.get_response_json('/api/limit/config', my_send_str)
        if response['type'] != 'success':
            raise Exception(f"Error: SetLimit error: {response['message']}")
        CURRENT_LIMIT[p_inverter_id] = p_limit
        return

    def set_power_status(self, p_inverter_id: int, p_active: bool):
        if p_active:
            logger.info('OpenDTU: Inverter "%s": Turn on', NAME[p_inverter_id])
        else:
            logger.info('OpenDTU: Inverter "%s": Turn off', NAME[p_inverter_id])

        logger.debug(f'POWER: {p_active}')
        my_send_str = f'''data={{"serial":"{SERIAL_NUMBER[p_inverter_id]}", "power":{str(p_active).lower()}}}'''
        logger.info(my_send_str)
        response = self.get_response_json('/api/power/config', my_send_str)
        if response['type'] != 'success':
            raise Exception(f"Error: SetPowerStatus error: {response['message']}")


class DebugDTU(DTU):
    def __init__(self, inverter_count: int):
        super().__init__(inverter_count)

    def get_ac_power(self, p_inverter_id):
        return cast_to_int(input("Current AC-Power: "))

    def check_min_version(self):
        return

    def get_available(self, p_inverter_id: int):
        logger.info('Debug: Inverter "%s" Available: %s', NAME[p_inverter_id], True)
        return True

    def get_actual_limit_in_w(self, p_inverter_id: int):
        return cast_to_int(input("Current InverterLimit: "))

    def get_info(self, p_inverter_id: int):
        SERIAL_NUMBER[p_inverter_id] = str(p_inverter_id)
        NAME[p_inverter_id] = str(p_inverter_id)
        TEMPERATURE[p_inverter_id] = '0 degC'
        logger.info('Debug: Inverter "%s" / serial number "%s" / temperature %s', NAME[p_inverter_id],
                    SERIAL_NUMBER[p_inverter_id], TEMPERATURE[p_inverter_id])

    def get_temperature(self, p_inverter_id: int):
        TEMPERATURE[p_inverter_id] = 0
        logger.info('Debug: Inverter "%s" temperature: %s', NAME[p_inverter_id], TEMPERATURE[p_inverter_id])

    def get_panel_min_voltage(self, p_inverter_id: int):
        logger.info('Lowest panel voltage inverter "%s": %s Volt', NAME[p_inverter_id], 90)
        return 90

    def wait_for_ack(self, p_inverter_id: int, p_timeout_in_s: int):
        return True

    def set_limit(self, p_inverter_id: int, p_limit: int):
        logger.info('Debug: Inverter "%s": setting new limit from %s Watt to %s Watt', NAME[p_inverter_id],
                    cast_to_int(CURRENT_LIMIT[p_inverter_id]), cast_to_int(p_limit))
        CURRENT_LIMIT[p_inverter_id] = p_limit

    def set_power_status(self, p_inverter_id: int, p_active: bool):
        if p_active:
            logger.info('Debug: Inverter "%s": Turn on', NAME[p_inverter_id])
        else:
            logger.info('Debug: Inverter "%s": Turn off', NAME[p_inverter_id])

    def authenticate(self):
        logger.info('Debug: Authenticating...')
        self.token = '12345'
        logger.info('Debug: Authenticating successful, received Token: %s', self.token)
