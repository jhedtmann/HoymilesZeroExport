# control/dtus.py

"""
This module contains the metering classes where the implementation is a true
metering and not a control device. Subclasses that control things reside in the 'control' package.
"""
from packaging import version

from HoymilesZeroExport import AVAILABLE, HOY_BATTERY_GOOD_VOLTAGE, session, logger, NAME, HOY_INVERTER_WATT, \
    SERIAL_NUMBER, TEMPERATURE
from metering.powermeters import Powermeter
from utils.helper_functions import cast_to_int


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
        self.Token = ''

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
        ParsedData = self.get_json('/api/index')
        Available = bool(ParsedData["inverter"][p_inverter_id]["is_avail"])
        logger.info('Ahoy: Inverter "%s" Available: %s', NAME[p_inverter_id], Available)
        return Available

    def get_actual_limit_in_w(self, p_inverter_id: int):
        ParsedData = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        LimitInPercent = float(ParsedData['power_limit_read'])
        LimitInW = HOY_INVERTER_WATT[p_inverter_id] * LimitInPercent / 100
        return LimitInW

    def get_info(self, p_inverter_id: int):
        ParsedData = self.get_json('/api/live')
        temp_index = ParsedData["ch0_fld_names"].index("Temp")

        ParsedData = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        SERIAL_NUMBER[p_inverter_id] = str(ParsedData['serial'])
        NAME[p_inverter_id] = str(ParsedData['name'])
        TEMPERATURE[p_inverter_id] = str(ParsedData["ch"][0][temp_index]) + ' degC'
        logger.info('Ahoy: Inverter "%s" / serial number "%s" / temperature %s', NAME[p_inverter_id],
                    SERIAL_NUMBER[p_inverter_id], TEMPERATURE[p_inverter_id])

    def get_temperature(self, p_inverter_id: int):
        ParsedData = self.get_json('/api/live')
        temp_index = ParsedData["ch0_fld_names"].index("Temp")

        ParsedData = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        TEMPERATURE[p_inverter_id] = str(ParsedData["ch"][0][temp_index]) + ' degC'
        logger.info('Ahoy: Inverter "%s" temperature: %s', NAME[p_inverter_id], TEMPERATURE[p_inverter_id])

    def get_panel_min_voltage(self, p_inverter_id: int):
        ParsedData = self.get_json('/api/live')
        PanelVDC_index = ParsedData["fld_names"].index("U_DC")

        ParsedData = self.get_json(f'/api/inverter/id/{p_inverter_id}')
        PanelVDC = []
        ExcludedPanels = GetNumberArray(HOY_BATTERY_IGNORE_PANELS[p_inverter_id])
        for i in range(1, len(ParsedData['ch']), 1):
            if i not in ExcludedPanels:
                PanelVDC.append(float(ParsedData['ch'][i][PanelVDC_index]))
        minVdc = float('inf')
        for i in range(len(PanelVDC)):
            if (minVdc > PanelVDC[i]) and (PanelVDC[i] > 5):
                minVdc = PanelVDC[i]
        if minVdc == float('inf'):
            minVdc = 0

        # save last 5 min-values in list and return the "highest" value.
        HOY_PANEL_VOLTAGE_LIST[p_inverter_id].append(minVdc)
        if len(HOY_PANEL_VOLTAGE_LIST[p_inverter_id]) > 5:
            HOY_PANEL_VOLTAGE_LIST[p_inverter_id].pop(0)
        max_value = None
        for num in HOY_PANEL_VOLTAGE_LIST[p_inverter_id]:
            if (max_value is None or num > max_value):
                max_value = num

        logger.info('Lowest panel voltage inverter "%s": %s Volt', NAME[p_inverter_id], max_value)
        return max_value

    def wait_for_ack(self, p_inverter_id: int, p_timeout_in_s: int):
        try:
            timeout = p_timeout_in_s
            timeout_start = time.time()
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
        myobj = {'cmd': 'limit_nonpersistent_absolute', 'val': p_limit, "id": p_inverter_id, "token": self.Token}
        response = self.get_response_json('/api/ctrl', myobj)
        if response["success"] == False and response["error"] == "ERR_PROTECTED":
            self.Authenticate()
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
        myobj = {'cmd': 'power', 'val': cast_to_int(p_active == True), "id": p_inverter_id, "token": self.Token}
        response = self.get_response_json('/api/ctrl', myobj)
        if response["success"] == False and response["error"] == "ERR_PROTECTED":
            self.Authenticate()
            self.set_power_status(p_inverter_id, p_active)
            return
        if response["success"] == False:
            raise Exception("Error: SetPowerStatus Request error")

    def Authenticate(self):
        logger.info('Ahoy: Authenticating...')
        myobj = {'auth': self.password}
        response = self.get_response_json('/api/ctrl', myobj)
        if response["success"] == False:
            raise Exception("Error: Authenticate Request error")
        self.Token = response["token"]
        logger.info('Ahoy: Authenticating successful, received Token: %s', self.Token)


class OpenDTU(DTU):
    def __init__(self, inverter_count: int, ip: str, user: str, password: str):
        super().__init__(inverter_count)
        self.ip = ip
        self.user = user
        self.password = password

    def GetJson(self, path):
        url = f'http://{self.ip}{path}'
        return session.get(url, auth=HTTPBasicAuth(self.user, self.password), timeout=10).json()

    def GetResponseJson(self, path, sendStr):
        url = f'http://{self.ip}{path}'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        return session.post(url=url, headers=headers, data=sendStr, auth=HTTPBasicAuth(self.user, self.password),
                            timeout=10).json()

    def get_ac_power(self, p_inverter_id):
        ParsedData = self.GetJson(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        return cast_to_int(ParsedData['inverters'][0]['AC']['0']['Power']['v'])

    def CheckMinVersion(self):
        MinVersion = 'v24.2.12'
        ParsedData = self.GetJson('/api/system/status')
        OpenDTUVersion = str((ParsedData["git_hash"]))
        if ("-Database" in OpenDTUVersion):  # trim string "v24.5.27-Database"
            OpenDTUVersion = OpenDTUVersion.replace("-Database", "")
        logger.info('OpenDTU: Current Version: %s', OpenDTUVersion)
        if version.parse(OpenDTUVersion) < version.parse(MinVersion):
            logger.error(
                'Error: Your OpenDTU Version is too old! Please update at least to Version %s - you can find the newest dev-releases here: https://github.com/tbnobody/OpenDTU/actions',
                MinVersion)
            quit()

    def get_available(self, p_inverter_id: int):
        ParsedData = self.GetJson(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        Reachable = bool(ParsedData['inverters'][0]["reachable"])
        logger.info('OpenDTU: Inverter "%s" reachable: %s', NAME[p_inverter_id], Reachable)
        return Reachable

    def get_actual_limit_in_w(self, p_inverter_id: int):
        ParsedData = self.GetJson('/api/limit/status')
        limit_relative = float(ParsedData[SERIAL_NUMBER[p_inverter_id]]['limit_relative'])
        LimitInW = HOY_INVERTER_WATT[p_inverter_id] * limit_relative / 100
        return LimitInW

    def get_info(self, p_inverter_id: int):
        if SERIAL_NUMBER[p_inverter_id] == '':
            ParsedData = self.GetJson('/api/livedata/status')
            SERIAL_NUMBER[p_inverter_id] = str(ParsedData['inverters'][p_inverter_id]['serial'])

        ParsedData = self.GetJson(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        TEMPERATURE[p_inverter_id] = str(
            round(float((ParsedData['inverters'][0]['INV']['0']['Temperature']['v'])), 1)) + ' degC'
        NAME[p_inverter_id] = str(ParsedData['inverters'][0]['name'])
        logger.info('OpenDTU: Inverter "%s" / serial number "%s" / temperature %s', NAME[p_inverter_id],
                    SERIAL_NUMBER[p_inverter_id], TEMPERATURE[p_inverter_id])

    def get_temperature(self, p_inverter_id: int):
        ParsedData = self.GetJson(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        TEMPERATURE[p_inverter_id] = str(
            round(float((ParsedData['inverters'][0]['INV']['0']['Temperature']['v'])), 1)) + ' degC'
        logger.info('OpenDTU: Inverter "%s" temperature: %s', NAME[p_inverter_id], TEMPERATURE[p_inverter_id])

    def get_panel_min_voltage(self, p_inverter_id: int):
        ParsedData = self.GetJson(f'/api/livedata/status?inv={SERIAL_NUMBER[p_inverter_id]}')
        PanelVDC = []
        ExcludedPanels = GetNumberArray(HOY_BATTERY_IGNORE_PANELS[p_inverter_id])
        for i in range(len(ParsedData['inverters'][0]['DC'])):
            if i not in ExcludedPanels:
                PanelVDC.append(float(ParsedData['inverters'][0]['DC'][str(i)]['Voltage']['v']))
        minVdc = float('inf')
        for i in range(len(PanelVDC)):
            if (minVdc > PanelVDC[i]) and (PanelVDC[i] > 5):
                minVdc = PanelVDC[i]
        if minVdc == float('inf'):
            minVdc = 0

        # save last 5 min-values in list and return the "highest" value.
        HOY_PANEL_VOLTAGE_LIST[p_inverter_id].append(minVdc)
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
            while time.time() < timeout_start + timeout:
                time.sleep(0.5)
                ParsedData = self.GetJson('/api/limit/status')
                ack = (ParsedData[SERIAL_NUMBER[p_inverter_id]]['limit_set_status'] == 'Ok')
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
        relLimit = cast_to_int(p_limit / HOY_INVERTER_WATT[p_inverter_id] * 100)
        mySendStr = f'''data={{"serial":"{SERIAL_NUMBER[p_inverter_id]}", "limit_type":1, "limit_value":{relLimit}}}'''
        response = self.GetResponseJson('/api/limit/config', mySendStr)
        if response['type'] != 'success':
            raise Exception(f"Error: SetLimit error: {response['message']}")
        CURRENT_LIMIT[p_inverter_id] = p_limit

    def set_power_status(self, p_inverter_id: int, p_active: bool):
        if p_active:
            logger.info('OpenDTU: Inverter "%s": Turn on', NAME[p_inverter_id])
        else:
            logger.info('OpenDTU: Inverter "%s": Turn off', NAME[p_inverter_id])

        logger.debug(f'POWER: {p_active}')
        mySendStr = f'''data={{"serial":"{SERIAL_NUMBER[p_inverter_id]}", "power":{str(p_active).lower()}}}'''
        logger.info(mySendStr)
        response = self.GetResponseJson('/api/power/config', mySendStr)
        if response['type'] != 'success':
            raise Exception(f"Error: SetPowerStatus error: {response['message']}")


class DebugDTU(DTU):
    def __init__(self, inverter_count: int):
        super().__init__(inverter_count)

    def get_ac_power(self, p_inverter_id):
        return cast_to_int(input("Current AC-Power: "))

    def CheckMinVersion(self):
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

    def Authenticate(self):
        logger.info('Debug: Authenticating...')
        self.Token = '12345'
        logger.info('Debug: Authenticating successful, received Token: %s', self.Token)
