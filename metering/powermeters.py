# metering/powermeters.py

"""
This module contains the metering classes where the implementation is a true
metering and not a control device. Subclasses that control things reside in the 'control' package.
"""


import json
import subprocess
import time
import paho.mqtt.client as mqtt

from requests.auth import HTTPDigestAuth

from HoymilesZeroExport import cast_to_int, extract_json_value, session, logger


class Powermeter:
    def get_powermeter_watts(self) -> int:
        raise NotImplementedError()


class Tasmota(Powermeter):
    def __init__(self, ip: str, user: str, password: str, json_status: str, json_payload_mqtt_prefix: str,
                 json_power_mqtt_label: str, json_power_input_mqtt_label: str, json_power_output_mqtt_label: str,
                 json_power_calculate: bool):
        self.ip = ip
        self.user = user
        self.password = password
        self.json_status = json_status
        self.json_payload_mqtt_prefix = json_payload_mqtt_prefix
        self.json_power_mqtt_label = json_power_mqtt_label
        self.json_power_input_mqtt_label = json_power_input_mqtt_label
        self.json_power_output_mqtt_label = json_power_output_mqtt_label
        self.json_power_calculate = json_power_calculate

    def get_json(self, path):
        url = f'http://{self.ip}{path}'
        return session.get(url, timeout=10).json()

    def get_powermeter_watts(self):
        if not self.user:
            parsed_data = self.get_json('/cm?cmnd=status%2010')
        else:
            parsed_data = self.get_json(f'/cm?user={self.user}&password={self.password}&cmnd=status%2010')
        if not self.json_power_calculate:
            return cast_to_int(parsed_data[self.json_status][self.json_payload_mqtt_prefix][self.json_power_mqtt_label])
        else:
            input = parsed_data[self.json_status][self.json_payload_mqtt_prefix][self.json_power_input_mqtt_label]
            ouput = parsed_data[self.json_status][self.json_payload_mqtt_prefix][self.json_power_output_mqtt_label]
            return cast_to_int(input - ouput)


class Shelly(Powermeter):
    def __init__(self, ip: str, user: str, password: str, emeterindex: str):
        self.ip = ip
        self.user = user
        self.password = password
        self.emeterindex = emeterindex

    def get_json(self, path):
        url = f'http://{self.ip}{path}'
        headers = {"content-type": "application/json"}
        return session.get(url, headers=headers, auth=(self.user, self.password), timeout=10).json()

    def get_rpc_json(self, path):
        url = f'http://{self.ip}/rpc{path}'
        headers = {"content-type": "application/json"}
        return session.get(url, headers=headers, auth=HTTPDigestAuth(self.user, self.password), timeout=10).json()

    def get_powermeter_watts(self) -> int:
        raise NotImplementedError()


class Shelly1PM(Shelly):
    def get_powermeter_watts(self):
        return cast_to_int(self.get_json('/status')['meters'][0]['power'])


class ShellyPlus1PM(Shelly):
    def get_powermeter_watts(self):
        return cast_to_int(self.get_rpc_json('/Switch.GetStatus?id=0')['apower'])


class ShellyEM(Shelly):
    def get_powermeter_watts(self):
        if self.emeterindex:
            return cast_to_int(self.get_json(f'/emeter/{self.emeterindex}')['power'])
        else:
            return sum(cast_to_int(emeter['power']) for emeter in self.get_json('/status')['emeters'])


class Shelly3EM(Shelly):
    def get_powermeter_watts(self):
        return cast_to_int(self.get_json('/status')['total_power'])


class Shelly3EMPro(Shelly):
    def get_powermeter_watts(self):
        return cast_to_int(self.get_rpc_json('/EM.GetStatus?id=0')['total_act_power'])


class ESPHome(Powermeter):
    def __init__(self, ip: str, port: str, domain: str, id: str):
        self.ip = ip
        self.port = port
        self.domain = domain
        self.id = id

    def GetJson(self, path):
        url = f'http://{self.ip}:{self.port}{path}'
        return session.get(url, timeout=10).json()

    def get_powermeter_watts(self):
        ParsedData = self.GetJson(f'/{self.domain}/{self.id}')
        return cast_to_int(ParsedData['value'])


class Shrdzm(Powermeter):
    def __init__(self, ip: str, user: str, password: str):
        self.ip = ip
        self.user = user
        self.password = password

    def GetJson(self, path):
        url = f'http://{self.ip}{path}'
        return session.get(url, timeout=10).json()

    def get_powermeter_watts(self):
        ParsedData = self.GetJson(f'/getLastData?user={self.user}&password={self.password}')
        return cast_to_int(cast_to_int(ParsedData['1.7.0']) - cast_to_int(ParsedData['2.7.0']))


class Emlog(Powermeter):
    def __init__(self, ip: str, meterindex: str, json_power_calculate: bool):
        self.ip = ip
        self.meterindex = meterindex
        self.json_power_calculate = json_power_calculate

    def GetJson(self, path):
        url = f'http://{self.ip}{path}'
        return session.get(url, timeout=10).json()

    def get_powermeter_watts(self):
        ParsedData = self.GetJson(f'/pages/getinformation.php?heute&meterindex={self.meterindex}')
        if not self.json_power_calculate:
            return cast_to_int(ParsedData['Leistung170'])
        else:
            input = ParsedData['Leistung170']
            ouput = ParsedData['Leistung270']
            return cast_to_int(input - ouput)


class IoBroker(Powermeter):
    def __init__(self, ip: str, port: str, current_power_alias: str, power_calculate: bool, power_input_alias: str,
                 power_output_alias: str):
        self.ip = ip
        self.port = port
        self.current_power_alias = current_power_alias
        self.power_calculate = power_calculate
        self.power_input_alias = power_input_alias
        self.power_output_alias = power_output_alias

    def get_json(self, path):
        url = f'http://{self.ip}:{self.port}{path}'
        return session.get(url, timeout=10).json()

    def get_powermeter_watts(self):
        input_power = -1
        output_power = -1
        if not self.power_calculate:
            parsed_data = self.get_json(f'/getBulk/{self.current_power_alias}')
            for item in parsed_data:
                if item['id'] == self.current_power_alias:
                    return cast_to_int(item['val'])
        else:
            parsed_data = self.get_json(f'/getBulk/{self.power_input_alias},{self.power_output_alias}')
            for item in parsed_data:
                if item['id'] == self.power_input_alias:
                    input_power = cast_to_int(item['val'])
                if item['id'] == self.power_output_alias:
                    output_power = cast_to_int(item['val'])

            return cast_to_int(input_power - output_power)


class HomeAssistant(Powermeter):
    def __init__(self, ip: str, port: str, use_https: bool, access_token: str, current_power_entity: str,
                 power_calculate: bool, power_input_alias: str, power_output_alias: str):
        self.ip = ip
        self.port = port
        self.use_https = use_https
        self.access_token = access_token
        self.current_power_entity = current_power_entity
        self.power_calculate = power_calculate
        self.power_input_alias = power_input_alias
        self.power_output_alias = power_output_alias

    def get_json(self, path):
        if self.use_https:
            url = f"https://{self.ip}:{self.port}{path}"
        else:
            url = f"http://{self.ip}:{self.port}{path}"
        headers = {"Authorization": "Bearer " + self.access_token, "content-type": "application/json"}
        return session.get(url, headers=headers, timeout=10).json()

    def get_powermeter_watts(self):
        if not self.power_calculate:
            parsed_data = self.get_json(f"/api/states/{self.current_power_entity}")
            return cast_to_int(parsed_data['state'])
        else:
            parsed_data = self.get_json(f"/api/states/{self.power_input_alias}")
            input_power = cast_to_int(parsed_data['state'])
            parsed_data = self.get_json(f"/api/states/{self.power_output_alias}")
            output_power = cast_to_int(parsed_data['state'])
            return cast_to_int(input_power - output_power)


class VZLogger(Powermeter):
    def __init__(self, ip: str, port: str, uuid: str):
        self.ip = ip
        self.port = port
        self.uuid = uuid

    def get_json(self):
        url = f"http://{self.ip}:{self.port}/{self.uuid}"
        return session.get(url, timeout=10).json()

    def get_powermeter_watts(self):
        return cast_to_int(self.get_json()['data'][0]['tuples'][0][1])


class AmisReader(Powermeter):
    def __init__(self, ip: str):
        self.ip = ip

    def get_json(self, path):
        url = f'http://{self.ip}{path}'
        return session.get(url, timeout=10).json()

    def get_powermeter_watts(self):
        parsed_data = self.get_json('/rest')
        return cast_to_int(parsed_data['saldo'])


class DebugReader(Powermeter):
    def get_powermeter_watts(self):
        return cast_to_int(input("Enter Powermeter Watts: "))

class Script(Powermeter):
    def __init__(self, file: str, ip: str, user: str, password: str):
        self.file = file
        self.ip = ip
        self.user = user
        self.password = password

    def get_powermeter_watts(self):
        power = subprocess.check_output([self.file, self.ip, self.user, self.password])
        return cast_to_int(power)

class MqttPowermeter(Powermeter):
    def __init__(
        self,
        broker: str,
        port: int,
        topic_incoming: str,
        json_path_incoming: str = None,
        topic_outgoing: str = None,
        json_path_outgoing: str = None,
        username: str = None,
        password: str = None,
    ):
        self.broker = broker
        self.port = port
        self.topic_incoming = topic_incoming
        self.json_path_incoming = json_path_incoming
        self.topic_outgoing = topic_outgoing
        self.json_path_outgoing = json_path_outgoing
        self.username = username
        self.password = password
        self.value_incoming = None
        self.value_outgoing = None

        # Initialize MQTT client
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        if self.username and self.password:
            self.client.username_pw_set(self.username, self.password)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        # Connect to the broker
        self.client.connect(self.broker, self.port)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"Connected with result code {reason_code}")
        # Subscribe to the topics
        client.subscribe(self.topic_incoming)
        logger.info(f"Subscribed to topic {self.topic_incoming}")
        if self.topic_outgoing and self.topic_outgoing != self.topic_incoming:
            client.subscribe(self.topic_outgoing)
            logger.info(f"Subscribed to topic {self.topic_outgoing}")

    def on_message(self, client, userdata, msg):
        payload = msg.payload.decode()
        try:
            data = json.loads(payload)
            if msg.topic == self.topic_incoming:
                self.value_incoming = extract_json_value(data, self.json_path_incoming) if self.json_path_incoming else int(float(payload))
                logger.info('MQTT: Incoming power: %s Watt', self.value_incoming)
            elif msg.topic == self.topic_outgoing:
                self.value_outgoing = extract_json_value(data, self.json_path_outgoing) if self.json_path_outgoing else int(float(payload))
                logger.info('MQTT: Outgoing power: %s Watt', self.value_outgoing)
        except json.JSONDecodeError:
            print("Failed to decode JSON")

    def get_powermeter_watts(self):
        if self.value_incoming is None:
            self.wait_for_message("incoming")
        if self.topic_outgoing and self.value_outgoing is None:
            self.wait_for_message("outgoing")

        return self.value_incoming - (self.value_outgoing if self.value_outgoing is not None else 0)

    def wait_for_message(self, message_type, timeout=5):
        start_time = time.time()
        while (message_type == "incoming" and self.value_incoming is None) or (message_type == "outgoing" and self.value_outgoing is None):
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for MQTT {message_type} message")
            time.sleep(1)
