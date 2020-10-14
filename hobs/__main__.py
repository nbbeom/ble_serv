import json
import os
import queue
import time

from loguru import logger
import paho.mqtt.client as mqtt
import boto3
import threading

from fastapi import FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware
from starlette.status import (
    HTTP_202_ACCEPTED,
    HTTP_404_NOT_FOUND, HTTP_409_CONFLICT)


from hobs.__init__ import __version__
from hobs.csv import csvManager
from hobs.sub import Subscriber


logger.info({'version': __version__})

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

MQTT_KEEP_ALIVE = 5
mqtt_port = os.getenv('HOBS_MQTT_PORT')
mqtt_host = os.getenv('HOBS_MQTT_HOST')
ca_certs = os.getenv('HOBS_CA_CERTS')
certifile = os.getenv('HOBS_CERTFILE')
keyfile = os.getenv('HOBS_KEYFILE')

sub = threading.Thread(target=Subscriber)
sub.start()


def device_get():
    dyn_resource = boto3.resource('dynamodb')
    table_exp = dyn_resource.Table('lab_rpi')
    response = table_exp.scan()
    rpidev = []
    # logger.info(response['Items'])
    for i in range(len(response['Items'])):
        rpidev.append({
            'name': response['Items'][i]['name'],
            'rpi_id': response['Items'][i]['rpi_id'],
            })
    return rpidev


responses_post = {
    HTTP_404_NOT_FOUND: {
        "description": "Device not found"
    },
    # HTTP_409_CONFLICT: {
    #     "description": "Already in use",
    #     "content": {
    #         "application/json": {
    #         }
    #     }
    # },
}

responses_get = {
    HTTP_404_NOT_FOUND: {
        "description": "Device not found"
    },
    HTTP_409_CONFLICT: {
        "description": "The status of container was abnormal",
        "content": {
            "application/json": {
            }
        }
    },
}

responses_put = responses_get

responses_delete = {
    HTTP_404_NOT_FOUND: {
        "description": "Device not found"
    },
}


class MqttRequester(mqtt.Client):
    def __init__(
        self,
        url=mqtt_host,
        port=int(mqtt_port),
        ca_certs=ca_certs,
        certfile=certifile,
        keyfile=keyfile,
    ):
        mqtt.Client.__init__(self)
        self.tls_set(
            ca_certs=ca_certs,
            certfile=certfile,
            keyfile=keyfile
        )
        self.tls_insecure_set(True)
        self._queue = queue.Queue(1)
        self.connect(url, port, MQTT_KEEP_ALIVE)
        self.loop_start()

    # def on_connect(self, mqttc, obj, flags, rc):
    #     logger.info({
    #         'event': 'on_connect',
    #         'rc': str(rc),
    #     })

    # def on_disconnect(self, client, userdata, rc):
    #     logger.info({
    #         'event': 'on_disconnect',
    #         'userdata': userdata,
    #         'rc': str(rc),
    #     })

    def on_message(self, mqttc, obj, msg):
        # logger.info({
        #     'event': 'on_message',
        #     'topic': msg.topic,
        #     'qos': str(msg.qos),
        #     'payload': msg.payload.decode(),
        # })
        self._queue.put(msg.payload)

    # def on_publish(self, mqttc, obj, mid):
    #     logger.debug({
    #         'event': 'on_publish',
    #         'mid': str(mid),
    #     })

    # def on_subscribe(self, mqttc, obj, mid, granted_qos):
    #     logger.debug({
    #         'event': 'on_subscribe',
    #         'mid': str(mid),
    #         'qos': str(granted_qos),
    #     })

    def on_log(self, mqttc, obj, level, string):
        if level is mqtt.MQTT_LOG_ERR:
            logger.error({
                'event': 'on_log',
                'level': level,
                'string': string,
            })

    def run(self, pub, dev_id):
        msg_id = str(time.time())

        self.subscribe('/dev_id/' + dev_id + '/srw/#')

        try:
            if pub == 'scan':
                self.publish('/dev_id/' + dev_id + '/wrs/', 'scan')
                resmsg = self._queue.get(block=True, timeout=15.0).decode()

            elif pub == 'get_status':
                self.publish('/dev_id/' + dev_id + '/wrs/', 'get_status')
                resmsg = self._queue.get(block=True, timeout=0.9).decode()

            elif pub == 'update':
                self.publish('/dev_id/' + dev_id + '/wrs/', 'update')
                resmsg = self._queue.get(block=True, timeout=10.0).decode()

            else:
                resmsg = "invalid pub"

            # logger.debug({
            #     'msg_id': msg_id,
            #     'resmsg': resmsg,
            # })

            return json.loads(resmsg)

        except queue.Empty as e:
            logger.warning({
                'title': 'mqtt',
                'dev-id': dev_id,
                'action': 'timeout',
                'except': e,
            })
            raise HTTPException(
                    status_code=HTTP_404_NOT_FOUND,
                    detail="Item not found"
                    )

        except Exception as e:
            logger.exception(e)
            raise e

        finally:
            self.disconnect()


@app.get(
    '/ble',
    status_code=HTTP_202_ACCEPTED,
    responses={**responses_get},
    tags=['device'],)
def get_ble(dev_id: str):
    try:
        logger.info({
            'dev-id': dev_id,
        })

        return MqttRequester().run('scan', dev_id)

    except Exception as e:
        logger.exception(e)
        raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="Item not found")


@app.get(
    '/status',
    tags=['device'])
def get_status(dev_id: str):
    try:
        # logger.info({
        #     'method': 'GET',
        #     'dev-id': dev_id,
        # })

        return MqttRequester().run('get_status', dev_id)

    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=404, detail="Item not found")


@app.put(
    '/',
    tags=['device'],)
def update(dev_id: str):
    try:
        logger.info({
            'method': 'GET',
            'dev-id': dev_id,
        })

        return MqttRequester().run('update', dev_id)

    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=404, detail="Item not found")


@app.get(
    '/db/device',
    tags=['db'],
    )
def get_device():
    rpi = device_get()

    rpid = {
        'devices': rpi
    }
    return rpid


# XXX status code 를 넣고 , CODE 넘버 바꾸고, Details 에다가 detail 만들것,
@app.post(
    '/db/csv',
    tags=['db'],
)
def get_csv(experiment_id: str):
    csvm = csvManager(experiment_id).execute()
    if csvm != 0:
        return {
            'code': 1,
            'detail': 'success',
            }
    else:
        return {
            'code': 0,
            'detail': 'Invalied exp_name',
        }