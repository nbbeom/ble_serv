import boto3
from loguru import logger


class dbm():
    def __init__(self, beaker_num):
        self._dyn_resource = boto3.resource('dynamodb')
        self._table_exp = self._dyn_resource.Table('uxn_lab_rasplist')
        self._table_set = self._dyn_resource.Table('uxn_lab_setting')
        self._beaker_num = beaker_num

    def device_get(self):
        response = self._table_exp.scan()
        rpidev = []
        logger.info(response['Items'])
        for i in range(len(response['Items'])):
            rpidev.append(response['Items'][i]['dev_id'])
        return rpidev

    def put_ble(self, beaker, addrList, experiment, interval, dev_id):
        item = {
            'beaker': self._beaker_num,
            'addrList': addrList,
            'experiment': experiment,
            'interval': interval,
            'dev_id': dev_id,
        }
        logger.debug(item)

        self._table_set.put_item(
            Item=item
        )
