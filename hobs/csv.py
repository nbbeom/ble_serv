from datetime import timezone, datetime
from pathlib import Path
import subprocess
import time

from loguru import logger
import boto3

S3_BUCKET_NAME = 'uxn-lab'
STORAGE_PATH = '/opt/storage'


class csvManager():

    def __init__(self, experiment_id):
        self._experiment_id = experiment_id
        self._experiment_name = self._get_experiment_name(experiment_id)
        self._filename = f'{self._get_experiment_name(experiment_id)}_{self._experiment_id}.csv'
        self._filepath = Path(f'{STORAGE_PATH}/{self._filename}')
        self._dyn_resource = boto3.resource('dynamodb')
        self._table_files = self._dyn_resource.Table('lab_files')

        logger.debug({
            'filename': self._filename,
            'filepath': self._filepath,
            'filepath.str': str(self._filepath),
            'filepath.name': self._filepath.name,
        })

    def _get_experiment_name(self, experiment_id):
        dyn_resource = boto3.resource('dynamodb')
        table_exp = dyn_resource.Table('lab_experiment')
        response = table_exp.scan(
                FilterExpression=boto3.
                dynamodb.conditions.
                Attr('experiment_id').
                eq(experiment_id)
            )
        print(response)
        return response['Items'][0]['name']

    def _export_csv(self, shell=True):
        try:
            # XXX 우아하게~~~~
            cmd1 = 'aws dynamodb query --table-name table_name'
            cmd2 = ' --key-condition-expression "experiment_id = :v1"'
            cmd3 = ' --expression-attribute-values \'{\":v1\": {\"S\": \"'
            cmd4 = '\"}}\' --output json | jq -r \'.Items\' | jq -r \'(.[0] |'
            cmd5 = ' keys_unsorted) as $keys | $keys, map([.[ $keys[] ].S])[]'
            cmd6 = ' | @csv\' > '

            cmd = cmd1 + cmd2 + cmd3 + self._experiment_id
            cmd = cmd + cmd4 + cmd5 + cmd6 + str(self._filepath)

            logger.debug({
                'cmd': cmd,
            })

            output = subprocess.check_output(
                cmd,
                shell=shell
            ).decode().rstrip()
            return output

        except Exception as e:
            # subprocess.CalledProcessError
            return None

    def put_props(self, item):
        key = item['file_name']
        try:
            self._table_files.update_item(
                key={'file_name': key},
                ExpressionAttributeValues={
                    'latest_modified_date': item['latest_modified_date'],
                    'experiment_name': self._experiment_name,
                                        }
            )
        except Exception:
            self._table_files.put_item(
                Item=item
            )

    def _upload_s3(self):
        s3 = boto3.client('s3')

        s3.upload_file(
            str(self._filepath),
            S3_BUCKET_NAME,
            self._filepath.name,
        )

        s3 = boto3.resource('s3')

        bucket = s3.Bucket(S3_BUCKET_NAME)
        for obj in bucket.objects.filter(Prefix=self._filename):
            latest_modified_date = datetime.strftime(
                    obj.last_modified,
                    '%Y-%m-%d %H:%M:%S',
                )
            latest_modified_date = datetime.strptime(
                latest_modified_date,
                '%Y-%m-%d %H:%M:%S'
                )
            latest_modified_date = datetime.strptime(
                aslocaltimestr(latest_modified_date),
                '%Y-%m-%d %H:%M:%S'
                )
            latest_modified_date = latest_modified_date.timetuple()
            latest_modified_date = time.mktime(latest_modified_date) * 1000

        item = {
            'experiment_name': self._experiment_name,
            'latest_modified_date': int(latest_modified_date),
            'file_name': self._filename,
        }

        self.put_props(item)

    def execute(self):
        if self._export_csv() == None:
            return 0
        else:
            self._upload_s3()
            return 1


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=None)


def aslocaltimestr(utc_dt):
    return utc_to_local(utc_dt).strftime('%Y-%m-%d %H:%M:%S')
