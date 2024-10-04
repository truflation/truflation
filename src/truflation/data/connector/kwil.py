"""
Kwil connector
"""
import re
import os
import uuid
import json
import time
import hashlib
import struct
import subprocess
import tempfile
import pandas as pd
from icecream import ic
import asyncio
import aiohttp
import logging

import truflation.data
from .factory import add_connector_factory
from .base import Connector

from eth_utils import to_checksum_address

from dotenv import load_dotenv
load_dotenv()
ic(os.environ)
EXECUTABLE_NAME = 'kwil-cli'



def hash_to_int32(address: str) -> int:
    checksum_address = address
    address_hash = hashlib.sha256(checksum_address.encode('utf-8')).digest()
    int32_hash = struct.unpack('>L', address_hash[:4])[0]
    return int32_hash

class CommandExecutor:
    def __init__(self, executable_name):
        self.executable_name = executable_name

    def execute_command(self, *args):
        executable_path = self.executable_path
        if not executable_path:
            raise ValueError(
                f"Executable '{self.executable_name}' not found in the system path.")
        command = [executable_path, *args]
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise ValueError (f"Command execution failed: {e}")

    def execute_command_json(self, *args):
        args_ext = args + ('--output', 'json',)
        return json.loads(self.execute_command(*args_ext))
    
class BlockchainInteraction:
    def __init__(self, executor):
        self.executor = executor

    #Refactor the query_tx_wait method using asyncio and aiohttp:
    async def query_tx_wait_async(self, txid):
        while True:
            retval = await self.query_tx_async(txid)
            if retval['result'].get('height') != -1:
                return retval
            await asyncio.sleep(0.2)

    async def query_tx_async(self, txid):
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(f'https://api.example.com/tx/{txid}') as response:
                        result = await response.json()
                        return result
                except aiohttp.ClientError as e:
                    logging.error(f'An error occured while querying transaction: {str(e)}')
                    pass
                await asyncio.sleep(0.2)
class ConnectorKwil(Connector):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.executor = CommandExecutor('kwil-cli')
        self.blockchain = BlockchainInteraction(self.executor)
        self.kwil_user = os.environ['KWIL_USER']
        self.executable_name = 'kwil-cli'
        self.executable_path = self._get_executable_path()
        self.round = 6
        ic(self.version())
        if self.version()['Version'] != '0.6.3':
            raise ValueError('invalid version')

    def _get_executable_path(self):
        return next(
            (
                os.path.join(path, self.executable_name)
                for path in os.environ["PATH"].split(os.pathsep)
                if os.path.isfile(os.path.join(path, self.executable_name))
            ),
            None,
        )

    def execute_command(self, *args):
        executable_path = self.executable_path
        if not executable_path:
            raise ValueError(
                f"Executable '{self.executable_name}' not found in the system path.")
        command = [executable_path, *args]
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, check=True
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            raise ValueError (f"Command execution failed: {e}")

    def execute_command_json(self, *args):
        args_ext = args + ('--output', 'json',)
        return json.loads(self.execute_command(*args_ext))

    def ping(self):
        return self.execute_command('utils', 'ping')

    def version(self):
        version_string = self.execute_command('version')
        pattern = r"([\w\s/]+):\s+([^\r\n]+)"
        matches = re.findall(pattern, version_string)
        return {key.strip(): value.strip() for key, value in matches}

    def query(self, dbid, query):
        return self.execute_command_json(*([
            'database',
            'query',
            query
        ] + self._get_db_arg(dbid)))

    def read_all(self, *args, **kwargs) -> pd.DataFrame | None:
        if len(args) == 0:
            raise Exception("need to specify source")
        if ':' not in args[0]:
            raise Exception('need db and table')
        (dbid, table) = args[0].split(':')
        result = self.query(
            dbid,
            f'select * from {table}'
        )
        ic(result)
        if result.get('result', '') == '':
            return None
        return self.fix_data_read(pd.DataFrame(result['result']))

    def write_all(self, data, *args, **kwargs) -> None:
        ic(args)
        ic(kwargs)
        filename = kwargs.get('key', None)
        if filename is None and len(args) > 0:
            filename = args[0]
        if filename is None:
            raise Exception("need to specify source")
        if ':' not in filename:
            raise Exception('need db and table')
        (dbid, table) = filename.split(':')
        ic(dbid, table, data)
        data = self.fix_data_write(data)
        with tempfile.NamedTemporaryFile(
                delete=True, suffix='.csv'
        ) as temp_file:
            temp_file_path = temp_file.name
            data.to_csv(
                temp_file,
                index=False
            )
            if not ic(self.has_schema(dbid)):
                r1 = ic(self.deploy(dbid))
                ic(self.query_tx_wait(r1['result']['tx_hash']))
                self.add_admin(dbid, self.kwil_user)
                if not ic(self.has_schema(dbid)):
                    raise ValueError('schema not created')
            result = self.execute_command_json(*([
                'database',
                'batch',
                '-p',
                temp_file.name,
                '-a',
                'add_record',
                '--map-inputs',
                'id:id,date_value:date_value,value:value,created_at:created_at',
            ] + self._get_db_arg(dbid)))
            ic(result)
            return ic(self.query_tx_wait(
                result['result']['tx_hash']
            ))
        raise ValueError

    def fix_data_read(self, df):
        ic(df)
        if df.empty:
            return df
        df.rename(columns={'date_value': 'date'}, inplace=True)
        df['created_at'] = pd.to_datetime(df['created_at'].astype(int))
        df.drop(columns=['id'], inplace=True)
        df['value'] = round(
            df['value'].astype(int) / 10 ** self.round,
            self.round
        )
        return df

    def fix_data_write(self, df):
        ic(df)
        if df.index.name == 'date':
            df = df.reset_index()
        df.rename(columns={'date': 'date_value'}, inplace=True)
        df['id'] = df.apply(lambda row: uuid.uuid4(), axis=1)
        df['value'] = round(df['value'] * 10**self.round, self.round).astype(int)
        df['created_at'] = df['created_at'].astype(int)
        ic(df)
        ic(df.dtypes)
        return df

    def deploy(self, db_name, data_filename=None):
        package_name = truflation.data
        if data_filename is None:
            data_filename = 'schemas/kwil/schema.development.kf'
        package_directory = os.path.dirname(package_name.__file__)
        data_filepath = os.path.join(package_directory, data_filename)
        ic(data_filepath)
        return self.execute_command_json(
            'database', 'deploy', '--path', data_filepath,
            '--name', db_name
        )

    def add_admin(self, dbid, user):
        user = to_checksum_address(user)
        id_hash = hash_to_int32(user)
        return self.execute_command_json(*([
            'database',
            'execute',
            f'$id:{id_hash}',
            f'$address:{user}',
            '-a',
            'add_admin_owner'
        ] + self._get_db_arg(dbid)))

    def query_tx(self, txid):
        return self.execute_command_json(
            'utils', 'query-tx', txid
        )

    def query_tx_wait(self, txid):
        while True:
            retval = self.query_tx(txid)
            if retval['result'].get('height') != -1:
                return retval
            time.sleep(0.2)

    def list_databases(self):
        return self.execute_command_json(
            'database', 'list'
        )

    def _get_db_arg(self, dbid:str) -> list:
        args = []
        owner = None
        if '@' in dbid:
            (owner, dbid) = dbid.split('@')
            args += ['-o', owner]
        if dbid[0] == 'x':
            args += ['-i', dbid]
        else:
            args += ['-n', dbid]
        return args

    def read_schema(self, dbid:str):
        return self.execute_command_json(*([
            'database', 'read-schema'
        ] + self._get_db_arg(dbid)))

    def has_schema(self, dbid:str):
        result = self.read_schema(dbid)
        ic(result)
        return result['result'] != ''

    @staticmethod
    def get_hash(string):
        hash_object = hashlib.sha256()
        hash_object.update(string.encode("utf-8"))
        hash_digest = hash_object.hexdigest()
        return 'kw' + hash_digest[:30]

def connector_factory_function(connector_type: str) -> Connector | None:
    if connector_type.startswith('kwil:'):
        return ConnectorKwil()
    return None


add_connector_factory(connector_factory_function)
