from datetime import datetime, timezone
from logging import Logger
import os
import traceback
import time
from typing import List
import pandas as pd
from collections import deque
from dotenv import load_dotenv

from .base import Connector
from .factory import add_connector_factory

from trufnetwork_sdk_py.client import TNClient, STREAM_TYPE_PRIMITIVE, StreamDefinitionInput, RecordBatch, StreamLocatorInput
from trufnetwork_sdk_py.utils import generate_stream_id


load_dotenv()

TN_PRIVATE_KEY = os.environ.get('TN_PRIVATE_KEY')
TN_ENDPOINT = os.environ.get('TN_ENDPOINT')

MAX_RETRIES = 3
RETRY_DELAY = 30  # seconds
QUERY_DELAY = 10 # seconds

PROVIDERS_MAP = {
    'com_truflation': '0x4710a8d8f0d845da110086812a32de6d90d7ff5c',
    'com_coingecko': '0x7f573e177ee7ec50eb5dee59478285054e4e74e7',
    'com_fmp': '0xf3c816dc0576ec011e5d28367d7fa8c17bb8c6b7',
    'com_zeroxtech': '0x6d86aa58292112d6da5c78eca1cb4989a853a6fa'
}

def parse_stream_id(stream_id, providers_map):
    if stream_id.endswith('_yoy'):
        method = 'getIndexChange'
        stream_id = stream_id[:-4]
    elif stream_id.endswith('_divergence'):
        method = 'get_divergence_index_change'
        stream_id = stream_id[:-11]
    else:
        method = 'getRecords'


    for prefix, provider in providers_map.items():
        prefix_with_sep = prefix + '_'
        if stream_id.startswith(prefix_with_sep):
            formatted_stream_id = stream_id[len(prefix_with_sep):]
            return formatted_stream_id, method, provider
    
    # If no known prefix is found
    raise ValueError(f"Unknown streamId prefix for '{stream_id}'")

# Convert date strings to Unix timestamps
def date_to_unix(date_str, format="%Y-%m-%d"):
    if not date_str:
        return None

    dt = datetime.strptime(date_str, format).replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _handle_failure(logger: Logger, context: str, stream_id, columns: list[str]):
    logger.log_error(f"Stream: {stream_id} with [{context}] Failed:\n{traceback.format_exc()}")
    return pd.DataFrame(columns=columns)

class ConnectorBatch(RecordBatch):
    data_provider: str

class TNConnector(Connector):
    def __init__(self, private_key: str = TN_PRIVATE_KEY, endpoint: str = TN_ENDPOINT, **kwargs):
        super().__init__()
        providers_map = PROVIDERS_MAP | kwargs.get('providers_map', {})

        self.client = TNClient(endpoint, private_key)
        self.providers = providers_map
        self._batch_buffer: dict[str, List[ConnectorBatch]] = {}
        self.batch_size = 10000

    def read_all(self, *args, **kwargs):
        if 'kwargs' in kwargs and isinstance(kwargs['kwargs'], dict):
            kwargs = kwargs['kwargs']

        raw_stream_id, method, data_provider = parse_stream_id(args[0], self.providers)
        stream_id = generate_stream_id(raw_stream_id)

        date_from = date_to_unix(kwargs.get('date_from','2010-01-01'))
        if 'date_to' not in kwargs:
            # If date_to is not provided, use the current date
            # This is to ensure that we always have a valid date range
            # and avoid issues with missing data.
           date_to = date_to_unix(datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
        else:
            date_to = date_to_unix(kwargs.get('date_to'))
    
        base_time = date_to_unix(kwargs.get('base_time'))
        frozen_at = date_to_unix(kwargs.get('frozen_at'))

        if method == 'getRecords':
            return self.readRecords(stream_id, data_provider, date_from, date_to)
        elif method == 'getIndexChange':
            return self.readIndexChange(stream_id, data_provider, date_from, date_to, base_time, frozen_at)
        elif method == 'get_divergence_index_change':
            return self.readCustomIndexChange('get_divergence_index_change', date_from, date_to, base_time, frozen_at)
        else:
            raise ValueError(f"Unknown method: {method}. Supported methods are 'getRecords' and 'getIndexChange'.")


    def readRecords(self, stream_id, data_provider, date_from=None, date_to=None):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                batches = self.gen_batch(date_from, date_to)

                all_dfs = []
                for batch_from, batch_to in batches:
                    records = self.client.get_records(
                        stream_id=stream_id,
                        data_provider=data_provider,
                        date_from=batch_from,
                        date_to=batch_to
                    )

                    df = pd.DataFrame(records, columns=['EventTime', 'Value'])
                    if not df.empty:
                        df['EventTime'] = df['EventTime'].apply(
                            lambda ts: datetime.fromtimestamp(int(ts), tz=timezone.utc)
                                            .replace(minute=0, second=0, tzinfo=None)
                        )

                    df = df.rename(columns={
                        'EventTime': 'date',
                        'Value': 'value'
                    })
                    df['created_at'] = datetime.now(timezone.utc).replace(tzinfo=None)
                    df['value'] = df['value'].astype(float)

                    all_dfs.append(df)

                if all_dfs:
                    return pd.concat(all_dfs, ignore_index=True)
                else:
                    # empty result
                    return pd.DataFrame(columns=['date', 'value', 'created_at'])
            except RuntimeError as e:
                msg = str(e)
                if "Stream not found" in msg:
                    # instead of error, just log debug and return empty
                    self.logging_manager.log_debug(
                        f"Stream not found {stream_id} (skipping read): {msg}"
                    )
                    return pd.DataFrame(columns=['date', 'value', 'created_at'])
                
                if "RPC timeout" in msg or "code = -32001" in msg:
                    if attempt < MAX_RETRIES:
                        self.logging_manager.log_warning(
                            f"[Attempt {attempt} on {stream_id}] RPC timeout, retrying in {QUERY_DELAY*attempt}s..."
                        )
                        time.sleep(QUERY_DELAY * attempt)
                        continue
                    else:
                        return _handle_failure(self.logging_manager, "readIndexChange", stream_id, ['date', 'value', 'created_at'])
                raise
            except Exception as e:
                if attempt < MAX_RETRIES:
                    self.logging_manager.log_exception(
                        f"[Attempt {attempt} on {stream_id}] Error reading records: {e}"
                    )
                    time.sleep(QUERY_DELAY * attempt)
                else:
                    return _handle_failure(self.logging_manager, "readRecords", stream_id, ['date', 'value', 'created_at'])      
    
    def readIndexChange(self, stream_id, data_provider, date_from=None, date_to=None, base_date = None, frozen_at=None):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                batches = self.gen_batch(date_from, date_to)

                all_dfs = []
                for batch_from, batch_to in batches:
                    records = self.client.get_index(
                        stream_id=stream_id,
                        data_provider=data_provider,
                        date_from=batch_from,
                        date_to=batch_to,
                        frozen_at= frozen_at,
                        base_date= base_date
                    )

                    # df = pd.DataFrame(records['values'], columns=records['column_names'])
                    df = pd.DataFrame(records, columns=['EventTime', 'Value'])
                    if not df.empty:
                        df['EventTime'] = df['EventTime'].apply(
                            lambda ts: datetime.fromtimestamp(int(ts), tz=timezone.utc)
                                            .replace(minute=0, second=0, tzinfo=None)
                        )

                    df = df.rename(columns={
                        'EventTime': 'date',
                        'Value': 'value'
                    })
                    df['created_at'] = datetime.now(timezone.utc).replace(tzinfo=None)
                    df['value'] = df['value'].astype(float)

                    all_dfs.append(df)

                if all_dfs:
                    return pd.concat(all_dfs, ignore_index=True)
                else:
                    # empty result
                    return pd.DataFrame(columns=['date', 'value', 'created_at'])
            except RuntimeError as e:
                msg = str(e)
                if "Stream not found" in msg:
                    # instead of error, just log debug and return empty
                    self.logging_manager.log_debug(
                        f"Stream not found {stream_id} (skipping read): {msg}"
                    )
                    return pd.DataFrame(columns=['date', 'value', 'created_at'])
                
                if "RPC timeout" in msg or "code = -32001" in msg:
                    if attempt < MAX_RETRIES:
                        self.logging_manager.log_warning(
                            f"[Attempt {attempt} on {stream_id}] RPC timeout, retrying in {QUERY_DELAY*attempt}s..."
                        )
                        time.sleep(QUERY_DELAY * attempt)
                        continue
                    else:
                        return _handle_failure(self.logging_manager, "readIndexChange", stream_id, ['date', 'value', 'created_at'])
                raise
            except Exception as e:
                if attempt < MAX_RETRIES:
                    self.logging_manager.log_exception(
                        f"[Attempt {attempt} on {stream_id}] Error reading index change: {e}"
                    )
                    time.sleep(QUERY_DELAY * attempt)
                else:
                    _handle_failure(self.logging_manager, "readIndexChange", stream_id, ['date', 'value', 'created_at'])
    
    def readCustomIndexChange(self, procedure: str, date_from=None, date_to=None, base_date = None, frozen_at=None):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                year_in_seconds = 31_536_000
                batches = self.gen_batch(date_from, date_to, year_in_seconds)

                all_dfs = []
                for batch_from, batch_to in batches:
                    records = self.client.call_procedure(procedure, [
                        batch_from,
                        batch_to,
                        base_date,
                        frozen_at,
                        year_in_seconds
                    ])

                    df = pd.DataFrame(records['values'], columns=records['column_names'])
                    if not df.empty:
                        df['event_time'] = df['event_time'].apply(
                            lambda ts: datetime.fromtimestamp(int(ts), tz=timezone.utc)
                                            .replace(minute=0, second=0, tzinfo=None)
                        )

                    df = df.rename(columns={
                        'event_time': 'date',
                        'value': 'value'
                    })
                    df['created_at'] = datetime.now(timezone.utc).replace(tzinfo=None)
                    df['value'] = df['value'].astype(float)

                    all_dfs.append(df)

                if all_dfs:
                    return pd.concat(all_dfs, ignore_index=True)
                else:
                    # empty result
                    return pd.DataFrame(columns=['date', 'value', 'created_at'])
            except RuntimeError as e:
                msg = str(e)
                if "Stream not found" in msg:
                    # instead of error, just log debug and return empty
                    self.logging_manager.log_debug(
                        f"Stream not found {procedure} (skipping read): {msg}"
                    )
                    return pd.DataFrame(columns=['date', 'value', 'created_at'])
                if "RPC timeout" in msg or "code = -32001" in msg:
                    if attempt < MAX_RETRIES:
                        self.logging_manager.log_warning(
                            f"[Attempt {attempt} on {procedure}] RPC timeout, retrying in {QUERY_DELAY*attempt}s..."
                        )
                        time.sleep(QUERY_DELAY * attempt)
                        continue
                    else:
                        return _handle_failure(self.logging_manager, "readIndexChange", procedure, ['date', 'value', 'created_at'])
                raise
            except Exception as e:
                if attempt < MAX_RETRIES:
                    self.logging_manager.log_exception(
                        f"[Attempt {attempt} on {procedure}] Error reading custom index change: {e}"
                    )
                    time.sleep(QUERY_DELAY * attempt)
                else:
                    _handle_failure(self.logging_manager, "readCustomIndexChange",procedure, ['date', 'value', 'created_at'])
        
    def write_all(
            self,
            data,
            *args,
            **kwargs
    ) -> None:
        self.logging_manager.log_info('Saving data to TN database...')
        if 'kwargs' in kwargs and isinstance(kwargs['kwargs'], dict):
            nested_kwargs = kwargs.pop('kwargs')
            kwargs = {**kwargs, **nested_kwargs}

        finalize    = kwargs.pop('finalize', False)
        table       = kwargs.pop('key', kwargs.pop('table', None))
        insert_mode   = kwargs.pop('if_exists', 'append')
        batch_key   = kwargs.pop('batch_key', None)

        if table is None and len(args) > 0:
            table = args[0]
        
        raw_stream_id, _, data_provider = parse_stream_id(table, self.providers)
        stream_id = generate_stream_id(raw_stream_id)
        buffer_key = batch_key or stream_id

        data = data.reset_index()
        data['date'] = pd.to_datetime(data['date']).astype('int64') // 10**9
        records = data[['date', 'value']].to_dict(orient='records')

        if buffer_key not in self._batch_buffer:
            self._batch_buffer[buffer_key] = []

        self._batch_buffer[buffer_key].append({'stream_id': stream_id, 'inputs': records, 'data_provider': data_provider })

        if finalize:
            self.logging_manager.log_info(f'Finalizing batch insert for: {buffer_key}')
            batches = self._batch_buffer.pop(buffer_key)

            stream_infos: List[StreamLocatorInput] = [
                {'stream_id': batch['stream_id'], 'data_provider': batch['data_provider']}
                for batch in batches
            ]

            exists_result = self.client.batch_stream_exists(stream_infos)

            # filter the streams that needs to be created or removed
            streams_to_create: List[StreamDefinitionInput] = []
            for result in exists_result:
                sid = result['stream_id']
                if not result['exists']:
                    streams_to_create.append(StreamDefinitionInput(stream_id=sid, stream_type=STREAM_TYPE_PRIMITIVE))
                elif insert_mode == 'replace':
                    self.drop_stream(sid)
                    streams_to_create.append(StreamDefinitionInput(stream_id=sid, stream_type=STREAM_TYPE_PRIMITIVE))

            # create streams that are not deployed
            if streams_to_create:
                self.batch_create_streams(streams_to_create)

            # insert records
            cleaned_batches: List[RecordBatch] = [
                {k: v for k, v in batch.items() if k != 'data_provider'}
                for batch in batches
            ]

            final_batches: List[List[RecordBatch]] = self._split_batches(cleaned_batches)
            for i, batch in enumerate(final_batches, start=1):
                self.write_batch(batch, i, len(final_batches))
                time.sleep(2)

    def _split_batches(self, batches: List[RecordBatch]):
        final_batches: List[List[RecordBatch]] = []

        queue = deque()
        for batch in batches:
            stream_id = batch['stream_id']
            inputs = batch['inputs']
            # Split into manageable chunks for this stream
            for i in range(0, len(inputs), self.batch_size):
                chunk = inputs[i:i + self.batch_size]
                queue.append({'stream_id': stream_id, 'inputs': chunk})

        while queue:
            current_batch: List[RecordBatch] = []
            current_count = 0

            while queue and current_count < self.batch_size:
                peek = queue[0]
                chunk_size = len(peek['inputs'])

                if current_count + chunk_size <= self.batch_size:
                    current_batch.append(queue.popleft())
                    current_count += chunk_size
                else:
                    remaining_space = self.batch_size - current_count
                    chunk = queue.popleft()
                    current_batch.append({
                        'stream_id': chunk['stream_id'],
                        'inputs': chunk['inputs'][:remaining_space]
                    })
                    queue.appendleft({
                        'stream_id': chunk['stream_id'],
                        'inputs': chunk['inputs'][remaining_space:]
                    })
                    current_count += remaining_space
                    break

            final_batches.append(current_batch)
        return final_batches
            
    def write_batch(self, batches: List[RecordBatch], current_batch: int, total_batches: int):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                insert_tx = self.client.batch_insert_records(batches)
                self.client.wait_for_tx(insert_tx)
                self.logging_manager.log_info(f'Data saved to TN database successfully. Batch {current_batch} from {total_batches}')
                return 
            except Exception as e:
                if attempt < MAX_RETRIES:
                    self.logging_manager.log_exception(
                        f"[Attempt {attempt}] Error inserting records: {e}"
                    )
                    time.sleep(RETRY_DELAY * attempt)
                else:
                    self.logging_manager.log_exception(f'Error saving data to TN database: {e}')
                    raise

    def gen_batch(self, date_from, date_to, interval = 31_536_000):
        batches = []
        
        if date_from and date_to:
            start = date_from  # start from the next second
            end   = date_to
            while start < end:
                batch_end = min(start + interval, end)
                batches.append((start, batch_end))
                start = batch_end + 1
        else:
            # no batching needed, one single call
            batches = [(date_from, date_to)]
        
        return batches

    def drop_stream(self, stream_id: str):
        self.logging_manager.log_info(f"Dropping stream '{stream_id}'...")
        
        try:
            drop_tx = self.client.destroy_stream(stream_id)
            self.client.wait_for_tx(drop_tx)
            self.logging_manager.log_info(f"Stream '{stream_id}' dropped successfully.")
        except Exception as e:
            self.logging_manager.log_error(f"Error dropping stream '{stream_id}': {e}")
            raise

    def batch_create_streams(self, stream_ids: list[StreamDefinitionInput]):
        self.logging_manager.log_info("Creating streams")
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                stream_tx = self.client.batch_deploy_streams(stream_ids)
                self.client.wait_for_tx(stream_tx)
                log_msg = "Streams created successfully:\n" + "\n".join(f" - {s['stream_id']}" for s in stream_ids)
                self.logging_manager.log_info(log_msg)
                return
            except Exception as e:
                if attempt < MAX_RETRIES:
                    self.logging_manager.log_exception(
                        f"[Attempt {attempt}] Error creating streams: {e}"
                    )
                    time.sleep(RETRY_DELAY * attempt)
                else:
                    self.logging_manager.log_exception(f"Error creating streams: {e}")
                    raise


def connector_factory_function(connector_type: str) -> Connector | None:
    if connector_type == 'trufnetwork':
        return TNConnector()
    return None

add_connector_factory(connector_factory_function)
        
