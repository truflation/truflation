#!/usr/bin/env python3

import os
import time
import pandas as pd
import uuid
import random

from dotenv import load_dotenv
from icecream import ic
load_dotenv()

from truflation.data.connector import connector_factory
KWIL_USER = os.environ['KWIL_USER']
DB_NAME = 'foo5'
#DB_NAME = 'x71b7d73977a13635dd00af3c4490a623bafd2698567fce772bcdb1f0'

# Create KWIL connector
connector = connector_factory('kwil:')
ic(connector.has_schema('foo6'))

ic(connector.ping())
ic(connector.list_databases())
exit(0)

result = ic(connector.deploy(DB_NAME))
ic(connector.query_tx_wait(result['result']['tx_hash']))

result = ic(connector.add_admin(DB_NAME, KWIL_USER))
ic(connector.query_tx_wait(result['result']['tx_hash']))
ic(connector.read_all(f'{DB_NAME}:prices'))
ic(connector.query(DB_NAME, 'select * from admin_users'))

# Sample data
data_fail = [
    {"id": 1, "date_value": "2021-12-01", "value": 10, "created_at": 123},
    {"id": 2, "date_value": "2021-12-02", "value": 15, "created_at": 456},
    {"id": 3, "date_value": "2021-12-03", "value": 20, "created_at": 789},
]

# Create DataFrame
data_frame_fail = pd.DataFrame(data_fail)

# Print the DataFrame
print(data_frame_fail)

def write_to_kwil(db_name, data_frame):
    result = connector.write_all(data_frame, f'{db_name}:prices')
    ic(result)
    print('Waiting for transaction to clear')
    ic(connector.query_tx_wait(result['result']['tx_hash']))
    ic(connector.read_all(f'{db_name}:prices'))


# Write data_frame_fail to KWIL
write_to_kwil(DB_NAME, data_frame_fail)

# Generate pseudo-random UUIDs for data_frame_success
random.seed(42)
data_frame_success = data_frame_fail.copy()
data_frame_success["id"] = data_frame_success["id"].apply(lambda x: str(uuid.uuid4()))

# Write data_frame_success to KWIL
write_to_kwil(DB_NAME, data_frame_success)
