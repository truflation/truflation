import os
import sys
import datetime
from unittest import mock
import pytest
from icecream import ic
from sqlalchemy import DATETIME, VARCHAR, Column, create_engine, func, select, MetaData, Table
from sqlalchemy.exc import OperationalError
# from truflation.data._metadata_handler import _MetadataHandler 

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(SCRIPT_DIR, ".."))

from src.truflation.data._metadata_handler import _MetadataHandler

mock_frequency = '''[
    {
        "index": "mock_category_mock_name",
        "frequency": "Daily",
        "other": "No",
        "exact": 1
    }
]'''

env = {"USE_METADATA_HANDLER": "1"}

@pytest.fixture(scope="session")
def handler():
    engine = create_engine('sqlite:///:memory:')

    with mock.patch.dict(os.environ , env), \
         mock.patch("os.path.exists", return_value=True), \
         mock.patch("builtins.open", mock.mock_open(read_data=mock_frequency)):
         
         handler = _MetadataHandler(engine=engine)

    yield handler

def test_create_mock_index_table(handler):
    _mockTable = Table(
        'mock_category_mock_name',
        handler.metadata,
        Column('date', DATETIME),
        Column('value', VARCHAR(255)),
        Column('created_at', DATETIME),
    )
    
    try:
        # Create the table
        handler.metadata.create_all(handler.engine)
        ic('Table mock_category_mock_name created successfully.')
        
    except OperationalError as err:
        ic('An error occurred while creating mock_category_mock_name table: {err}')
        
    with handler.engine.connect() as conn:
        try:
            insert_query = _mockTable.insert().values(
                                date = datetime.date(2024, 3, 31),
                                value = '123.123',
                                created_at = datetime.datetime(2024, 4, 1, 23, 2, 9),
                            )
            conn.execute(insert_query)
            conn.commit()
            ic('Row inserted successfully into mock_category_mock_name table')
            
        except OperationalError as err:
            ic(f'An error occurred while inserting row: {err}')
            conn.rollback()

def test_get_frequency(handler):
    frequency = handler.get_frequency_data("mock_category_mock_name")
    
    assert frequency['frequency'] == 'Daily'
    assert frequency['exact'] == 1

def test_validate_table(handler):
    assert handler.validate_table('mock_category_mock_name') is True
    assert handler.validate_table('__metadata__') is False

def test_create_table(handler):
    handler.metadata = MetaData()
    handler.create_table()

def test_add_index(handler):
    handler.add_index("mock_category_mock_name")

    with handler.engine.connect() as conn:
        try:
            _metadata = Table(handler.table, handler.metadata, autoload_with = handler.engine)

            count_query = select(func.count()).select_from(_metadata)
            count_result = conn.execute(count_query).scalar()
            assert count_result == 6

            query = select(_metadata).where(_metadata.c.table_name == "mock_category_mock_name")
            result = conn.execute(query).fetchall()

            assert result is not None
            assert result[0][1] == "category"
            assert result[0][2] == "mock_category"

            assert result[1][1] == "name"
            assert result[1][2] == "mock_name"
            
            assert result[2][1] == "latest_date"
            assert result[2][2] == '2024-03-31'
            
            assert result[3][1] == "last_update"
            assert result[3][2] == '2024-04-01 23:02:09'

            assert result[4][1] == "frequency"
            assert result[4][2] == "Daily"
            
            assert result[5][1] == "other"
            assert result[5][2] == "No"

        except OperationalError as err:
            ic(f'{err}')
            conn.rollback()

def test_insert_row_manually(handler):
    
    handler.insert_row("mock_category_mock_name", "latest_date", "2000-1-1" , "string" )
    with handler.engine.connect() as conn:
        try:
            _metadata = Table(handler.table, handler.metadata, autoload_with = handler.engine)
            query = select(_metadata).where(_metadata.c.table_name == "mock_category_mock_name").where(_metadata.c._key == "latest_date")
            result = conn.execute(query).fetchone()
            assert result[2] == '2000-1-1'
        except OperationalError as err:
            ic(f'{err}')
            conn.rollback()

def test_reset(handler):
    handler.reset()

    with handler.engine.connect() as conn:
        try:
            _metadata = Table(handler.table, handler.metadata, autoload_with = handler.engine)

            count_query = select(func.count()).select_from(_metadata)
            count_result = conn.execute(count_query).scalar()
            assert count_result == 6

            query = select(_metadata).where(_metadata.c.table_name == "mock_category_mock_name")
            result = conn.execute(query).fetchall()

            assert result is not None
            assert result[0][1] == "category"
            assert result[0][2] == "mock_category"

            assert result[1][1] == "name"
            assert result[1][2] == "mock_name"
            
            assert result[2][1] == "latest_date"
            assert result[2][2] == '2024-03-31'
            
            assert result[3][1] == "last_update"
            assert result[3][2] == '2024-04-01 23:02:09'

            assert result[4][1] == "frequency"
            assert result[4][2] == "Daily"
            
            assert result[5][1] == "other"
            assert result[5][2] == "No"

        except OperationalError as err:
            ic(f'{err}')
            conn.rollback()

def test_empty_metadata(handler):

    handler.empty_metadata_table()

    with handler.engine.connect() as conn:
        try:
            _metadata = Table(handler.table, handler.metadata, autoload_with = handler.engine)
            count_query = select(func.count()).select_from(_metadata)
            count_result = conn.execute(count_query).scalar()

            assert count_result == 0

        except OperationalError as err:
            ic(f'{err}')
            conn.rollback()