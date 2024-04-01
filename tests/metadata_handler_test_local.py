import os
from tokenize import String
from unittest import mock
import pytest
from sqlalchemy import DATETIME, VARCHAR, Column, create_engine, func, select, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError
from truflation.data._metadata_handler import _MetadataHandler 

mock_frequency = '''[
    {
        "index": "mock_category_mock_name",
        "frequency": "Daily",
        "other": "No",
        "exact": 1
    }
]'''

env = {"USE_METADATA_HANDLER": "1"}

@pytest.fixture
def handler():

    engine = create_engine('sqlite:///:memory:')
    # SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    # metadata = MetaData()

    with mock.patch.dict(os.environ , env), \
         mock.patch("os.path.exists", return_value=True), \
         mock.patch("builtins.open", mock.mock_open(read_data=mock_frequency)):
         
         handler = _MetadataHandler(engine)
    #      metadata.create_all(engine)

    # yield handler
    # engine.dispose()
    return handler

# def test_create_mock_index_table(handler):

#         # self._metadata = Table(
#         #     self.table,
#         #     self.metadata,
#         #     Column('table_name', VARCHAR(255)),
#         #     Column('_key', VARCHAR(255)),
#         #     Column('value', VARCHAR(255)),
#         #     Column('value_type', VARCHAR(255)),
#         #     Column('created_at', DATETIME),
#         #     Column('updated_at', DATETIME),
#         # )
        
#         # try:
#         #     # Create the table
#         #     self.metadata.create_all(self.engine)
#         #     ic(f'Table {self.table} created successfully.')
            
#         # except OperationalError as err:
#         #     ic(f'An error occurred while creating {self.table} table: {err}')

#     self.engine = engine

#     _mockTable = Table(
#         'mock_category_mock_name',
#         MetaData(),
#         Column('date', VARCHAR(255)),
#         Column('value', VARCHAR(255)),
#         Column('created_at', DATETIME),
#     )

def test_get_frequency(handler):
    frequency = handler.get_frequency_data("mock_category_mock_name")
    assert frequency['frequency'] == 'Daily'
    assert frequency['exact'] == 1

def test_validata_table(handler):
    assert handler.validate_table('mock_category_mock_name') is True
    assert handler.validate_table('__metadata__') is False

def test_create_table(handler):
    handler.metadata = MetaData()
    handler.create_table()

def test_add_index(handler):
    
    print(f'engine: {handler.engine}');
    handler.add_index("mock_category_mock_name");

    with handler.engine.connect() as conn:
        try:
            _metadata = Table(handler.table, handler.metadata, autoload_with = handler.engine)
            query = select(_metadata).where(_metadata.c.table_name == "mock_category_mock_name").where(_metadata.c._key == "last_update")
            result = conn.execute(query).fetchone()

            count_query = select(func.count()).select_from(_metadata)
            count_result = conn.execute(count_query).scalar()
            assert count_result == 6

            assert result is not None
            assert result[0] == "mock_category_mock_name"
            

            query = select(_metadata).where(_metadata.c.table_name == "mock_category_mock_name")
            result = conn.execute(query).fetchall()

            print(f'result: {result}')

            assert result is not None
            assert result[0][1] == "category"
            assert result[0][2] == "com_aaa"

            assert result[1][1] == "name"
            assert result[1][2] == "us_gasoline"

            assert result[4][1] == "frequency"
            assert result[4][2] == "Daily"


        except OperationalError as err:
            print(f'{err}');
            conn.rollback()

def test_insert_row_manually(handler):
    
    handler.insert_row("mock_category_mock_name", "last_update", "2000-1-1" , "string" )
    with handler.engine.connect() as conn:
        try:
            _metadata = Table(handler.table, handler.metadata, autoload_with = handler.engine)
            query = select(_metadata).where(_metadata.c.table_name == "mock_category_mock_name").where(_metadata.c._key == "last_update")
            result = conn.execute(query).fetchone()
            assert result[2] == '2000-1-1'
        except OperationalError as err:
            print(f'{err}');
            conn.rollback()
    
def test_empty_metadata(handler):
    
    handler.empty_metadata_table();

    with handler.engine.connect() as conn:
        try:
            _metadata = Table(handler.table, handler.metadata, autoload_with = handler.engine)
            count_query = select(func.count()).select_from(_metadata)
            count_result = conn.execute(count_query).scalar()

            assert count_result == 0

        except OperationalError as err:
            print(f'{err}');
            conn.rollback()

# def test_reset(handler):
    
#     handler.reset();

#     with handler.engine.connect() as conn:
#         try:
#             _metadata = Table(handler.table, handler.metadata, autoload_with = handler.engine)
#             count_query = select(func.count()).select_from(_metadata)
#             count_result = conn.execute(count_query).scalar()

#             assert count_result == 0

#         except OperationalError as err:
#             print(f'{err}');
#             conn.rollback()

