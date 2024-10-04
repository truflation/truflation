import pytest
from unittest import mock
from truflation.data.connector import RestToCsvConnector

@pytest.mark.asyncio
async def test_fetch_data_from_rest():
    connector = RestToCsvConnector("https://api.example.com/data", "test.csv")
    with mock.patch('requests.get') as mock_get:
        mock_get.return_value.json.return_value = {'key': 'value'}
        data = connector.fetch_data_from_rest()
        assert data == {'key': 'value'}
