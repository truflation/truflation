#!/usr/bin/env python3
import os
import traceback
from datetime import datetime
import ujson

from sanic import Sanic, exceptions
from sanic.response import json, text

from sqlalchemy import text, select

from dotenv import load_dotenv
from icecream import ic
from truflation.data.connector import connector_factory
from truflation.data.source_details import SourceDetails
from truflation.data.connector import get_database_handle
from truflation.data.signer import Signer

load_dotenv()

app = Sanic('rt_http_server')
BASE_DIRECTORY = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "json"
)
LIMIT=5
signer = Signer.factory(
    os.environ.get('RT_HTTP_SERVER_SIGNER')
)

def set_base_directory(base_directory: str):
    global BASE_DIRECTORY
    BASE_DIRECTORY = base_directory

def set_signer(sign_string, *args, **kwargs):
    global signer
    signer = Signer.factory(sign_string, *args, **kwargs)

def is_valid_file_path(file_path):
    if not file_path.startswith(BASE_DIRECTORY):
        return False
    if not os.path.isfile(file_path):
        return False
    return True

def get_file_type(filename):
    if filename.endswith(".csv"):
        return "csv"
    if filename.endswith(".json"):
        return "json"
    return "unknown"

def convdate(timestamp):
    return str(datetime.utcfromtimestamp(
        int(timestamp)/1000
    ))

@app.route("/auth-info")
async def auth_info(request):
    return json(signer.auth_info())

def sign_json(payload):
    payload_sign = signer.preprocess(payload)
    signature = signer.signature(payload_sign)
    if signature is not None:
        if isinstance(signature, dict):
            jresponse = json(payload_sign | signature)
        if isinstance(signature, str):
            jresponse = json(payload_sign | { 'i': signature })
            jresponse.headers['Authorization'] = 'Bearer ' + signature
        return jresponse
    return json(payload)

@app.route("/history")
async def get_history(request):
    """
See spec at

https://www.tradingview.com/charting-library-docs/latest/connecting_data/UDF/
    """
    query = request.args
    try:
        symbol = query.get('symbol')
        table, column = symbol.split(':')
        start = query.get('from')
        end = query.get('to')
        countback = int(query.get('countback', LIMIT))

        sqlstring = \
            f"SELECT date, {column} " \
            "FROM custom_index_fiat " \
            "WHERE category = :table "

        params = {'table': table, 'countback': countback}

        if start is not None:
            sqlstring += "and date >= :start "
            params['start'] =convdate(start)
        if end is not None:
            sqlstring += "and date <= :end "
            params['end'] = convdate(end)
        sqlstring += "ORDER BY date DESC " \
            "LIMIT :countback "

        sqlstring = text(
            f"select * from ( {sqlstring} ) as sub order by date ASC"
        )
        sqlstring = sqlstring.bindparams(**params)

        source_details = SourceDetails(
            'tables',
            get_database_handle(),
            sqlstring
        )
        reader = connector_factory(source_details.source_type)
        df = reader.read_all(
            source_details.source,
            *source_details.args,
            **source_details.kwargs
        )

        j = {
            's': 'ok',
            't': df['date'].map(
                lambda x: int( datetime.timestamp(x) * 1000)
            ).tolist(),
            'c': df[column].tolist()
        }
        return sign_json(j)
    except Exception as e:
        print(e)
        traceback.print_tb(e.__traceback__)
        return json({
            's': 'error'
        })

@app.route("/data/<filename:[A-z][A-z0-9_.]*>")
async def get_file_contents(_, filename):

    # Construct the full path to the file
    file_path = os.path.join(BASE_DIRECTORY, filename)
    if not is_valid_file_path(file_path):
        return json({"error": "Invalid file"}, status=404)
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            file_type = get_file_type(filename)
            if file_type == 'json':
                return json(ujson.load(file))
            contents = file.read()
            response = text(contents)
            response.headers["Content-Type"] = f"application/{file_type}"
            return response
    except FileNotFoundError:
        return json({"error": "File not found"}, status=404)
    except Exception as e:
        return json({"error": str(e)}, status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
