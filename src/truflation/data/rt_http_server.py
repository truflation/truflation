#!/usr/bin/env python3
import os
from datetime import datetime
import ujson

from sanic import Sanic
from sanic.response import json, text
from jwcrypto import jwk, jwt
from sqlalchemy import text, select

from dotenv import load_dotenv
from icecream import ic
from truflation.data.connector import connector_factory
from truflation.data.source_details import SourceDetails
from truflation.data.connector import get_database_handle

load_dotenv()

app = Sanic('rt_http_server')
BASE_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "json")
LIMIT=5

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

@app.route("/auth")
async def auth(request):
    pubkey = os.environ.get('RT_PUB_KEY')
    if pubkey is not None:
        key = jwk.JWK.from_pem(pubkey.encode('utf-8'))
        return json(key)
    return json({})

def sign_json(payload):
    privkey = os.environ.get('RT_PRIV_KEY')
    if privkey is not None:
        key = jwk.JWK.from_pem(privkey.encode('utf-8'))
        token = jwt.JWT(header={'alg': 'ES512'},
                        claims=payload)
        token.make_signed_token(key)
        jresponse = json(payload | { 'j': token.serialize() })
        jresponse.headers['Authorization'] = 'Bearer ' + token.serialize()
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
        ic(e)
        return json({
            's': 'error'
        })

@app.route("/data/<filename:[A-z][A-z0-9_.]*>")
async def get_file_contents(_, filename):
    try:
        # Construct the full path to the file
        file_path = os.path.join(BASE_DIRECTORY, filename)
        if not is_valid_file_path(file_path):
            return text("Invalid file", status=403)
        with open(file_path, "r", encoding='utf-8') as file:
            file_type = get_file_type(filename)
            if file_type == 'json':
                return sign_json(ujson.load(file))
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
