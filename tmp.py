"""
Solve client problem of: receiving fewer data after 45 minutes of streaming. Clearly this is not Kaiko's problem,
so this script is for writing every data point and the time to a file and then check the number of data points
per minute to see if it's our problem.
"""

from __future__ import print_function

import logging

import grpc
from google.protobuf.json_format import MessageToJson
import pandas as pd

import datetime
from kaikosdk import sdk_pb2_grpc
from kaikosdk.core import instrument_criteria_pb2
from kaikosdk.stream.market_update_v1 import request_pb2 as pb_market_update
from kaikosdk.stream.market_update_v1 import commodity_pb2 as pb_commodity
from kaikosdk.stream.aggregates_ohlcv_v1 import request_pb2 as pb_ohlcv
from kaikosdk.stream.trades_v1 import request_pb2 as pb_trades

from concurrent.futures import ProcessPoolExecutor
import os
import matplotlib.pyplot as plt


def trade_request(exchange, instrument_class, code):
    i = 0
    credentials = grpc.ssl_channel_credentials(root_certificates=None)
    call_credentials = grpc.access_token_call_credentials(os.environ['KAIKO_API_KEY'])
    composite_credentials = grpc.composite_channel_credentials(credentials, call_credentials)
    channel = grpc.secure_channel('gateway-v0-grpc.kaiko.ovh', composite_credentials)

    try:
        with channel:
            stub = sdk_pb2_grpc.StreamTradesServiceV1Stub(channel)
            responses = stub.Subscribe(pb_trades.StreamTradesRequestV1(
                instrument_criteria=instrument_criteria_pb2.InstrumentCriteria(
                    exchange=exchange,
                    instrument_class=instrument_class,
                    code=code
                )
            ))
            for response in responses:
                with open('check_UNKNOWN_problem/data_example_counts.txt', 'a') as f:
                    f.write(str(datetime.datetime.now()) + ';' + str(i) + '\n')
                    i += 1
                    print(i)
    except grpc.RpcError as e:
        print(e.details(), e.code())


def check_frequency():
    df = pd.read_csv('check_UNKNOWN_problem/data_example_counts.txt', sep=';', header=None, names=['datetime', 'count'])
    df['datetime'] = pd.to_datetime(df['datetime'])

    # count the number of rows in each minute
    df = df.groupby(pd.Grouper(key='datetime', freq='1min')).count()
    print(df)


if __name__ == '__main__':
    # remove data_example_counts.txt
    if os.path.exists('data_example_counts.txt'):
        os.remove('data_example_counts.txt')
    trade_request('binc', 'spot', 'btc-usdt')

    check_frequency()
