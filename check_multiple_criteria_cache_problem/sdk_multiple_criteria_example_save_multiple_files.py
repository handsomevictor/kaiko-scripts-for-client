
"""
Use the Kaiko Stream API to subscribe to market updates - input exchange and pairs customization

Here the logic is to use concurrent processes (or threads) to subscribe to multiple exchanges and pairs, so that
the input parameter can be customized.
"""

from __future__ import print_function
import logging

import grpc
from google.protobuf.json_format import MessageToJson

from kaikosdk import sdk_pb2_grpc
from kaikosdk.core import instrument_criteria_pb2
from kaikosdk.stream.market_update_v1 import request_pb2 as pb_market_update
from kaikosdk.stream.market_update_v1 import commodity_pb2 as pb_commodity
from kaikosdk.stream.aggregates_ohlcv_v1 import request_pb2 as pb_ohlcv
from kaikosdk.stream.trades_v1 import request_pb2 as pb_trades

from concurrent.futures import ProcessPoolExecutor
import os


# def trade_request(exchange, instrument_class, code):
#     credentials = grpc.ssl_channel_credentials(root_certificates=None)
#     call_credentials = grpc.access_token_call_credentials(os.environ['KAIKO_API_KEY'])
#     composite_credentials = grpc.composite_channel_credentials(credentials, call_credentials)
#     channel = grpc.secure_channel('gateway-v0-grpc.kaiko.ovh', composite_credentials)
#
#     try:
#         with channel:
#             stub = sdk_pb2_grpc.StreamTradesServiceV1Stub(channel)
#             responses = stub.Subscribe(pb_trades.StreamTradesRequestV1(
#                 instrument_criteria=instrument_criteria_pb2.InstrumentCriteria(
#                     exchange=exchange,
#                     instrument_class=instrument_class,
#                     code=code
#                 )
#             ))
#             for response in responses:
#                 print("Received message %s" % (MessageToJson(response, including_default_value_fields = True)))
#                 # save data to local
#                 with open('data_example_trades.txt', 'a') as f:
#                     f.write(MessageToJson(response, including_default_value_fields=True) + '\n')
#     except grpc.RpcError as e:
#         print(e.details(), e.code())


def ohlcv_request(exchange, instrument_class, code):
    credentials = grpc.ssl_channel_credentials(root_certificates=None)
    call_credentials = grpc.access_token_call_credentials(os.environ['KAIKO_API_KEY'])
    composite_credentials = grpc.composite_channel_credentials(credentials, call_credentials)
    channel = grpc.secure_channel('gateway-v0-grpc.kaiko.ovh', composite_credentials)
    try:
        with channel:
            stub = sdk_pb2_grpc.StreamAggregatesOHLCVServiceV1Stub(channel)
            responses = stub.Subscribe(pb_ohlcv.StreamAggregatesOHLCVRequestV1(
                aggregate='1s',
                instrument_criteria=instrument_criteria_pb2.InstrumentCriteria(
                    exchange=exchange,
                    instrument_class=instrument_class,
                    code=code
                )
            ))
            for response in responses:
                print("Received message %s" % (MessageToJson(response, including_default_value_fields=True)))
                with open('data_example_ohlcv.txt', 'a') as f:
                    f.write(MessageToJson(response, including_default_value_fields=True) + '\n')
    except grpc.RpcError as e:
        print(e.details(), e.code())


def run():
    # target = {
    #     'first': {
    #         'exchange': 'binc',
    #         'instrument_class': 'spot',
    #         'code': 'btc-usd,eth-btc'
    #     },
    #     'second': {
    #         'exchange': 'btmx',
    #         'instrument_class': 'spot',
    #         'code': 'btc-usdt'
    #     },
    #     'third': {
    #         'exchange': 'cbse',
    #         'instrument_class': 'spot',
    #         'code': 'btc-usd'
    #     }
    # }

    target = {
        'first': {
            'exchange': 'bnce,bfnx,stmp,btrx,cbse,gmni,krkn,polo,itbi',
            'instrument_class': 'spot',
            'code': 'aave-usd,aave-usdt,ada-usd,ada-usdt,alcx-usd,alcx-usdt,algo-usd,algo-usdt,alpha-usd,alpha-usdt,'
                    'ant-usd,ant-usdt,ape-usd,ape-usdt,ar-usd,ar-usdt,astr-usd,astr-usdt,atom-usd,atom-usdt,audio-usd,'
                    'audio-usdt,avax-usd,avax-usdt,axs-usd,axs-usdt,badger-usd,badger-usdt,bal-usd,bal-usdt,bat-usd,'
                    'bat-usdt,bch-usd,bch-usdt,bnb-usd,bnb-usdt,btc-usd,btc-usdt,btg-usd,btg-usdt,bts-usd,bts-usdt,'
                    'busd-usd,busd-usdt,cake-usd,cake-usdt,cel-usd,cel-usdt,celo-usd,celo-usdt,ckb-usd,ckb-usdt,comp-usd,'
                    'comp-usdt,crv-usd,crv-usdt,cvx-usd,cvx-usdt,dai-usd,dai-usdt,dash-usd,dash-usdt,dcr-usd,dcr-usdt,'
                    'doge-usd,doge-usdt,dot-usd,dot-usdt,dydx-usd,dydx-usdt,eos-usd,eos-usdt,etc-usd,etc-usdt,eth-usd,'
                    'eth-usdt,fil-usd,fil-usdt,ftm-usd,ftm-usdt,ftt-usd,ftt-usdt,fxs-usd,fxs-usdt,gala-usd,gala-usdt,'
                    'glmr-usd,glmr-usdt,grt-usd,grt-usdt,icx-usd,icx-usdt,ilv-usd,ilv-usdt,imx-usd,imx-usdt,iota-usd,'
                    'iota-usdt,joe-usd,joe-usdt,kava-usd,kava-usdt,ksm-usd,ksm-usdt,leo-usd,leo-usdt,link-usd,link-usdt,'
                    'lrc-usd,lrc-usdt,lsk-usd,lsk-usdt,ltc-usd,ltc-usdt,mana-usd,mana-usdt,matic-usd,matic-usdt,mina-usd,'
                    'mina-usdt,mith-usd,mith-usdt,mkr-usd,mkr-usdt,near-usd,near-usdt,neo-usd,neo-usdt,nexo-usd,nexo-usdt,'
                    'nmr-usd,nmr-usdt,omg-usd,omg-usdt,one-usd,one-usdt,ont-usd,ont-usdt,perp-usd,perp-usdt,qtum-usd,'
                    'qtum-usdt,ray-usd,ray-usdt,ren-usd,ren-usdt,rep-usd,rep-usdt,rune-usd,rune-usdt,sand-usd,sand-usdt,'
                    'sc-usd,sc-usdt,scrt-usd,scrt-usdt,shib-usd,shib-usdt,snx-usd,snx-usdt,sol-usd,sol-usdt,srm-usd,'
                    'srm-usdt,steem-usd,steem-usdt,stx-usd,stx-usdt,sushi-usd,sushi-usdt,trx-usd,trx-usdt,uma-usd,'
                    'uma-usdt,uni-usd,uni-usdt,usdc-usd,usdc-usdt,usdt-usd,usdt-usdt,vet-usd,vet-usdt,waves-usd,'
                    'waves-usdt,wbtc-usd,wbtc-usdt,xem-usd,xem-usdt,xlm-usd,xlm-usdt,xmr-usd,xmr-usdt,xrp-usd,xrp-usdt,'
                    'xtz-usd,xtz-usdt,zec-usd,zec-usdt,zrx-usd,zrx-usdt'
        }
    }

    exchange_lst = [i['exchange'] for i in target.values()]
    instrument_class_lst = [i['instrument_class'] for i in target.values()]
    code_lst = [i['code'] for i in target.values()]

    # save data to local, create a txt file, if it exists, delete it
    if os.path.exists('data_example_ohlcv.txt'):
        os.remove('data_example_ohlcv.txt')
        print('delete old data file')

    with ProcessPoolExecutor(max_workers=len(target)) as pool:
        pool.map(ohlcv_request, exchange_lst, instrument_class_lst, code_lst)


if __name__ == '__main__':
    logging.basicConfig()
    run()
