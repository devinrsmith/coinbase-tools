#!/usr/bin/env python

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List
import aiokafka
import asyncio
import websockets

from . import subscribe_full

@dataclass
class Config:
    bootstrap_servers : List[str]
    topic: str
    product_partitions : Dict[str, int]

def key_serializer(key):
    return None if not key else bytes(key, encoding='utf8')

def value_serializer(value):
    return bytes(value, encoding='utf8')

async def _proxy_to_kafka(ws, producer : aiokafka.AIOKafkaProducer, topic : str, key_bytes : bytes, partition : int):
    batches = 0
    records = 0
    bytes = 0
    offset = 0
    leftover_msg = None
    while True:
        batch = producer.create_batch()
        first_msg = leftover_msg or await ws.recv()
        leftover_msg = None
        if not batch.append(key=key_bytes, value=value_serializer(first_msg), timestamp=None):
            raise RuntimeError("can't even send one message")
        while True:
            try:
                next_msg = await asyncio.wait_for(ws.recv(), 0.000000001)
            except asyncio.TimeoutError:
                break
            if not batch.append(key=key_bytes, value=value_serializer(next_msg), timestamp=None):
                leftover_msg = next_msg
                break
        batch.close()
        fut = await producer.send_batch(batch, topic, partition=partition)
        record = await fut

        records = records + batch.record_count()
        bytes = bytes + batch.size()
        offset = record.offset
        batches = batches + 1
        if batches % 100 == 1:
            print(f"{batches},{records},{bytes},{offset}")

async def _subscribe_full_and_proxy(producer : aiokafka.AIOKafkaProducer, topic : str, product_ids: List[str], key_bytes : bytes, partition : int):
    async with websockets.connect(subscribe_full.COINBASE_WS_FEED, compression=None) as ws:
        await subscribe_full._subscribe_full(ws, product_ids)
        await ws.recv() # skip subscribe response
        await _proxy_to_kafka(ws, producer,topic, key_bytes, partition)

async def _run_all(config : Config):
    partition_dict = defaultdict(list)
    for (product_id, partition) in config.product_partitions.items():
        partition_dict[partition].append(product_id)
    async with aiokafka.AIOKafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        key_serializer=key_serializer,
        value_serializer=value_serializer,
        compression_type=None) as producer:
        await asyncio.gather(
            *[ _subscribe_full_and_proxy(producer, config.topic, product_ids, None, partition) for (partition, product_ids) in partition_dict.items() ]
        )

def __entrypoint__():
    config = Config(
        bootstrap_servers=['10.150.0.4:9092'],
        topic='io.deephaven.coinbase.FullOrderbook',
        product_partitions={
            'BTC-USD' : 0,
            'ETH-USD' : 1,
            'USDT-USD' : 2,
            'ADA-USD' : 3,
            'DOGE-USD' : 4,
            'SHIB-USD': 5,
            'SOL-USD': 6,
            'EOS-USD': 7,
        }
    )
    asyncio.run(_run_all(config))

def __script_entrypoint__():
    return __entrypoint__()

def __main_entrypoint__():
    return __entrypoint__()

if __name__ == '__main__':
    __main_entrypoint__()
