#!/usr/bin/env python

from collections import defaultdict
from dataclasses import dataclass
import json
from sre_constants import MIN_UNTIL
from typing import Dict, List, Optional
import aiokafka
import asyncio
from dataclasses_json import dataclass_json
import websockets
import time
import argparse

from . import subscribe_full

# Our max memory usage can be roughly calculated by:
# Num partitions * max_batch_bytes * 2

MIN_LINGER = 0.000000001

@dataclass_json
@dataclass
class Config:
    bootstrap_servers : List[str]
    topic: str
    product_partitions : Dict[str, int]
    max_batch_bytes: int = 2 ** 20 # 1 MiB
    max_linger: float = 0.0
    client_id: Optional[str] = None

    @property
    def max_queue(self) -> int:
        # max_queue * FULL_CHANNEL_MAX_BYTES_PER_MSG  == max_batch_bytes
        # =>
        # max_queue =
        return int(self.max_batch_bytes / subscribe_full.FULL_CHANNEL_MAX_BYTES_PER_MSG)

    async def _subscribe_full_and_proxy(self, producer : aiokafka.AIOKafkaProducer, product_ids: List[str], partition : int):
        async with websockets.connect(
            subscribe_full.COINBASE_WS_FEED,
            max_size = subscribe_full.FULL_CHANNEL_MAX_BYTES_PER_MSG,
            max_queue = self.max_queue,
            compression = None) as ws:
            await subscribe_full._subscribe_full(ws, product_ids)
            await ws.recv() # skip subscribe response
            await _proxy_to_kafka(ws, producer, self.topic, None, partition, self.max_linger)

    async def run(self):
        partition_dict = defaultdict(list)
        for (product_id, partition) in self.product_partitions.items():
            partition_dict[partition].append(product_id)
        async with aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            max_batch_size=self.max_batch_bytes,
            compression_type=None) as producer:
            await asyncio.gather(
                *[ self._subscribe_full_and_proxy(producer, product_ids, partition) for (partition, product_ids) in partition_dict.items() ]
            )

def key_serializer(key):
    return None if not key else bytes(key, encoding='utf8')

def value_serializer(value):
    return bytes(value, encoding='utf8')

async def _recv(ws):
    next_msg = await ws.recv()
    return next_msg, time.time()

async def _recv_timeout(ws, timeout):
    try:
        next_msg = await asyncio.wait_for(ws.recv(), max(MIN_LINGER, timeout))
    except asyncio.TimeoutError:
        return None
    return next_msg, time.time()

def _append(batch, msg, time, key_bytes : bytes):
    return batch.append(key=key_bytes, value=value_serializer(msg), timestamp=int(time * 1000))

async def _proxy_to_kafka(ws, producer : aiokafka.AIOKafkaProducer, topic : str, key_bytes : bytes, partition : int, max_linger : float):
    batches = 0
    records = 0
    bytes = 0
    offset = 0
    leftover = None
    while True:
        # if len(ws.messages) == ws.max_queue:
        #     print("warning: read queue full")
        batch = producer.create_batch()
        (msg, now) = leftover or await _recv(ws)
        if not _append(batch, msg, now, key_bytes):
            raise RuntimeError("can't even send one message")
        linger_time = now + max_linger
        linger_timeout = max_linger
        while True:
            next = await _recv_timeout(ws, linger_timeout)
            if not next:
                break
            (msg, now) = next
            if not _append(batch, msg, now, key_bytes):
                leftover = (msg, now)
                break
            # Note: even if linger_timeout <= 0, we still should pull anything that is "immediately" available from the ws.recv() queue
            linger_timeout = linger_time - now
        batch.close()
        fut = await producer.send_batch(batch, topic, partition=partition)
        record = await fut
        records = records + batch.record_count()
        bytes = bytes + batch.size()
        offset = record.offset
        batches = batches + 1
        print(f"{partition},{batches},{records},{bytes},{offset}")

def parse_args(args) -> Config:
    parser = argparse.ArgumentParser(description='Publish to kafka.')
    parser.add_argument('--config', type=argparse.FileType('r', encoding='UTF-8'), required=True)
    parsed_args = parser.parse_args(args)
    config_file = parsed_args.config
    with config_file:
        return Config.from_json(config_file.read())

def main(args):
    config = parse_args(args)
    print(config)
    asyncio.run(config.run())

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
