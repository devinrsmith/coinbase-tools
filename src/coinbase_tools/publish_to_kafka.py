#!/usr/bin/env python

from collections import defaultdict
from dataclasses import dataclass
from sre_constants import MIN_UNTIL
from typing import Dict, List, Optional, Set
import aiokafka
import asyncio
from dataclasses_json import dataclass_json
import websockets
import time
import argparse

from . import subscribe_full

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
    producer_compression_type: Optional[str] = None
    feed_uri: str = subscribe_full.COINBASE_WS_FEED
    feed_max_size: int = subscribe_full.FULL_CHANNEL_MAX_BYTES_PER_MSG

    @property
    def max_queue(self) -> int:
        # max_queue * FULL_CHANNEL_MAX_BYTES_PER_MSG  == max_batch_bytes
        # =>
        # max_queue =
        return int(self.max_batch_bytes / subscribe_full.FULL_CHANNEL_MAX_BYTES_PER_MSG)

    @property
    def partitions(self) -> Set[int]:
        return set(self.product_partitions.values())

    @property
    def max_memory_estimate(self) -> int:
        # * 2, to account for ws recv buffer and producer send buffer
        return len(self.partitions) * self.max_batch_bytes * 2

    @property
    def partition_dict(self):
        partition_dict = defaultdict(list)
        for (product_id, partition) in self.product_partitions.items():
            partition_dict[partition].append(product_id)
        return partition_dict

    async def _subscribe_full_and_proxy(self, producer : aiokafka.AIOKafkaProducer, product_ids: List[str], partition : int):
        async with websockets.connect(
            self.feed_uri,
            max_size = self.feed_max_size,
            max_queue = self.max_queue,
            compression = None) as ws:
            await subscribe_full._subscribe_full(ws, product_ids)
            await ws.recv() # skip subscribe response
            await _proxy_to_kafka_print(ws, producer, self.topic, partition, self.max_batch_bytes, self.max_linger)

    async def run(self):
        async with aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            max_batch_size=self.max_batch_bytes,
            compression_type=self.producer_compression_type) as producer:
            await asyncio.gather(
                *[ self._subscribe_full_and_proxy(producer, product_ids, partition) for (partition, product_ids) in self.partition_dict.items() ]
            )

@dataclass
class Metrics:
    partition: int
    batches : int
    records : int
    bytes : int
    offset : int
    ws_queue_count : int

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

def _append(batch, msg, time):
    return batch.append(key=None, value=value_serializer(msg), timestamp=int(time * 1000))

async def _proxy_to_kafka(ws, producer : aiokafka.AIOKafkaProducer, topic : str, partition : int, max_linger : float):
    batches = 0
    records = 0
    bytes = 0
    offset = 0
    ws_queue_count = 0
    leftover = None
    while True:
        batch = producer.create_batch()
        # Encountering full queues is likely a sign of falling behind, or not having enough buffer space to handle bursts
        ws_queue_count = ws_queue_count + len(ws.messages)
        (msg, now) = leftover or await _recv(ws)
        leftover = None
        if not _append(batch, msg, now):
            raise RuntimeError("can't even send one message")
        linger_time = now + max_linger
        linger_timeout = max_linger
        while True:
            next = await _recv_timeout(ws, linger_timeout)
            if not next:
                break
            (msg, now) = next
            if not _append(batch, msg, now):
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
        yield Metrics(partition, batches, records, bytes, offset, ws_queue_count)

async def _proxy_to_kafka_print(ws, producer : aiokafka.AIOKafkaProducer, topic : str, partition : int, max_batch_bytes : int, max_linger : float):
    last_metric = None
    async for metric in _proxy_to_kafka(ws, producer, topic, partition, max_linger):
        if not last_metric or int(last_metric.bytes / max_batch_bytes) != int(metric.bytes / max_batch_bytes):
            print(metric)
        last_metric = metric

def parse_args(args) -> Config:
    parser = argparse.ArgumentParser(description='Publish to kafka.')
    parser.add_argument('--config', type=argparse.FileType('r', encoding='UTF-8'), required=True)
    parsed_args = parser.parse_args(args)
    config_file = parsed_args.config
    with config_file:
        return Config.from_json(config_file.read())

def main(args=None):
    config = parse_args(args)
    print(config)
    asyncio.run(config.run())

if __name__ == '__main__':
    main()
