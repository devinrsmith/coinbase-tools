#!/usr/bin/env python

from dataclasses import dataclass
from typing import Dict, List, Optional, Set
import aiokafka
import asyncio
from dataclasses_json import dataclass_json
import time
import argparse

from aiokafka.helpers import create_ssl_context
import websockets

from . import feed as coinbase_feed

@dataclass_json
@dataclass
class Config:
    # TODO: separate out kafka config into own object

    bootstrap_servers : List[str]
    topic: str
    partitions : Dict[int, List[str]]

    max_memory: int = 2 ** 25 # 32 MiB
    # max_batch_bytes: int = 2 ** 20 # 1 MiB
    max_linger: float = 0.0
    client_id: Optional[str] = None
    producer_compression_type: Optional[str] = None
    feed: coinbase_feed.Config = coinbase_feed.Config()

    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = "PLAIN"
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None

    @property
    def feed_max_memory(self) -> int:
        return int(self.max_memory / len(self.partitions) / 2)

    @property
    def max_batch_memory(self) -> int:
        return int(self.max_memory / len(self.partitions) / 2)

    # @property
    # def max_memory_estimate(self) -> int:
    #     # * 2, to account for ws recv buffer and producer send buffer
    #     return len(self.partitions) * self.max_batch_bytes * 2

    # @property
    # def partitions(self) -> Set[int]:
    #     return set(self.product_partitions.values())

    # def partitions(self) -> Set[int]:
    #     return set(self.product_partitions.values())

    # def products_for(self, partition : int) -> List[str]:
    #     product_ids = []
    #     for (product_id, p) in self.product_partitions.items():
    #         if partition == p:
    #             product_ids.append(product_id)
    #     return product_ids

    async def _subscribe_full_and_proxy(self, producer : aiokafka.AIOKafkaProducer, partition : int):
        product_ids = self.partitions[partition]
        # todo: async for connection in self.feed.connect(self.feed_max_memory):
        # todo: allow configuration
        while True:
            # We'll match the websocket buffer size with the amount of bytes we can batch up so the producer/consumer are in sympathy
            async with self.feed.connect(self.feed_max_memory) as connection:
                try:
                    await connection.subscribe_full(product_ids)
                    await _proxy_to_kafka_print(connection, producer, self.topic, partition, self.max_batch_memory, self.max_linger)
                except websockets.ConnectionClosed as e:
                    print('Reconnecting after err:', e)
                    continue

    async def run(self):
        async with aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=self.client_id,
            max_batch_size=self.max_batch_memory,
            compression_type=self.producer_compression_type,
            ssl_context=create_ssl_context(),
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.sasl_plain_username,
            sasl_plain_password=self.sasl_plain_password) as producer:
            await asyncio.gather(
                *[ self._subscribe_full_and_proxy(producer, partition) for partition in self.partitions.keys() ]
            )

@dataclass
class Metrics:
    partition: int
    batches : int
    records : int
    bytes : int
    offset : int
    ws_queue_count : int

def _append(batch, msg, time):
    return batch.append(key=None, value=bytes(msg, encoding='utf8'), timestamp=int(time * 1000))

async def _proxy_to_kafka(connection : coinbase_feed.Connection, producer : aiokafka.AIOKafkaProducer, topic : str, partition : int, max_linger : float):
    batches = 0
    records = 0
    bytes = 0
    offset = 0
    ws_queue_count = 0
    leftover = None
    while True:
        batch = producer.create_batch()
        # Encountering full queues is likely a sign of falling behind, or not having enough buffer space to handle bursts
        ws_queue_count = ws_queue_count + connection.queue_length()
        if leftover:
            (msg, now) = leftover
            leftover = None
        else:
            msg = await connection.recv_raw()
            now = time.time()
        if not _append(batch, msg, now):
            raise RuntimeError("can't even send one message")
        linger_time = now + max_linger
        linger_timeout = max_linger
        while True:
            msg = await connection.recv_raw_timeout(linger_timeout)
            if not msg:
                break
            now = time.time()
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

async def _proxy_to_kafka_print(connection : coinbase_feed.Connection, producer : aiokafka.AIOKafkaProducer, topic : str, partition : int, max_batch_bytes : int, max_linger : float):
    last_metric = None
    async for metric in _proxy_to_kafka(connection, producer, topic, partition, max_linger):
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
    print(config.to_json())
    asyncio.run(config.run())

if __name__ == '__main__':
    main()
