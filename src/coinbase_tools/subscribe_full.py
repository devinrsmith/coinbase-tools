#!/usr/bin/env python

import argparse
import asyncio
from dataclasses import dataclass
import datetime
import sys
from typing import List
from dataclasses_json import dataclass_json

from . import feed as coinbase_feed

COINBASE_WS_FEED="wss://ws-feed.exchange.coinbase.com"

# max seen size: ~300?
# 1 KiB should be generous
FULL_CHANNEL_MAX_BYTES_PER_MSG = 2 ** 10

@dataclass_json
@dataclass
class Group:
    handle: str
    product_ids: List[str]
    max_memory: int = 2**20 # 1 MiB
    feed: coinbase_feed.Config = coinbase_feed.Config()

    async def _subscribe_full_and_write(self, out):
        async with self.feed.connect(self.max_memory) as connection:
            await connection.subscribe_full(self.product_ids)
            out.write("wire_time,product_id,sequence,time\n")
            async for message in connection.recv_all():
                now = datetime.datetime.now(datetime.timezone.utc)
                out.write(f"{now},{message.get('product_id')},{message.get('sequence')},{message.get('time')}\n")

    async def _subscribe_full_and_write_group(self):
        if self.handle == '-':
            await self._subscribe_full_and_write(sys.stdout)
        else:
            with open(self.handle, mode='w') as out:
                await self._subscribe_full_and_write(out)

@dataclass_json
@dataclass
class Config:
    groups: List[Group]
    
    async def run(self):
        await asyncio.gather(
            *[ group._subscribe_full_and_write_group() for group in self.groups ]
        )

def parse_args(args) -> Config:
    parser = argparse.ArgumentParser(description='Subscribe to coinbase full feed and write out.')
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
