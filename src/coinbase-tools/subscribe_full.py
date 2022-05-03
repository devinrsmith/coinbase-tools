#!/usr/bin/env python

import asyncio
from dataclasses import dataclass
import datetime
import json
import sys
from typing import List, Optional
import websockets

COINBASE_WS_FEED = "wss://ws-feed.exchange.coinbase.com"

# max seen size: ~300?
# 1 KiB should be generous
FULL_CHANNEL_MAX_BYTES_PER_MSG = 2 ** 10

@dataclass
class Group:
    handle: str
    product_ids: List[str]
    compression: Optional[str] = "deflate"

    async def _subscribe_full_and_write_group(self):
        if self.handle == '-':
            await _subscribe_full_and_write(sys.stdout, self.product_ids, self.compression)
        else:
            with open(self.handle, mode='w') as out:
                await _subscribe_full_and_write(out, self.product_ids, self.compression)

async def _subscribe_full(ws, product_ids: List[str]):
    json_inner = '","'.join(product_ids)
    subscribe_msg = f'{{"type":"subscribe","channels":[{{"name":"full","product_ids":["{json_inner}"]}}]}}'
    await ws.send(subscribe_msg)

def _parse_message(msg):
    message_json = json.loads(msg)
    product_id = message_json.get('product_id')
    sequence = message_json.get('sequence')
    time = message_json.get('time')
    return (product_id, sequence, time)

async def _write_messages(ws, out):
    out.write("wire_time,product_id,sequence,time\n")
    async for message in ws:
        now = datetime.datetime.now(datetime.timezone.utc)
        product_id, sequence, time = _parse_message(message)
        out.write(f"{now},{product_id},{sequence},{time}\n")

async def _subscribe_full_and_write(out, product_ids : List[str], compression: Optional[str]):
    async with websockets.connect(COINBASE_WS_FEED, compression=compression) as ws:
        await _subscribe_full(ws, product_ids)
        await ws.recv() # skip subscribe response
        await _write_messages(ws, out)

async def _run_all(groups: List[Group]):
    await asyncio.gather(
        *[ group._subscribe_full_and_write_group() for group in groups ]
    )

def __entrypoint__():
    # config = [
    #     Group("/mnt/tmpfs/BTC-USD.csv", ["BTC-USD"]),
    #     Group("/mnt/tmpfs/ETH-USD.csv", ["ETH-USD"]),
    #     Group("/mnt/tmpfs/USDT-USD.csv", ["USDT-USD"]),
    #     Group("/mnt/tmpfs/ADA-USD.csv", ["ADA-USD"]),
    #     Group("/mnt/tmpfs/DOGE-USD.csv", ["DOGE-USD"]),
    #     Group("/mnt/tmpfs/SHIB-USD.csv", ["SHIB-USD"]),
    #     Group("/mnt/tmpfs/SOL-USD.csv", ["SOL-USD"]),
    #     Group("/mnt/tmpfs/EOS-USD.csv", ["EOS-USD"]),
    # ]
    # config = [
    #     Group("-", ["BTC-USD", "ETH-USD", "USDT-USD", "ADA-USD", "DOGE-USD", "SHIB-USD", "SOL-USD", "EOS-USD"])
    # ]
    config = [
        Group("/mnt/tmpfs/BTC-USD.None.csv", ["BTC-USD"], compression=None),
        Group("/mnt/tmpfs/BTC-USD.deflate.csv", ["BTC-USD"], compression="deflate")
    ]
    asyncio.run(_run_all(config))

def __script_entrypoint__():
    return __entrypoint__()

def __main_entrypoint__():
    return __entrypoint__()

if __name__ == '__main__':
    __main_entrypoint__()
