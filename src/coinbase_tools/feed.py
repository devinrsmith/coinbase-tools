import asyncio
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import json
from typing import List, Optional
from dataclasses_json import dataclass_json
import websockets

WS_FEED_URI="wss://ws-feed.exchange.coinbase.com"

# max seen size: ~300?
# 1 KiB should be generous
FULL_CHANNEL_MAX_BYTES_PER_MSG = 2 ** 10

MIN_LINGER = 0.000000001


@dataclass_json
@dataclass
class Config:
    uri: str = WS_FEED_URI
    max_message_size: int = FULL_CHANNEL_MAX_BYTES_PER_MSG
    compression: Optional[str] = "deflate"

    @asynccontextmanager
    async def connect(self, max_memory : int):
        max_queue = max(1, int(max_memory / self.max_message_size))
        async with websockets.connect(
            self.uri,
            max_size=self.max_message_size,
            compression=self.compression,
            max_queue=max_queue) as ws:
            yield Connection(self, ws)

class Connection:
    def __init__(self, config: Config, ws):
        self._config = config
        self._ws = ws
        self._products = None
    
    def queue_length(self):
        return len(self._ws.messages)

    async def subscribe_full(self, product_ids: List[str]):
        if self._products:
            raise Exception("May only subscribe once")
        self._products = product_ids
        json_inner = '","'.join(self._products)
        msg = f'{{"type":"subscribe","channels":[{{"name":"full","product_ids":["{json_inner}"]}}]}}'
        await self._ws.send(msg)
        await self._ws.recv() # skip subscribe response

    async def recv_raw(self):
        return await self._ws.recv()

    async def recv_raw_timeout(self, timeout : float):
        try:
            return await asyncio.wait_for(self._ws.recv(), max(MIN_LINGER, timeout))
        except asyncio.TimeoutError:
            return None
    
    async def recv_raw_all(self):
        async for msg in self._ws:
            yield msg

    async def recv(self) -> dict:
        return json.loads(await self.recv_raw())

    async def recv_timeout(self, timeout : float) -> dict:
        return json.loads(await self.recv_raw_timeout(timeout))

    async def recv_all(self):
        async for msg in self._ws:
            yield json.loads(msg)

async def connect_test(config : Config):
    async with config.connect() as connection:
        print(connection)
        await connection.subscribe_full()
        print(await connection.recv())

def main():
    config = Config(product_ids=["BTC-USD"])
    asyncio.run(connect_test(config))

if __name__ == '__main__':
    main()