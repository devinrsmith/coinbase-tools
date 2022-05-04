from cProfile import run
import aiokafka
import asyncio

async def async_main(args):
    producer = aiokafka.AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()
    try:
        for i in range(100):
            x = await producer.send("my_topic", f"{i}".encode('utf-8'))
            await x
    finally:
        await producer.stop()

def main(args):
    asyncio.run(async_main(args))

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
