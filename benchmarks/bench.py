import asyncio
import argparse
import os
from concurrent import futures
from ery.aioprotocol import start_server, new_channel
from ery.protocol import Request, Payload


async def main(address, nprocs, nbytes, duration):
    server = await start_server(address, handler)
    loop = asyncio.get_running_loop()
    with futures.ProcessPoolExecutor(max_workers=nprocs) as executor:
        async with server:
            clients = [
                loop.run_in_executor(executor, bench_client, address, nbytes, duration,)
                for _ in range(nprocs)
            ]
            res = await asyncio.gather(*clients)
            counts, times = zip(*res)
            count = sum(counts)
            duration = sum(times) / nprocs

    print(f"{count / duration} RPS")
    print(f"{duration / count * 1e6} us per request")
    print(f"{nbytes * count / (duration * 1e6)} MB/s")


async def handler(channel):
    async for req in channel:
        resp = Payload(req.id, frames=(b"hi",))
        await channel.send(resp)


def bench_client(address, nbytes, duration):
    return asyncio.run(client(address, nbytes, duration))


async def client(address, nbytes, duration):
    running = True

    def stop():
        nonlocal running
        running = False

    loop = asyncio.get_running_loop()
    loop.call_later(duration, stop)

    payload = os.urandom(nbytes)
    req = Request(1234, b"hello", frames=[payload])

    async with await new_channel(address) as channel:
        count = 0
        start = loop.time()
        while running:
            await channel.send(req)
            await channel.recv()
            count += 1
        stop = loop.time()
        time = stop - start

    return count, time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark channels")
    parser.add_argument("--nprocs", default=1, type=int, help="Number of processes")
    parser.add_argument(
        "--nbytes", default=10, type=float, help="payload size in bytes"
    )
    parser.add_argument(
        "--duration", default=10, type=int, help="bench duration in secs"
    )
    parser.add_argument("--unix", action="store_true", help="Whether to use unix domain sockets")
    parser.add_argument("--uvloop", action="store_true", help="Whether to use uvloop")
    args = parser.parse_args()

    if args.uvloop:
        import uvloop

        uvloop.install()
    if args.unix:
        address = 'benchsock'
    else:
        address = ("127.0.0.1", 5556)

    print(
        f"Benchmarking: nprocs={args.nprocs}, nbytes={args.nbytes}, duration={args.duration}, "
        f"uvloop={args.uvloop}, unix-sockets={args.unix}"
    )

    asyncio.run(
        main(address, nprocs=args.nprocs, nbytes=int(args.nbytes), duration=args.duration,)
    )
