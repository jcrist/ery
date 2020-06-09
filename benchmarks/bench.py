import asyncio
import argparse
import os
from concurrent import futures
from ery.aioprotocol import start_server, new_channel
from ery.protocol import Payload


async def main(address, nprocs, concurrency, nbytes, duration):
    server = await start_server(address, handler)
    loop = asyncio.get_running_loop()
    with futures.ProcessPoolExecutor(max_workers=nprocs) as executor:
        async with server:
            clients = [
                loop.run_in_executor(
                    executor, bench_client, address, concurrency, nbytes, duration,
                )
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
    try:
        async for req in channel:
            resp = Payload(req.id, frames=(b"hi",))
            await channel.send(resp)
    except OSError:
        pass


def bench_client(address, concurrency, nbytes, duration):
    return asyncio.run(client(address, concurrency, nbytes, duration))


async def client(address, concurrency, nbytes, duration):
    running = True

    def stop():
        nonlocal running
        running = False
        for t in tasks:
            t.cancel()

    loop = asyncio.get_running_loop()

    payload = os.urandom(nbytes)

    async def run(channel, payload):
        nonlocal count
        while running:
            await channel.request(b"hello", frames=[payload])
            count += 1

    async with await new_channel(address) as channel:
        loop.call_later(duration, stop)
        count = 0
        start = loop.time()
        tasks = [
            asyncio.ensure_future(run(channel, payload)) for _ in range(concurrency)
        ]
        res = await asyncio.gather(*tasks, return_exceptions=True)
        stop = loop.time()
        time = stop - start

    return count, time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark channels")
    parser.add_argument("--nprocs", default=1, type=int, help="Number of processes")
    parser.add_argument(
        "--concurrency",
        "-c",
        default=1,
        type=int,
        help="Number of concurrent calls per proc",
    )
    parser.add_argument(
        "--nbytes", default=10, type=float, help="payload size in bytes"
    )
    parser.add_argument(
        "--duration", default=10, type=int, help="bench duration in secs"
    )
    parser.add_argument(
        "--unix", action="store_true", help="Whether to use unix domain sockets"
    )
    parser.add_argument("--uvloop", action="store_true", help="Whether to use uvloop")
    args = parser.parse_args()

    if args.uvloop:
        import uvloop

        uvloop.install()
    if args.unix:
        address = "benchsock"
    else:
        address = ("127.0.0.1", 5556)

    print(
        f"Benchmarking: nprocs={args.nprocs}, concurrency={args.concurrency}, nbytes={args.nbytes}, "
        f"duration={args.duration}, uvloop={args.uvloop}, unix-sockets={args.unix}"
    )

    asyncio.run(
        main(
            address,
            nprocs=args.nprocs,
            concurrency=args.concurrency,
            nbytes=int(args.nbytes),
            duration=args.duration,
        )
    )
