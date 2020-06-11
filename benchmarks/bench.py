import asyncio
import argparse
import os
import time
from concurrent import futures
from ery.aioprotocol import start_server, new_channel
from ery.protocol import Payload


async def main(address, nprocs, concurrency, nbytes, duration, notify=False):
    outputs = []
    server = await start_server(
        address, lambda channel: handler(channel, outputs, notify)
    )
    loop = asyncio.get_running_loop()
    with futures.ProcessPoolExecutor(max_workers=nprocs) as executor:
        async with server:
            clients = [
                loop.run_in_executor(
                    executor,
                    bench_client,
                    address,
                    concurrency,
                    nbytes,
                    duration,
                    notify,
                )
                for _ in range(nprocs)
            ]
            await asyncio.gather(*clients)
            counts, times = zip(*outputs)
            count = sum(counts)
            duration = sum(times) / nprocs

    print(f"{count / duration} RPS")
    print(f"{duration / count * 1e6} us per request")
    print(f"{nbytes * count / (duration * 1e6)} MB/s")


async def handler(channel, outputs, notify):
    start = time.time()
    count = 0
    try:
        if notify:
            async for req in channel:
                count += 1
        else:
            async for req in channel:
                resp = Payload(req.id, body=b"hi")
                await channel.send(resp)
                count += 1
    except OSError:
        pass
    finally:
        duration = time.time() - start
        outputs.append((count, duration))


def bench_client(address, concurrency, nbytes, duration, notify):
    return asyncio.run(client(address, concurrency, nbytes, duration, notify))


async def client(address, concurrency, nbytes, duration, notify):
    running = True

    def stop():
        nonlocal running
        running = False
        for t in tasks:
            t.cancel()

    loop = asyncio.get_running_loop()

    payload = os.urandom(nbytes)

    async def run(channel, payload):
        meth = channel.notify if notify else channel.request
        while running:
            await meth(b"hello", body=payload)

    async with await new_channel(address) as channel:
        loop.call_later(duration, stop)
        tasks = [
            asyncio.ensure_future(run(channel, payload)) for _ in range(concurrency)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark channels")
    parser.add_argument(
        "--procs", "-p", default=1, type=int, help="Number of processes"
    )
    parser.add_argument(
        "--concurrency",
        "-c",
        default=1,
        type=int,
        help="Number of concurrent calls per proc",
    )
    parser.add_argument(
        "--bytes", "-b", default=10, type=float, help="payload size in bytes"
    )
    parser.add_argument(
        "--seconds", "-s", default=5, type=int, help="bench duration in secs"
    )
    parser.add_argument(
        "--unix", action="store_true", help="Whether to use unix domain sockets"
    )
    parser.add_argument(
        "--notify",
        action="store_true",
        help="If set, `notify` is used instead of `request`",
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
        f"processes={args.procs}, concurrency={args.concurrency}, bytes={args.bytes}, "
        f"time={args.seconds}, uvloop={args.uvloop}, unix-sockets={args.unix}"
    )

    asyncio.run(
        main(
            address,
            nprocs=args.procs,
            concurrency=args.concurrency,
            nbytes=int(args.bytes),
            duration=args.seconds,
            notify=args.notify,
        )
    )
