import asyncio
import argparse
import os
import ssl
from concurrent import futures

import ery.core


async def main(unix, nprocs, concurrency, nbytes, duration, use_ssl=False):
    if use_ssl:
        benchdir = os.path.abspath(os.path.dirname(__file__))
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.load_cert_chain(
            os.path.join(benchdir, "cert.pem"), os.path.join(benchdir, "key.pem"),
        )
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        context = None
    service = BenchService()
    server = ery.core.Server(service)
    if not unix:
        server.add_tcp_listener(host="127.0.0.1", port=5556, ssl=context)
        address = "tcp://127.0.0.1:5556"
    else:
        server.add_unix_listener(path="benchsock", ssl=context)
        address = "unix:benchsock"

    await server.start()
    loop = asyncio.get_running_loop()
    with futures.ProcessPoolExecutor(max_workers=nprocs) as executor:
        try:
            clients = [
                loop.run_in_executor(
                    executor,
                    bench_client,
                    address,
                    concurrency,
                    nbytes,
                    duration,
                    use_ssl,
                )
                for _ in range(nprocs)
            ]
            await asyncio.gather(*clients)
            count = service.count
        finally:
            await server.stop()

    print(f"{count / duration} RPS")
    print(f"{duration / count * 1e6} us per request")
    print(f"{nbytes * count / (duration * 1e6)} MB/s")


class BenchService(ery.core.Service):
    def __init__(self):
        self.count = 0

    @ery.core.request
    async def bench(self, data, comm):
        self.count += 1
        await comm.send(b"hi")


def bench_client(address, concurrency, nbytes, duration, use_ssl):
    return asyncio.run(client(address, concurrency, nbytes, duration, use_ssl))


async def client(address, concurrency, nbytes, duration, use_ssl):
    if use_ssl:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        context = None

    running = True

    def stop():
        nonlocal running
        running = False
        for t in tasks:
            t.cancel()

    loop = asyncio.get_running_loop()

    payload = os.urandom(nbytes)

    async def run(client, payload):
        while running:
            await client.request(b"bench", data=payload)

    async with ery.core.Client(address, ssl=context) as client:
        loop.call_later(duration, stop)
        tasks = [
            asyncio.ensure_future(run(client, payload)) for _ in range(concurrency)
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
        "--ssl", action="store_true", help="If set, `ssl` is used",
    )
    parser.add_argument("--uvloop", action="store_true", help="Whether to use uvloop")
    args = parser.parse_args()

    if args.uvloop:
        import uvloop

        uvloop.install()

    print(
        f"processes={args.procs}, concurrency={args.concurrency}, bytes={args.bytes}, "
        f"time={args.seconds}, uvloop={args.uvloop}, unix-sockets={args.unix}, "
        f"ssl={args.ssl}"
    )

    asyncio.run(
        main(
            unix=args.unix,
            nprocs=args.procs,
            concurrency=args.concurrency,
            nbytes=int(args.bytes),
            duration=args.seconds,
            use_ssl=args.ssl,
        )
    )
