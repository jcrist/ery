import asyncio
import argparse
import os
import time
import ssl
from concurrent import futures

import ery
from ery.protocol import Payload


async def main(
    address, nprocs, concurrency, nbytes, duration, notify=False, use_ssl=False
):
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
    outputs = []
    server = ery.Server(lambda conn: handler(conn, outputs, notify))
    if isinstance(address, tuple):
        host, port = address
        server.add_tcp_listener(host=host, port=port, ssl=context)
    else:
        server.add_unix_listener(path=address, ssl=context)

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
                    notify,
                    use_ssl,
                )
                for _ in range(nprocs)
            ]
            await asyncio.gather(*clients)
            counts, times = zip(*outputs)
            count = sum(counts)
            duration = sum(times) / nprocs
        finally:
            await server.stop()

    print(f"{count / duration} RPS")
    print(f"{duration / count * 1e6} us per request")
    print(f"{nbytes * count / (duration * 1e6)} MB/s")


async def handler(conn, outputs, notify):
    start = time.time()
    count = 0
    try:
        if notify:
            async for req in conn:
                count += 1
        else:
            async for req in conn:
                resp = Payload(req.id, body=b"hi")
                await conn.send(resp)
                count += 1
    except OSError:
        pass
    finally:
        duration = time.time() - start
        outputs.append((count, duration))


def bench_client(address, concurrency, nbytes, duration, notify, use_ssl):
    return asyncio.run(client(address, concurrency, nbytes, duration, notify, use_ssl))


async def client(address, concurrency, nbytes, duration, notify, use_ssl):
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

    async def run(conn, payload):
        meth = conn.notify if notify else conn.request
        while running:
            await meth(b"hello", body=payload)

    async with await ery.connect(address, ssl=context) as conn:
        loop.call_later(duration, stop)
        tasks = [asyncio.ensure_future(run(conn, payload)) for _ in range(concurrency)]
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
    parser.add_argument(
        "--ssl", action="store_true", help="If set, `ssl` is used",
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
        f"time={args.seconds}, uvloop={args.uvloop}, unix-sockets={args.unix}, "
        f"ssl={args.ssl}, notify={args.notify}"
    )

    asyncio.run(
        main(
            address,
            nprocs=args.procs,
            concurrency=args.concurrency,
            nbytes=int(args.bytes),
            duration=args.seconds,
            notify=args.notify,
            use_ssl=args.ssl,
        )
    )
