import asyncio
import json
import logging
import websockets
import os

from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


def setup_logger(name: str, level: int = logging.DEBUG) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)

    log = logging.getLogger(name)
    log.setLevel(level)

    if not log.handlers:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(stream)s] %(message)s", "%Y-%m-%d %H:%M:%S"
        )

        ch = logging.StreamHandler()

        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        log.addHandler(ch)

        fh = TimedRotatingFileHandler(
            filename=f"logs/{name}.log",
            when="H",
            interval=3,
            utc=True,
            backupCount=int((24 * 14) / 3),
        )

        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        fh.suffix = "%Y-%m-%d_%H"

        log.addHandler(fh)

    return log


async def listen_ws(url: str, streams: list[str], name: str) -> None:
    log = logging.LoggerAdapter(logger, {"stream": name})

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=40) as ws:
                log.info("Connected")

                subscribe_message = {"method": "SUBSCRIBE", "params": streams, "id": 1}
                await ws.send(json.dumps(subscribe_message))

                log.info(f"Subscribed to streams: {streams}")

                async for message in ws:
                    try:
                        data = json.loads(message)
                        payload = data.get("data")

                        log.debug(f"Payload: {payload}")
                    except json.JSONDecodeError:
                        log.warning("Received non-JSON message, ignoring.")

        except websockets.ConnectionClosed:
            log.warning("Connection closed, reconnecting...")

        except Exception as e:
            log.error(f"Unexpected error: {e}, reconnecting...")

        await asyncio.sleep(3)


url1 = "wss://stream.binance.com:9443/stream"
url2 = "wss://fstream.binance.com/stream"

stream1 = ["btcusdt@aggTrade"]
stream2 = ["btcusdt@aggTrade"]


async def main():
    await asyncio.gather(
        listen_ws(url1, stream1, "spot"), listen_ws(url2, stream2, "perp")
    )


if __name__ == "__main__":
    logger = setup_logger("market_data")

    asyncio.run(main())
