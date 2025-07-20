#!/usr/bin/env python3
"""
listener.py – F1 24 PS5 → JSON → RabbitMQ (un solo exchange).
"""
from __future__ import annotations
import asyncio, base64, json, logging, os, struct, time
from collections import defaultdict
import aio_pika
from rich.console import Console
from rich.live import Live
from rich.table import Table

from dotenv import load_dotenv
load_dotenv()     

# ───────── Config ───────────────────────────────────────────────────────────
UDP_HOST = os.getenv("F1_UDP_HOST", "0.0.0.0")
UDP_PORT = int(os.getenv("F1_UDP_PORT", 20777))
AMQP_URL = os.getenv("AMQP_URL", "amqp://f1:f1pass@localhost:5672/")
EXCHANGE_NAME = "f1.telemetry"          # único exchange
ROUTING_FMT = "2024.{packetId}"         # rk = 2024.0, 2024.2, …
HEADER_STRUCT = struct.Struct("<HBBBBBQfIIB")
HEADER_FIELDS = (
    "packetFormat", "gameYear", "gameMajorVersion", "gameMinorVersion",
    "packetVersion", "packetId", "sessionUID", "sessionTime",
    "frameIdentifier", "overallFrameIdentifier", "playerCarIndex",
)
# ────────────────────────────────────────────────────────────────────────────

class Telemetry(asyncio.DatagramProtocol):
    def __init__(self, exchange: aio_pika.Exchange, live: Live):
        self.exch, self.live = exchange, live
        self.stats = defaultdict(int); self._last = time.time()

    def connection_made(self, transport):                # noqa: D401
        logging.info("⏳ Escuchando %s:%s …", UDP_HOST, UDP_PORT)

    def datagram_received(self, data: bytes, _addr):     # noqa: D401
        if len(data) < HEADER_STRUCT.size: return
        header_vals = HEADER_STRUCT.unpack_from(data)
        header = dict(zip(HEADER_FIELDS, header_vals))
        pid = header["packetId"];  self.stats[pid] += 1

        msg = {
            "header": header,
            "payload_b64": base64.b64encode(data[HEADER_STRUCT.size:]).decode(),
        }
        asyncio.create_task(self._publish(json.dumps(msg).encode(),
                                          ROUTING_FMT.format(packetId=pid)))

        if (now := time.time()) - self._last >= .25:
            self._last = now;  self._render()

    async def _publish(self, body, routing_key):
        await self.exch.publish(
            aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=routing_key,
        )

    def _render(self):
        tbl = Table(title="Paquetes recibidos – F1 24", style="cyan", expand=True)
        tbl.add_column("PID", justify="right"); tbl.add_column("Total", justify="right")
        for pid in sorted(self.stats): tbl.add_row(str(pid), str(self.stats[pid]))
        self.live.update(tbl)

async def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s",
                        datefmt="%H:%M:%S")
    conn = await aio_pika.connect_robust(AMQP_URL)
    chan = await conn.channel(publisher_confirms=False)
    exch = await chan.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC,
                                       durable=True)
    console = Console(force_terminal=True)
    with Live(console=console, refresh_per_second=4) as live:
        loop = asyncio.get_running_loop()
        await loop.create_datagram_endpoint(lambda: Telemetry(exch, live),
                                            local_addr=(UDP_HOST, UDP_PORT))
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Listener detenido.")