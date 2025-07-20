#!/usr/bin/env python3
"""
listener.py – Captura UDP de F1 24 (PS5) y publica en RabbitMQ.

• Escucha 0.0.0.0:20777 (configurable por env).
• Muestra una tabla en vivo con el nº de paquetes por packetId.
• Si RabbitMQ está accesible, reenvía cada datagrama al exchange
    indicado con routing-key basada en el packetId.
"""
from __future__ import annotations

import asyncio
import logging
import os
import struct
import time
from collections import defaultdict
from typing import Any
from rich.console import Console

import aio_pika
from rich.live import Live
from rich.table import Table

# ---------- Parámetros de entorno ----------
UDP_HOST = os.getenv("F1_UDP_HOST", "0.0.0.0")
UDP_PORT = int(os.getenv("F1_UDP_PORT", 20777))

AMQP_URL = os.getenv("AMQP_URL", "amqp://f1:f1pass@localhost/")
AMQP_EXCHANGE = os.getenv("AMQP_EXCHANGE", "f1.telemetry")
# -------------------------------------------

# Cabecera oficial 2024: 24 bytes little-endian
HEADER_STRUCT = struct.Struct("<HBBBBBQfIBB")
PACKET_ID_OFFSET = 5  # posición del m_packetId dentro del unpack


class TelemetryProtocol(asyncio.DatagramProtocol):
    """DatagramProtocol asíncrono que contabiliza y publica los paquetes."""

    def __init__(self, amqp_exchange: aio_pika.Exchange | None, live: Live) -> None:
        self.exchange = amqp_exchange
        self.live = live
        self.stats: dict[int, int] = defaultdict(int)
        self._last_redraw: float = time.time()

    # -- callbacks UDP ---------------------------------------------------------

    def connection_made(self, transport: asyncio.BaseTransport) -> None:  # noqa: D401
        self.transport = transport  # type: ignore[assignment]
        logging.info("⏳ Esperando datos en %s:%s …", UDP_HOST, UDP_PORT)

    def datagram_received(self, data: bytes, addr: Any) -> None:  # noqa: D401
        if len(data) < HEADER_STRUCT.size:
            logging.warning("Datagrama demasiado corto de %s – ignorado", addr)
            return

        header = HEADER_STRUCT.unpack_from(data)
        packet_id = header[PACKET_ID_OFFSET]
        self.stats[packet_id] += 1

        # Publica a RabbitMQ (fire-and-forget)
        if self.exchange:
            asyncio.create_task(self._publish(data, packet_id))

        # Refresca la tabla como máximo 4 veces por segundo
        now = time.time()
        if now - self._last_redraw >= 0.25:
            self._last_redraw = now
            self._render_table()

    # -- RabbitMQ --------------------------------------------------------------

    async def _publish(self, payload: bytes, packet_id: int) -> None:
        routing_key = f"f1.2024.{packet_id}"
        try:
            await self.exchange.publish(  # type: ignore[union-attr]
                aio_pika.Message(
                    body=payload,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key=routing_key,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("❌ Error al publicar en RabbitMQ: %s", exc)

    # -- UI --------------------------------------------------------------------

    def _render_table(self) -> None:
        table = Table(title="Paquetes recibidos – F1 24", style="bold cyan", expand=True)
        table.add_column("Packet ID", justify="right")
        table.add_column("Total", justify="right")

        for pid in sorted(self.stats):
            table.add_row(str(pid), str(self.stats[pid]))

        self.live.update(table)


# -----------------------------------------------------------------------------


async def connect_rabbitmq() -> aio_pika.Exchange | None:
    """Intenta conexión “robusta” a RabbitMQ; si falla, devuelve None."""
    try:
        connection = await aio_pika.connect_robust(AMQP_URL)
        channel = await connection.channel()
        return await channel.declare_exchange(
            AMQP_EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True
        )
    except Exception as exc:  # pylint: disable=broad-except
        logging.warning("RabbitMQ no disponible (%s) – continuo sin publicar", exc)
        return None


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    exchange = await connect_rabbitmq()

    loop = asyncio.get_running_loop()
    console = Console(force_terminal=True)
    with Live(console=console, refresh_per_second=4) as live:
        await loop.create_datagram_endpoint(
            lambda: TelemetryProtocol(exchange, live),
            local_addr=(UDP_HOST, UDP_PORT),
        )
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋  Listener detenido por el usuario.")
