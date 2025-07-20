#!/usr/bin/env python3
"""
consumer_db.py – RabbitMQ → PostgreSQL (batch insert, Json adapter corregido)
"""

from __future__ import annotations
import os, sys, json, time, asyncio, aio_pika, psycopg_pool
from psycopg.types.json import Json            # ← import correcto
from dotenv import load_dotenv

# ── Config + compatibilidad Windows ─────────────────────────────────────────
load_dotenv()
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

AMQP_URL = os.getenv("AMQP_URL", "amqp://f1:f1pass@localhost:5672/")
PG_DSN   = os.getenv(
    "PG_DSN",
    "postgresql://postgres:postgres@localhost:5432/telemetry_database",
)
QUEUE     = os.getenv("QUEUE", "test.dump")
BATCH_SZ  = int(os.getenv("BATCH_SIZE", 1000))
BATCH_MS  = int(os.getenv("BATCH_MS", 500))
# ────────────────────────────────────────────────────────────────────────────

SQL = """
INSERT INTO raw_packets (packet_id, session_uid, frame_id, json_body)
VALUES (%s, %s, %s, %s)
"""

async def main() -> None:
    # 1️⃣ pool
    pool = psycopg_pool.AsyncConnectionPool(
        PG_DSN, min_size=1, max_size=3, open=False
    )
    await pool.open()

    async def insert_batch(rows: list[tuple]) -> None:
        if not rows:
            return
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(SQL, rows)
            await conn.commit()

    # 2️⃣ RabbitMQ
    rconn = await aio_pika.connect_robust(AMQP_URL)
    chan  = await rconn.channel()
    await chan.set_qos(prefetch_count=BATCH_SZ * 2)
    queue = await chan.declare_queue(QUEUE, durable=True)

    print("✅ Consumer conectado – esperando mensajes…")
    rows: list[tuple] = []
    last_flush = time.time()

    async with queue.iterator() as q_iter:
        async for msg in q_iter:
            async with msg.process():
                body = json.loads(msg.body)
                h = body["header"]
                rows.append((
                    h["packetId"],
                    h["sessionUID"],
                    h["frameIdentifier"],
                    Json(body),           # adapta dict → jsonb
                ))

            now = time.time()
            if len(rows) >= BATCH_SZ or (now - last_flush)*1000 >= BATCH_MS:
                await insert_batch(rows)
                rows.clear()
                last_flush = now

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋  Consumer detenido.")
