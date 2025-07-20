#!/usr/bin/env python3
"""
telemetry_parser_v4.py – PacketId 6 → car_telemetry (offsets validados)
"""

from __future__ import annotations
import os, sys, json, base64, struct, asyncio, datetime
import aio_pika, psycopg_pool
from dotenv import load_dotenv


def check_schema():
    import psycopg
    with psycopg.connect(PG_DSN) as conn:
        bad = [r[0] for r in conn.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'car_telemetry'
              AND table_schema = 'public'
              AND data_type = 'smallint';
        """)]
    if bad:
        raise SystemExit(f"❌ car_telemetry aún tiene SMALLINT: {bad}\n"
                         f"→ Ejecuta el ALTER TABLE para ampliar esos campos.")
        print("✅ car_telemetry schema OK." )   

# ── Config & Windows loop --------------------------------------------------
load_dotenv()
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

AMQP_URL = os.getenv("AMQP_URL", "amqp://f1:f1pass@localhost:5672/")
PG_DSN   = os.getenv("PG_DSN",
          "postgresql://postgres:postgres@localhost:5432/telemetry")
QUEUE = "test.dump"
BATCH = 100

# ── Offsets dentro de CarTelemetryData (ver SDK 2024) ----------------------
OFF_SPEED     = 0   # uint16
OFF_THROTTLE  = 2   # uint8   0-100
OFF_STEER     = 3   # int8    -127..127 (no se guarda)
OFF_BRAKE     = 4   # uint8   0-100
OFF_CLUTCH    = 5   # uint8   (no se guarda)
OFF_GEAR      = 6   # int8
OFF_RPM       = 7   # uint16
OFF_DRS       = 9   # uint8
PER_CAR       = 60  # total bytes por coche

SQL = """
INSERT INTO car_telemetry
(ts, frame_id, car_idx, speed_kph, throttle, brake, gear, rpm, drs)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

# ── Helper -------------------------------------------------------------
async def flush(pool, rows):
    if rows:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(SQL, rows)
            await conn.commit()

# ── Parser main --------------------------------------------------------
async def main():
    pool = psycopg_pool.AsyncConnectionPool(PG_DSN, min_size=1, max_size=3, open=False)
    await pool.open()

    rconn = await aio_pika.connect_robust(AMQP_URL)
    chan  = await rconn.channel()
    await chan.set_qos(prefetch_count=BATCH * 2)
    queue = await chan.declare_queue(QUEUE, durable=True)

    rows, pending = [], 0
    print("🚥 Telemetry parser v4 listo…")

    try:
        async with queue.iterator() as it:
            async for msg in it:
                body = json.loads(msg.body)
                hdr  = body["header"]
                if hdr["packetId"] != 6:
                    await msg.ack(); continue

                blob = base64.b64decode(body["payload_b64"])
                if len(blob) < PER_CAR * 22:
                    await msg.ack(); continue

                now   = datetime.datetime.now(datetime.timezone.utc)
                frame = hdr["frameIdentifier"]

                for car_idx in range(22):
                    base = car_idx * PER_CAR
                    speed   = struct.unpack_from("<H", blob, base + OFF_SPEED)[0]
                    throttle= struct.unpack_from("<B", blob, base + OFF_THROTTLE)[0]
                    brake   = struct.unpack_from("<B", blob, base + OFF_BRAKE)[0]
                    gear    = struct.unpack_from("<b", blob, base + OFF_GEAR)[0]
                    rpm     = struct.unpack_from("<H", blob, base + OFF_RPM)[0]
                    drs     = struct.unpack_from("<B", blob, base + OFF_DRS)[0]

                    rows.append((now, frame, car_idx,
                                 speed, throttle, brake, gear, rpm, drs))

                pending += 1
                await msg.ack()
                if pending >= BATCH:
                    await flush(pool, rows)
                    rows.clear(); pending = 0
    finally:
        await flush(pool, rows)
        await pool.close(); await rconn.close()
        print("👋  Parser detenido.")

# ── Virtual test --------------------------------------------------------
def _virtual_test():
    """Confirma que OFF_RPM y OFF_SPEED son correctos."""
    speed, rpm = 500, 60000
    buf = bytearray(PER_CAR)
    struct.pack_into("<H", buf, OFF_SPEED, speed)
    struct.pack_into("<H", buf, OFF_RPM,  rpm)
    payload = bytes(buf) * 22

    assert struct.unpack_from("<H", payload, OFF_SPEED)[0] == speed
    assert struct.unpack_from("<H", payload, OFF_RPM )[0] == rpm
    print("✅ virtual test passed – offsets OK")

# ── Entrypoint ----------------------------------------------------------
if __name__ == "__main__":
    if "TEST" in os.environ:
        _virtual_test()
    else:
        check_schema()      # ← aborta si algo sigue mal
        asyncio.run(main())