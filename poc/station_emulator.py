from datetime import datetime, timezone
import json
import random
import threading
import time
import pika

STATION_ID = "st_101"
COMMAND_QUEUE = f"station.{STATION_ID}.commands"
EVENT_EXCHANGE = "station.events"


def simulate_return(powerbank_serial: str, slot_number: int):
    time.sleep(6)
    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    ch = conn.channel()
    ch.exchange_declare(exchange=EVENT_EXCHANGE, exchange_type="topic")

    return_event = {
        "event_type": "PowerbankReturned",
        "station_id": STATION_ID,
        "slot_number": slot_number,
        "powerbank_serial": powerbank_serial,
        "charge_level": 42,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    ch.basic_publish(
        exchange=EVENT_EXCHANGE,
        routing_key="station.return",
        body=json.dumps(return_event),
    )
    print(
        f"[Станция {STATION_ID}] Пользователь вернул {powerbank_serial} в слот №{slot_number}\n"
    )
    conn.close()


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("localhost")
    )
    channel = connection.channel()

    channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
    channel.exchange_declare(exchange=EVENT_EXCHANGE, exchange_type="topic")

    def callback(ch, method, properties, body):
        cmd = json.loads(body)
        print(f"[Станция {STATION_ID}] Получена команда: {cmd}")
        time.sleep(1.5)

        is_success = random.random() < 0.75
        pb_serial = "PB-99481"
        slot_num = cmd["slot_number"]

        if is_success:
            event = {
                "event_type": "SlotUnlocked",
                "station_id": STATION_ID,
                "slot_number": slot_num,
                "correlation_id": cmd["correlation_id"],
                "powerbank_serial": pb_serial,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            threading.Thread(
                target=simulate_return, args=(pb_serial, slot_num), daemon=True
            ).start()
        else:
            event = {
                "event_type": "SlotUnlockFailed",
                "station_id": STATION_ID,
                "slot_number": slot_num,
                "correlation_id": cmd["correlation_id"],
                "reason": "MECHANICAL_JAM",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        channel.basic_publish(
            exchange=EVENT_EXCHANGE,
            routing_key="station.status",
            body=json.dumps(event),
        )
        print(
            f"[Станция {STATION_ID}] Отправлено событие: {event['event_type']}"
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=COMMAND_QUEUE, on_message_callback=callback)
    print(f"[*] Станция {STATION_ID} запущена и ждет команды...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
