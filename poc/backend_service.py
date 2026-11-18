from datetime import datetime, timezone
import json
import time
import uuid
import pika

STATION_ID = "st_101"
COMMAND_QUEUE = f"station.{STATION_ID}.commands"
EVENT_EXCHANGE = "station.events"


def run_rental_flow():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("localhost")
    )
    channel = connection.channel()

    channel.queue_declare(queue=COMMAND_QUEUE, durable=True)
    channel.exchange_declare(exchange=EVENT_EXCHANGE, exchange_type="topic")

    result = channel.queue_declare(queue="", exclusive=True)
    callback_queue = result.method.queue
    channel.queue_bind(
        exchange=EVENT_EXCHANGE, queue=callback_queue, routing_key="station.#"
    )

    rental_id = str(uuid.uuid4())
    command = {
        "message_id": str(uuid.uuid4()),
        "correlation_id": rental_id,
        "command": "UNLOCK_SLOT",
        "slot_number": 3,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    print(
        f"[Бэкенд] Холд 500 руб. выполнен. Отправка команды для аренды {rental_id}..."
    )
    channel.basic_publish(
        exchange="", routing_key=COMMAND_QUEUE, body=json.dumps(command)
    )

    start_wait = time.time()
    rental_active = False

    while True:
        method_frame, header_frame, body = channel.basic_get(
            queue=callback_queue, auto_ack=True
        )

        if body:
            event = json.loads(body)
            if (
                not rental_active
                and event.get("correlation_id") == rental_id
            ):
                if event["event_type"] == "SlotUnlocked":
                    rental_active = True
                    print(
                        f"[Бэкенд] УСПЕХ: Слот открыт. Аренда {rental_id} переведена в ACTIVE."
                    )
                elif event["event_type"] == "SlotUnlockFailed":
                    print(
                        f"[Бэкенд] ОШИБКА: {event['reason']}. Аренда FAILED, холд отменен."
                    )
                    break

            elif rental_active and event["event_type"] == "PowerbankReturned":
                print(
                    f"[Бэкенд] ВОЗВРАТ: Пауэрбанк {event['powerbank_serial']} принят."
                )
                print(
                    f"[Бэкенд] Аренда {rental_id} COMPLETED. Списано 100 руб., остаток 400 руб. разблокирован."
                )
                break

        if not rental_active and (time.time() - start_wait > 5.0):
            print(
                "[Бэкенд] ТАЙМАУТ: Станция не ответила за 5 секунд. Статус станции -> OFFLINE, холд отменен."
            )
            break

        time.sleep(0.2)

    connection.close()


if __name__ == "__main__":
    run_rental_flow()
