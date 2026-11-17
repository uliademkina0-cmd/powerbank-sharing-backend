# powerbank-sharing-backend
System architecture, API contracts, and Python PoC for smart powerbank sharing
# Умный шеринг пауэрбанков (System Design & IoT Backend PoC)

Проект демонстрирует проектирование бэкенд-архитектуры для сервиса краткосрочной аренды пауэрбанков с оплатой поминутно или по подписке, интеграцией с платежным шлюзом и асинхронным управлением физическими станциями через брокер сообщений (RabbitMQ).

## Архитектурные артефакты
* **Модель данных (ER-диаграмма):** Учет пользователей, подписок, станций, состояния ячеек, сессий аренды и финансовых транзакций.
* **Бизнес-процессы (UML Activity / Flowchart):** Сценарии взятия и возврата устройства, включая обработку краевых случаев (таймаут станции, заклинивание замка, просрочка возврата).
* **Взаимодействие сервисов (UML Sequence):** Асинхронная разблокировка слота через RabbitMQ без блокировки HTTP-запроса мобильного клиента.
* **Контракты API:** Спецификации [OpenAPI (REST)](docs/openapi.yaml) и [AsyncAPI (Broker Events)](docs/asyncapi.yaml).
* **Proof of Concept (PoC):** Эмулятор IoT-станции и бэкенд-сервиса на Python (`pika`) в папке [`poc/`](poc/).

---

## 1. Модель данных (ER-диаграмма)

```mermaid
erDiagram
    USERS ||--o{ SUBSCRIPTIONS : "оформляет"
    USERS ||--o{ RENTALS : "совершает"
    USERS ||--o{ TRANSACTIONS : "оплачивает"
    
    STATIONS ||--|{ SLOTS : "содержит"
    STATIONS ||--o{ RENTALS : "старт аренды"
    STATIONS ||--o{ RENTALS : "возврат аренды"
    
    SLOTS |o--o| POWERBANKS : "хранит"
    POWERBANKS ||--o{ RENTALS : "используется в"
    
    RENTALS ||--o{ TRANSACTIONS : "порождает"

    USERS {
        uuid id PK
        varchar phone UK "Номер телефона"
        varchar status "active, blocked"
        timestamp created_at
    }

    SUBSCRIPTIONS {
        uuid id PK
        uuid user_id FK
        varchar plan_type "daily, monthly"
        timestamp valid_until "Действует до"
        boolean is_active
    }

    STATIONS {
        uuid id PK
        varchar serial_number UK
        decimal latitude
        decimal longitude
        varchar status "online, offline, maintenance"
        int total_slots
    }

    SLOTS {
        uuid id PK
        uuid station_id FK
        uuid powerbank_id FK "NULL, если слот пуст"
        int slot_number "Порядковый номер ячейки"
        varchar status "empty, occupied, jammed, error"
    }

    POWERBANKS {
        uuid id PK
        varchar serial_number UK
        int charge_level "Процент заряда 0-100"
        varchar status "available, rented, broken"
    }

    RENTALS {
        uuid id PK
        uuid user_id FK
        uuid powerbank_id FK
        uuid start_station_id FK
        uuid end_station_id FK "NULL, пока идет аренда"
        timestamp started_at
        timestamp ended_at "NULL, пока идет аренда"
        varchar status "pending_unlock, active, completed, failed, overdue"
    }

    TRANSACTIONS {
        uuid id PK
        uuid user_id FK
        uuid rental_id FK "NULL, если оплата подписки"
        decimal amount
        varchar type "hold, charge, refund, penalty, subscription"
        varchar status "pending, success, failed"
        varchar external_payment_id "ID в платежном шлюзе"
        timestamp created_at
    }
```

---

## 2. Процесс взятия и возврата устройства (Обработка ошибок)

```mermaid
flowchart TD
    Start([Начало: Пользователь сканирует QR станции]) --> CheckSub{Есть активная подписка?}
    
    CheckSub -- Да --> FindSlot[Поиск заряженного слота >= 80%]
    CheckSub -- Нет --> HoldMoney[Холд депозита 500 руб. через платежный шлюз]
    
    HoldMoney --> HoldSuccess{Холд успешен?}
    HoldSuccess -- Нет --> ErrorPay([Конец: Ошибка оплаты])
    HoldSuccess -- Да --> FindSlot
    
    FindSlot --> SendCmd[Отправка команды UNLOCK_SLOT в брокер]
    SendCmd --> WaitIoT{Ответ станции за 5 сек?}
    
    WaitIoT -- Таймаут --> StationOffline[Пометить станцию OFFLINE]
    StationOffline --> Refund1[Отмена холда]
    Refund1 --> EndFail([Конец: Станция недоступна])
    
    WaitIoT -- Ошибка замка JAMMED --> MarkJammed[Пометить слот JAMMED]
    MarkJammed --> HasOtherSlot{Есть другой слот?}
    HasOtherSlot -- Да --> FindSlot
    HasOtherSlot -- Нет --> Refund1
    
    WaitIoT -- Успех UNLOCKED --> ActiveRent[Статус аренды: ACTIVE. Идет таймер]
    
    ActiveRent --> ReturnCheck{Возврат в течение 24 часов?}
    ReturnCheck -- Нет --> Overdue[Статус: OVERDUE. Списание штрафа 3000 руб.]
    Overdue --> EndOverdue([Конец: Устройство выкуплено])
    
    ReturnCheck -- Да --> InsertPB[Пользователь вставил пауэрбанк в слот]
    InsertPB --> CalcPrice[Расчет стоимости по минутам / подписке]
    CalcPrice --> ChargeFinal[Списание итога и возврат остатка холда]
    ChargeFinal --> EndSuccess([Конец: Аренда завершена])
```

---

## 3. Асинхронная разблокировка слота (Sequence Diagram)

```mermaid
sequenceDiagram
    autonumber
    actor User as Мобильное приложение
    participant API as Backend API
    participant PG as Платежный шлюз
    participant MQ as RabbitMQ (Брокер)
    participant IoT as IoT-станция

    User->>API: POST /api/v1/rentals (station_id)
    API->>PG: POST /payments/hold (amount: 500)
    PG-->>API: 200 OK (hold_id)
    
    API->>MQ: Publish UnlockSlotCommand (queue: station.101.commands)
    API-->>User: 202 Accepted (rental_id, status: pending_unlock)
    
    Note over User,API: Приложение ждет Push или опрашивает GET /rentals/{id}
    
    MQ->>IoT: Consume UnlockSlotCommand
    IoT->>IoT: Подача напряжения на соленоид замка
    
    alt Замок открылся успешно
        IoT->>MQ: Publish SlotUnlockedEvent (topic: station.events)
        MQ->>API: Consume SlotUnlockedEvent
        API->>API: Обновление статуса аренды -> ACTIVE
        API-->>User: Push: "Заберите пауэрбанк из слота №3"
    else Замок заклинило
        IoT->>MQ: Publish SlotUnlockFailedEvent (reason: JAMMED)
        MQ->>API: Consume SlotUnlockFailedEvent
        API->>PG: POST /payments/void (отмена холда)
        API->>API: Обновление статуса аренды -> FAILED
        API-->>User: Push: "Не удалось открыть ячейку, средства разблокированы"
    end
```

---

## 4. Локальный запуск PoC (Python + RabbitMQ)

1. Перейдите в директорию с кодом и поднимите брокер сообщений:
   ```bash
   cd poc
   docker-compose up -d
   ```
2. Установите зависимости:
   ```bash
   pip install -r requirements.txt
   ```
3. Запустите эмулятор станции в первом терминале:
   ```bash
   python station_emulator.py
   ```
4. Запустите сценарий аренды во втором терминале:
   ```bash
   python backend_service.py
   ```
