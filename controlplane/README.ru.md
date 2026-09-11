# OpenFlux Control Plane

[English](README.md) | **Русский**

Полное описание HTTP API: [API.ru.md](API.ru.md) ([English](API.md)).

Многопользовательское управление ключами/токенами, учёт трафика и координация exit-нод для
OpenFlux. Это отдельный Go-модуль и отдельно разворачиваемый сервис — он не передаёт и не
ретранслирует туннелируемый трафик сам; тот по-прежнему идёт напрямую между клиентом и exit-нодой
через транспорт (Yandex Docs и т.д.), как и раньше. Control plane обрабатывает только служебный
трафик: выдачу/проверку ключей, пакетные отчёты об использовании трафика и координацию нод.

## Конфигурация (переменные окружения)

| Переменная                         | Обязательна | Описание                                                              |
|------------------------------------|-------------|------------------------------------------------------------------------|
| `CONTROLPLANE_DATABASE_URL`        | да          | Строка подключения к Postgres (`postgres://user:pass@host:5432/db`)   |
| `CONTROLPLANE_TOKEN_PEPPER`        | да          | Секрет, добавляемый перед хешированием каждого токена; при смене все существующие токены становятся недействительны |
| `CONTROLPLANE_ADMIN_TOKEN`         | да          | Начальный bearer-токен для эндпоинтов `/v1/admin/*`                   |
| `CONTROLPLANE_LISTEN_ADDR`         | нет         | По умолчанию `:8080`                                                   |
| `CONTROLPLANE_RATE_LIMIT_RPS`      | нет         | Запросов/сек с одного IP на `/v1/resolve`, по умолчанию `1`            |

Миграции в `internal/db/migrations` применяются автоматически при старте (отслеживаются в таблице
`schema_migrations`, повторный запуск безопасен).

## Сборка и запуск

```bash
go build -o controlplane ./cmd/controlplane
CONTROLPLANE_DATABASE_URL="postgres://openflux:secret@localhost:5432/openflux?sslmode=disable" \
CONTROLPLANE_TOKEN_PEPPER="$(openssl rand -hex 32)" \
CONTROLPLANE_ADMIN_TOKEN="$(openssl rand -hex 32)" \
./controlplane
```

## Панель управления

Самодостаточный веб-интерфейс (без сборки, без внешних ресурсов — `internal/api/web/admin.html`,
встроен прямо в бинарник) отдаётся по адресу `/admin/` на русском языке. Это тонкий клиент поверх
того же JSON API `/v1/admin/*`, что описан ниже: один раз вставляете `CONTROLPLANE_ADMIN_TOKEN`
(хранится в `localStorage` браузера), дальше вкладка «Ключи» — это создание/фильтр/включение/
отключение/удаление ключей, а вкладка «Настройки» — это единственная нода, зарегистрированная на
этом сервере (статус, активность, обновление токена), плюс управление ingest-токенами.
`deploy/install.sh` ставит перед ним Nginx с сертификатом Let's Encrypt.

## Понятия

- **Node (нода)** — один процесс/машина exit-ноды. Имеет свой bearer-токен, опрашивает назначенные
  ей ключи и отправляет отчёты об использовании.
- **Key (ключ)** — токен доступа одного конечного пользователя. Привязан к `transport` + `doc_url`
  (например, конкретной ссылке на Yandex Docs), может иметь `traffic_limit_bytes` и
  включаться/отключаться. Создаётся либо напрямую администратором, либо пакетно через
  ingest-токен.
- **Ingest token (ingest-токен)** — ограниченный по правам токен, выдаваемый стороннему
  приложению/скрипту, которое генерирует ключи (и их Yandex Docs) и регистрирует их здесь.

Новые ключи по возможности автоматически назначаются активной ноде с наибольшим свободным
резервом (`max_keys`), поэтому масштабирование — это регистрация новых нод, а не рост одного
процесса.

## Обход API

```bash
BASE=http://localhost:8080
ADMIN=$CONTROLPLANE_ADMIN_TOKEN

# 1. Регистрируем exit-ноду, получаем её токен
NODE=$(curl -s -X POST $BASE/v1/admin/nodes \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"name":"node-eu-1","max_keys":300}')
echo "$NODE"
NODE_TOKEN=$(echo "$NODE" | jq -r .token)
NODE_ID=$(echo "$NODE" | jq -r .id)

# 2. Создаём ingest-токен для стороннего скрипта генерации ключей
INGEST=$(curl -s -X POST $BASE/v1/admin/ingest-tokens \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  -d '{"label":"key-gen-bot"}')
INGEST_TOKEN=$(echo "$INGEST" | jq -r .token)

# 3. Это стороннее приложение регистрирует новый ключ (свою ссылку на Yandex Docs)
curl -s -X POST $BASE/v1/ingest/keys \
  -H "Authorization: Bearer $INGEST_TOKEN" -H 'Content-Type: application/json' \
  -d '{"label":"user-42","doc_url":"https://docs.yandex.ru/docs/edit?url=...","traffic_limit_bytes":10737418240}'

# 3b. Позже можно посмотреть список ingest-токенов или отозвать один из них (например, утёкший)
curl -s $BASE/v1/admin/ingest-tokens -H "Authorization: Bearer $ADMIN"
curl -s -X POST $BASE/v1/admin/ingest-tokens/<id>/disable -H "Authorization: Bearer $ADMIN"

# 4. Exit-нода опрашивает назначенные ей ключи
curl -s $BASE/v1/nodes/keys -H "Authorization: Bearer $NODE_TOKEN"

# 5. Exit-нода отправляет пакетный отчёт об использовании
curl -s -X POST $BASE/v1/nodes/$NODE_ID/usage \
  -H "Authorization: Bearer $NODE_TOKEN" -H 'Content-Type: application/json' \
  -d '{"deltas":[{"key_id":"<key-id>","bytes_sent_delta":1048576,"bytes_received_delta":2097152}]}'

# 6. Клиент резолвит свой ключ в данные для подключения
curl -s -X POST $BASE/v1/resolve -H "Authorization: Bearer <client key token>"
```

## Заметки о масштабировании

- Сервис не хранит состояния, кроме Postgres, так что можно запускать несколько реплик за балансировщиком.
- `/v1/resolve` — единственный эндпоинт, к которому напрямую обращается недоверенный клиентский
  токен; он ограничен по частоте на IP (in-memory token bucket — сбрасывается при перезапуске
  реплики, так что это скорее сдерживающая мера от случайного злоупотребления, чем жёсткая
  гарантия).
- Использование трафика репортится пакетно каждой нодой по интервалу, а не на каждый пакет,
  поэтому объём записи в БД масштабируется по числу нод, а не по числу пользователей.
