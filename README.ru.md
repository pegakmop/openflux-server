# OpenFlux Server

[English](README.md) | **Русский**

Форк [p1neappleXpress/OpenFlux](https://github.com/p1neappleXpress/OpenFlux). Исследовательский
инструмент сетевого стека: TCP-туннель с подключаемыми транспортами, плюс многопользовательский
control plane и gomobile-биндинги для
[Android-приложения](https://github.com/wlruscfd/openflux-app).

Смежные репозитории: [openflux-app](https://github.com/wlruscfd/openflux-app) (Android-клиент) и
[openflux-deploy](https://github.com/wlruscfd/openflux-deploy) (разворачивает `controlplane` из
этого репозитория на VPS).

## Обзор
```
Client (SOCKS5) --> Transport --> Exit Node --> Internet
```

TCP-пакеты передаются через Transport. На данный момент доступны два транспорта:
1. Yandex — отправляет пакеты через курсорные сообщения Yandex Docs;
2. Max — отправляет пакеты через WebRTC DataChannel (только десктопный клиент/exit-node —
   Android-приложение его не поддерживает; почему — см. комментарий к пакету в `mobile/mobile.go`).

Клиентская часть запускает SOCKS5-прокси, выходная нода декапсулирует и пересылает пакеты в пункт
назначения.

## Требования
1. Golang v. 1.26.3+ — для сборки бинарника десктопного клиента / выходной ноды (universal-bypass-tool);
2. Android NDK v.27.0.12077973+ — для сборки `.aar`, который встраивает Android-приложение (`./build_android_aar.sh`);
3. XCode v. 26.6+ — для сборки бинарника для iOS-клиента;
4. Linux VPS/VDS для выходной ноды, а если нужен многопользовательский control plane — то и для
   `controlplane` тоже (см. [openflux-deploy](https://github.com/wlruscfd/openflux-deploy)).

## Структура

```
main.go
transport/
├── transport.go      # Transport interface
├── yandex/           # Yandex Docs backend
└── oneme/            # MAX Messenger backend (только десктоп)
tunnel/
├── tunnel.go         # TCP tunnel core
├── endpoint.go       # Virtual NIC
└── rawsocket.go      # Raw socket (exit node)
socks5/                # SOCKS5-сервер (десктопный клиент)
gateway/               # TUN-based прозрачный прокси (Android-клиент, через VpnService)
mobile/                # Точка входа gomobile bind, используется openflux-app
nodeagent/             # Оркестратор exit-node для managed-режима (controlplane)
controlplane/          # Сервис ключей/трафика/токенов + панель управления — отдельный
│                      # Go-модуль, см. controlplane/README.md
network/               # Checksums, packet parsing
utils/                 # Debug logging
```

## Сборка (бинарник десктоп-клиента / выходной ноды)

```bash
go mod tidy
go build -o universal-bypass-tool .
```

## Сборка для Android
См. README [openflux-app](https://github.com/wlruscfd/openflux-app) — `./build_android_aar.sh`
здесь собирает `mobile/` в `.aar` через `gomobile bind` для этого репозитория.

## Сборка для iOS (клиентский бинарник)
```bash
export XCODE_PATH="<путь до вашего Xcode.app>" # опционально, по умолчанию /Applications/Xcode.app
./build_ios.sh
```

## Использование

### Настройка выходной ноды
1. У вас должен быть root-доступ на машине выходной ноды;
2. Поддерживается только устаревший редактор документов Yandex (переключается в настройках интерфейса).

```bash
sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP
sudo ./universal-bypass-tool --exit-node --url "YOUR_YANDEX_DOC_URL" --debug
```

### Настройка десктопного клиента

```bash
./universal-bypass-tool --client --url "YOUR_YANDEX_DOC_URL" --socks5 :1080 --debug
```

Затем настройте SOCKS5-прокси в браузере на localhost:1080.

## Флаги

| Флаг          | По умолчанию        | Описание                       |
|---------------|---------------------|--------------------------------|
| `--client`    |                     | Запуск в режиме клиента        |
| `--exit-node` |                     | Запуск в режиме ноды           |
| `--socks5`    | `:1080`             | Адрес SOCKS5 прокси            |
| `--url`       | `https://localhost` | URL документа (Yandex Docs)    |
| `--maxToken`  | ``                  | Токен авторизации (Max)        |
| `--maxUid`    | ``                  | ID пользователя (Max)          |
| `--debug`     | `false`             | Включить подробное логирование |
| `--transport` | `yandex`            | Выбор транспорта               |
| `--managed`      | `false` | Только для exit-node: получать активные ключи из controlplane вместо одного `--url` |
| `--control-url`  | ``      | Managed-режим: базовый URL сервиса `openflux-control` |
| `--node-token`   | ``      | Managed-режим: токен этой ноды, выданный controlplane |

## Многопользовательский режим (controlplane)

Для работы с большим числом ключей/пользователей на нескольких exit-нодах — токены авторизации,
учёт трафика по ключам, включение/отключение ключей, веб-панель управления и API для загрузки
ключей сторонними приложениями — см. [controlplane/README.md](controlplane/README.md). Exit-нода
включает этот режим флагами `--exit-node --managed --control-url ... --node-token ...`; обычный
режим с одним `--url` из раздела выше продолжает работать без изменений для ручного/разового
использования. Чтобы реально развернуть `controlplane` на VPS (Postgres, systemd, Nginx, Let's
Encrypt), см. [openflux-deploy](https://github.com/wlruscfd/openflux-deploy).

## Реализация собственных транспортов

Вы можете реализовать интерфейс `Transport` из `transport/transport.go` и зарегистрировать свой
транспорт в switch-блоке в `main.go`.

## Лицензия

Проект распространяется под лицензией **GNU General Public License v3.0 or later**.
Полный текст — в файле [LICENSE](LICENSE).

Лицензии третьих сторон — в файле [NOTICE](NOTICE).

## Дисклеймер

Только для образовательного использования. Тестируйте на собственных машинах и сетях.
