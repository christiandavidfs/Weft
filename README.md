# Weft

Sincronización de audio multi-room en tiempo real, multiplataforma: reproducí música/audio en un dispositivo y soná en todos los demás **al mismo tiempo** (estilo Sonos, pero propio y con roles dinámicos).

**Plataformas**: macOS · Windows · Linux · Android · (iOS próximamente)

---

## ¿Qué hace?

- **Un dispositivo transmite, todos reproducen sincronizados**: el audio se streamea desde la fuente hacia todos los reproductores con una tolerancia de desfase < 20 ms entre dispositivos.
- **Roles dinámicos**: cualquier dispositivo puede pedir ser la fuente durante la sesión. El coordinador consulta al transmisor actual; si cede, hay un handoff con crossfade (~100 ms) sin cortes bruscos. Sin conflictos: solo el dueño del "token" de transmisión suena.
- **Captura de audio del sistema** (loopback) o **reproducción de archivos**.
- **Latencia objetivo ~100 ms** con corrección de drift por resampling (imperceptible).

## Arquitectura

Se separan dos planos en la red:

| Plano | Rol | Función |
|-------|-----|---------|
| **Control** | **Coordinador** | Gestiona membresía, quién transmite (token), el reloj de sesión y los handoffs |
| **Media** | **Transmisor** | Captura/lee audio y lo streamea con timestamps |
| **Media** | **Receptor** | Reproduce sincronizado a la línea de tiempo común |

### Sincronización

1. Un **reloj de sesión virtual** (NTP-lite) es la referencia para todos los nodos; cada receptor calcula offset y drift con mensajes periódicos de sync.
2. Cada paquete de audio lleva un `sesionTime`; el receptor reproduce cuando `relojLocal + offset == sesionTime`.
3. El receptor mantiene un **jitter buffer** y corrige el drift ajustando la velocidad de reproducción (resampling ±0.1%) en vez de cortar.

### Handoff de roles (cambio de fuente)

```
Receptor nuevo        Coordinador             Transmisor actual
    | request_transmit    |                         |
    |-------------------->| ask_current(cede?)      |
    |                     |------------------------>|
    |                     |<-- si / no / timeout    |
    |  grant / deny       |                         |
    |<--------------------| begin_crossfade(~100ms) |
    |                     |------------------------>|
    |  start_stream(con sesionTime heredado)        |
```

- Timeout si el transmisor actual no responde (~2 s).
- El nuevo transmisor **hereda la línea de tiempo de la sesión**: los receptores nunca ven un salto de reloj.
- Rollback: si el nuevo no logra streamear, el token vuelve al anterior.

## Stack

| Capa | Tecnología |
|------|------------|
| UI/App | Flutter (un codebase, 4 plataformas) |
| Motor de audio y sync | Rust (`cpal` + Opus + reloj NTP-lite) expuesto vía `flutter_rust_bridge` |
| Control | WebSocket + Protobuf |
| Media | RTP/UDP propio |
| Discovery | mDNS/zeroconf (LAN) |

## Estructura del repo

```
weft/
├── core/          # Librería Rust pura: reloj de sesión, engine de sync (sin deps de Flutter)
├── protocol/      # Specs .proto (en desarrollo)
└── app/           # Flutter
    ├── rust/      # Glue flutter_rust_bridge (weft_rust) → depende de core/
    └── rust_builder/  # cargokit: compila el crate Rust en cada plataforma
```

## Requisitos generales

Estas herramientas se necesitan en **todas** las plataformas:

- Rust (`rustup` recomendado) — `cargo` 1.95+
- Flutter 3.44+
- Protobuf compiler `protoc`
- `flutter_rust_bridge_codegen` (`cargo install flutter_rust_bridge_codegen`)

El back-end de audio usa `cpal`, que en cada SO habla con el backend de audio
nativo (CoreAudio, WASAPI, ALSA, Oboe). Si vas a probar solo el `core` (Rust
sin GUI) basta con la dependencia de audio de tu sistema; para correr la app
Flutter de escritorio hace falta el toolchain GTK/Win/… correspondiente.

## Configuración por plataforma

> **Estado del scaffold Flutter:** hoy solo está generado el target `macos/`.
> Para habilitar otra plataforma primero genera su folder desde `app/`:
> ```sh
> flutter create . --project-name weft --platforms=linux,windows,android,ios
> ```
> (mantiene el pubspec/tests existentes; se añaden los folders de la plataforma).
> Los prerequisitos de sistema de abajo son los que necesitará cada una una vez
> habilitada. El `core` Rust en sí ya compila en macOS/Linux/Windows.

### macOS
```sh
brew install flutter protobuf
# Xcode + CocoaPods (terminal: pod setup)
sudo gem install cocoapods
```

### Linux (Debian/Ubuntu)
```sh
sudo apt update
sudo apt install -y build-essential pkg-config \
    libasound2-dev     # cpal → ALSA
    clang cmake ninja-build \
    libgtk-3-dev liblzma-dev libstdc++-12-dev   # app Flutter Linux
```
Instala Rust y Flutter con sus instaladores oficiales (`flutter` requiere
GTK3 y los paquetes de arriba detectados por `flutter doctor`).

### Arch Linux
```sh
sudo pacman -S --needed base-devel pkg-config \
    alsa-lib clang cmake ninja gtk3 liblzma git
```
(El resto — Rust, Flutter, `protoc` — vía `pacman`/`paru`: `rustup`,
`flutter`, `protobuf`.)

### Windows
- **Visual Studio Build Tools 2022** con la carga de trabajo *"Desktop development with C++"* (necesaria para el toolchain MSVC de Rust y para Flutter Windows).
- Rust con toolchain MSVC (`rustup default stable-msvc`).
- `protoc` (winget / chocolatey).

### Android
- **Android Studio** + SDK (Android 8.0+, `minSdk`).
- **NDK 26+** y **CMake/Ninja** (`sdkmanager "ndk;26.3.11579264"`), requeridos para compilar el crate Rust (cargokit) y el backend Oboe de `cpal`.
- JDK 17.
- `flutter devices` debe listar tu dispositivo/emulador antes de correr.

> Nota: en Android la captura de audio la hace la app (el `core` reproduce
> archivos). Las fuentes de sistema (loopback) son por-plataforma y aún no están
> soportadas en Android.

## Build y test

```sh
# Core Rust (sin GUI) — válido en macOS/Linux/Windows
cargo build -p weft_core            # en core/

# Tests del core
cargo test -p weft_core --test session

# App Flutter
cd app
flutter pub get

# macOS / Linux / Windows escritorio
flutter run -d macos   |   flutter run -d linux   |   flutter run -d windows

# Android (con dispositivo/emulador conectado)
flutter run -d android

# Tests (el test carga el dylib/so/dll nativo compilado por cargo)
cargo build                          # en desktop app/rust → libweft_rust.dylib (o .so/.dll)
FRB_DART_LOAD_EXTERNAL_LIBRARY_NATIVE_LIB_DIR=rust/target/debug flutter test

# Regenerar bindings tras tocar el API Rust
flutter_rust_bridge_codegen generate
```

## Roadmap

- [x] **Fase 0**: esqueleto — core Rust + bridge + app multiplataforma compilando
- [x] **Fase 1**: sesión — discovery mDNS, coordinador y membresía, token de transmisión con aprobación
- [x] **Fase 2**: sincronía básica — un dispositivo transmite un archivo, los demás reproducen alineados (UDP, jitter buffer, NTP-lite, playback cpal)
- [~] **Fase 3**: captura de micrófono opcional con downmix estéreo y resampler (macOS nativo; loopback del sistema pendiente: requiere driver virtual por plataforma, Android: solo archivos)
- [x] **Fase 4**: handoff de roles con crossfade — el coordinador pide el token al transmisor actual (`cede?`, timeout 2 s) y, si cede, el receptor mezcla la cola del viejo con la cabeza del nuevo (~100 ms) sin cortes; rollback si el nuevo no emite
- [x] **Fase 5**: escala a 10+ dispositivos — fan-out del plano de medios probado con 1 transmisor y 10 receptores; tuning de jitter/drift configurable (`MediaConfig`: capacidad del jitter buffer, latencia objetivo y umbral de drift) sin recompilar
- [x] **Fase 6**: Tailscale / redes remotas y modo DJ — unirse al coordinador por IP en vez de mDNS (`start_joining`/`connect_to_coordinator`); modo DJ con **múltiples transmisores simultáneos**: `MediaConfig.dj` activa el mezclador (`DjMixer`, una fuente por transmisor compartiendo el reloj de sesión), la sesión permite a varios dispositivos transmitir a la vez (`dj_transmit`) y la app Flutter permite activar el modo y unirse/salir como transmisor DJ
