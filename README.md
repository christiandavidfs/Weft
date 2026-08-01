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

## Requisitos

- Flutter 3.44+ (`brew install flutter`)
- Rust (`rustup` / brew) — `cargo` 1.95+
- protoc (`brew install protobuf`)
- `flutter_rust_bridge_codegen` (`cargo install flutter_rust_bridge_codegen`)
- macOS: Xcode + CocoaPods

## Build y test

```sh
# Core Rust
cargo build -p weft_core            # en core/

# App macOS
cd app
flutter pub get
flutter run -d macos

# Tests (el test carga el dylib nativo compilado por cargo)
cargo build                          # en app/rust → genera target/debug/libweft_rust.dylib
FRB_DART_LOAD_EXTERNAL_LIBRARY_NATIVE_LIB_DIR=rust/target/debug flutter test

# Regenerar bindings tras tocar el API Rust
flutter_rust_bridge_codegen generate
```

## Roadmap

- [x] **Fase 0**: esqueleto — core Rust + bridge + app multiplataforma compilando
- [x] **Fase 1**: sesión — discovery mDNS, coordinador y membresía, token de transmisión con aprobación
- [ ] **Fase 2**: sincronía básica — un dispositivo transmite un archivo, los demás reproducen alineados
- [ ] **Fase 3**: captura de audio del sistema (loopback por plataforma; Android: solo archivos)
- [ ] **Fase 4**: handoff de roles con crossfade
- [ ] **Fase 5**: escala a 10+ dispositivos, tuning de jitter/drift
- [ ] **Fase 6**: futuro — soporte Tailscale (redes remotas) y modo DJ con mezcla de múltiples fuentes
