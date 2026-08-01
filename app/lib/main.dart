import 'dart:async';

import 'package:flutter/material.dart';
import 'dart:io' show Platform;

import 'package:weft/src/rust/api/engine.dart';
import 'package:weft/src/rust/api/network.dart';
import 'package:weft/src/rust/frb_generated.dart';

Future<void> main() async {
  await RustLib.init();
  runApp(const WeftApp());
}

class WeftApp extends StatelessWidget {
  const WeftApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Weft',
      theme: ThemeData(
        colorScheme: ColorScheme.fromSeed(seedColor: Colors.deepPurple),
      ),
      home: const HomePage(),
    );
  }
}

class HomePage extends StatefulWidget {
  const HomePage({super.key});

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  final TextEditingController _nameController = TextEditingController();
  final TextEditingController _fileController = TextEditingController();
  NetworkStatusView _status = emptyStatus();
  MediaStatsView? _media;
  final List<String> _events = [];
  StreamSubscription<NetworkEventView>? _sub;
  Timer? _poll;

  @override
  void initState() {
    super.initState();
    _nameController.text = _defaultName();
    _sub = networkEvents().listen((ev) {
      setState(() {
        _events.insert(0, '[${ev.kind}] ${ev.deviceName.isNotEmpty ? ev.deviceName : ev.deviceId}: ${ev.message}');
        if (_events.length > 8) _events.removeLast();
        _status = networkStatus();
        _media = networkMediaStats();
      });
    });
    _poll = Timer.periodic(const Duration(seconds: 1), (_) {
      if (mounted) {
        setState(() {
          _status = networkStatus();
          _media = networkMediaStats();
        });
      }
    });
  }

  @override
  void dispose() {
    _sub?.cancel();
    _poll?.cancel();
    _nameController.dispose();
    super.dispose();
  }

  String _defaultName() {
    try {
      final host = Platform.localHostname.split('.').first;
      return host.isEmpty ? 'dispositivo' : host;
    } catch (_) {
      return 'dispositivo';
    }
  }

  Future<void> _start() async {
    final name = _nameController.text.trim();
    try {
      networkStartWith(deviceName: name.isEmpty ? 'dispositivo' : name, enableAudio: true);
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context)
          .showSnackBar(SnackBar(content: Text('Error: $e')));
    }
    setState(() {
      _status = networkStatus();
      _media = networkMediaStats();
    });
  }

  void _stop() {
    networkStop();
    setState(() {
      _status = networkStatus();
      _media = null;
    });
  }

  void _transmitFile() {
    final path = _fileController.text.trim();
    if (path.isEmpty) return;
    try {
      networkTransmitFile(path: path);
    } catch (e) {
      if (!mounted) return;
      ScaffoldMessenger.of(context)
          .showSnackBar(SnackBar(content: Text('Error: $e')));
    }
  }

  String _displayName(String id) {
    if (id.isEmpty) return '—';
    final member = _status.members.where((m) => m.deviceId == id);
    if (member.isNotEmpty) return member.first.deviceName;
    final peer = _status.peers.where((p) => p.deviceId == id);
    if (peer.isNotEmpty) return peer.first.deviceName;
    if (id == _status.deviceId) return _status.deviceName;
    return id.substring(0, 8);
  }

  @override
  Widget build(BuildContext context) {
    final running = _status.running;
    final isCoordinator = running && _status.role == 'coordinator';
    final amTransmitter = running && _status.transmitterId == _status.deviceId;
    return Scaffold(
      appBar: AppBar(title: const Text('Weft — Fase 2')),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(16),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            _card(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  TextField(
                    controller: _nameController,
                    enabled: !running,
                    decoration: const InputDecoration(
                      labelText: 'Nombre del dispositivo',
                      border: OutlineInputBorder(),
                    ),
                  ),
                  const SizedBox(height: 12),
                  Row(
                    children: [
                      Expanded(
                        child: FilledButton.icon(
                          onPressed: running ? null : _start,
                          icon: const Icon(Icons.play_arrow),
                          label: const Text('Iniciar red'),
                        ),
                      ),
                      const SizedBox(width: 12),
                      Expanded(
                        child: OutlinedButton.icon(
                          onPressed: running ? _stop : null,
                          icon: const Icon(Icons.stop),
                          label: const Text('Detener'),
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
            const SizedBox(height: 12),
            _card(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  Text('Estado de la sesión',
                      style: Theme.of(context).textTheme.titleMedium),
                  const SizedBox(height: 8),
                  _row('Rol', running ? _status.role : 'off'),
                  _row('Sesión',
                      running ? _shortId(_status.sessionId) : '—'),
                  _row('Coordinador', running ? _displayName(_status.coordinatorId) : '—'),
                  _row('Transmisor', running ? _displayName(_status.transmitterId) : '—'),
                  const SizedBox(height: 12),
                  Row(
                    children: [
                      Expanded(
                        child: FilledButton.icon(
                          onPressed: running && !amTransmitter ? _requestTransmit : null,
                          icon: const Icon(Icons.radio),
                          label: const Text('Pedir transmitir'),
                        ),
                      ),
                      const SizedBox(width: 12),
                      Expanded(
                        child: OutlinedButton.icon(
                          onPressed: amTransmitter ? _releaseTransmit : null,
                          icon: const Icon(Icons.stop_circle),
                          label: const Text('Liberar'),
                        ),
                      ),
                    ],
                  ),
                  if (amTransmitter) ...[
                    const SizedBox(height: 12),
                    TextField(
                      controller: _fileController,
                      decoration: const InputDecoration(
                        labelText: 'Ruta del archivo de audio (wav/mp3/flac)',
                        border: OutlineInputBorder(),
                      ),
                    ),
                    const SizedBox(height: 8),
                    FilledButton.tonalIcon(
                      onPressed: _transmitFile,
                      icon: const Icon(Icons.upload_file),
                      label: const Text('Transmitir archivo'),
                    ),
                  ],
                ],
              ),
            ),
            if (running) ...[
              const SizedBox(height: 12),
              _card(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    Text('Plano de medios', style: Theme.of(context).textTheme.titleMedium),
                    const SizedBox(height: 8),
                    _row('Puerto UDP', _media != null ? '${_media!.mediaPort}' : '—'),
                    _row('Reloj',
                        (_media?.clockSynced ?? false) ? 'sincronizado' : 'no sincronizado'),
                    if (_media?.clockSynced ?? false) ...[
                      _row('Offset', '${_media!.clockOffsetUs} µs'),
                      _row('RTT', '${_media!.clockRttUs} µs'),
                    ],
                    _row('Recibidos', '${_media?.receivedPackets ?? 0} paquetes'),
                    _row('Transmitidos', '${_media?.transmittedPackets ?? 0} paquetes'),
                    _row('Buffer', '${_media?.bufferedPackets ?? 0} paquetes (${_media?.bufferedUs ?? 0} µs)'),
                    if ((_media?.lastError ?? '').isNotEmpty)
                      _row('Error', _media!.lastError),
                  ],
                ),
              ),
            ],
            if (isCoordinator && _status.pendingTransmitRequests.isNotEmpty) ...[
              const SizedBox(height: 12),
              _card(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    Text('Solicitudes de transmisión',
                        style: Theme.of(context).textTheme.titleMedium),
                    const SizedBox(height: 8),
                    for (final id in _status.pendingTransmitRequests)
                      Padding(
                        padding: const EdgeInsets.symmetric(vertical: 4),
                        child: Row(
                          children: [
                            Expanded(
                              child: Text('${_displayName(id)} pide el token'),
                            ),
                            IconButton(
                              icon: const Icon(Icons.check, color: Colors.green),
                              onPressed: () => networkApproveTransmit(deviceId: id),
                              tooltip: 'Aprobar',
                            ),
                            IconButton(
                              icon: const Icon(Icons.close, color: Colors.red),
                              onPressed: () => networkDenyTransmit(deviceId: id),
                              tooltip: 'Rechazar',
                            ),
                          ],
                        ),
                      ),
                  ],
                ),
              ),
            ],
            const SizedBox(height: 12),
            _card(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  Text('Miembros (${_status.members.length})',
                      style: Theme.of(context).textTheme.titleMedium),
                  const SizedBox(height: 8),
                  if (_status.members.isEmpty)
                    const Text('Aún no hay miembros en la sesión.')
                  else
                    for (final m in _status.members)
                      ListTile(
                        dense: true,
                        contentPadding: EdgeInsets.zero,
                        leading: const Icon(Icons.devices),
                        title: Text(m.isMe ? '${m.deviceName} (tú)' : m.deviceName),
                        subtitle: Text(m.addr.isEmpty ? _status.role : m.addr),
                        trailing: Row(
                          mainAxisSize: MainAxisSize.min,
                          children: [
                            if (m.deviceId == _status.coordinatorId)
                              const Chip(label: Text('coord'), visualDensity: VisualDensity.compact),
                            if (m.isTransmitter)
                              const Chip(label: Text('tx'), visualDensity: VisualDensity.compact),
                          ],
                        ),
                      ),
                ],
              ),
            ),
            const SizedBox(height: 12),
            _card(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  Text('Dispositivos descubiertos (${_status.peers.length})',
                      style: Theme.of(context).textTheme.titleMedium),
                  const SizedBox(height: 8),
                  if (_status.peers.isEmpty)
                    const Text('Esperando dispositivos en la red...')
                  else
                    for (final p in _status.peers)
                      ListTile(
                        dense: true,
                        contentPadding: EdgeInsets.zero,
                        leading: const Icon(Icons.wifi_tethering),
                        title: Text('${p.deviceName} ${p.isCoordinator ? '(coord)' : ''}'),
                        subtitle: Text(p.addr),
                      ),
                ],
              ),
            ),
            const SizedBox(height: 12),
            _card(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: [
                  Text('Eventos', style: Theme.of(context).textTheme.titleMedium),
                  const SizedBox(height: 8),
                  if (_events.isEmpty)
                    const Text('Sin eventos todavía.')
                  else
                    for (final e in _events)
                      Padding(
                        padding: const EdgeInsets.symmetric(vertical: 2),
                        child: Text(e, style: Theme.of(context).textTheme.bodySmall),
                      ),
                ],
              ),
            ),
            const SizedBox(height: 12),
            Text(
              'Core: ${bridgeVersion()}',
              textAlign: TextAlign.center,
              style: Theme.of(context).textTheme.bodySmall,
            ),
          ],
        ),
      ),
    );
  }

  void _requestTransmit() {
    networkRequestTransmit();
    setState(() => _status = networkStatus());
  }

  void _releaseTransmit() {
    networkReleaseTransmit();
    setState(() => _status = networkStatus());
  }

  String _shortId(String id) => id.isEmpty ? '—' : '${id.substring(0, 8)}…';

  Widget _card({required Widget child}) {
    return Card(
      margin: EdgeInsets.zero,
      child: Padding(padding: const EdgeInsets.all(16), child: child),
    );
  }

  Widget _row(String label, String value) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 3),
      child: Row(
        children: [
          SizedBox(
            width: 110,
            child: Text(label, style: const TextStyle(fontWeight: FontWeight.bold)),
          ),
          Expanded(child: Text(value)),
        ],
      ),
    );
  }
}

NetworkStatusView emptyStatus() => const NetworkStatusView(
      running: false,
      deviceId: '',
      deviceName: '',
      role: 'off',
      sessionId: '',
      coordinatorId: '',
      transmitterId: '',
      members: [],
      peers: [],
      pendingTransmitRequests: [],
    );
