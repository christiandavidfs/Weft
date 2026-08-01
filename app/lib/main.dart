import 'package:flutter/material.dart';
import 'package:weft/src/rust/api/engine.dart';
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
  bool _active = false;
  BigInt? _sessionId;

  void _startSession() {
    setState(() {
      _sessionId = engineStartSession();
      _active = true;
    });
  }

  void _stopSession() {
    engineStopSession();
    setState(() {
      _sessionId = null;
      _active = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    final status = engineStatus();
    return Scaffold(
      appBar: AppBar(title: const Text('Weft — Fase 0')),
      body: Center(
        child: Padding(
          padding: const EdgeInsets.all(32),
          child: Column(
            mainAxisAlignment: MainAxisAlignment.center,
            children: [
              Text(
                'Core: ${bridgeVersion()}',
                style: Theme.of(context).textTheme.titleMedium,
              ),
              const SizedBox(height: 24),
              _StatusRow(
                label: 'Sesión activa',
                value: _active ? 'sí (id $_sessionId)' : 'no',
              ),
              _StatusRow(
                label: 'Latencia objetivo',
                value: '${status.targetLatencyMs} ms',
              ),
              _StatusRow(
                label: 'Reloj de sesión',
                value: status.elapsedUs != null ? '${status.elapsedUs} µs' : '—',
              ),
              const SizedBox(height: 32),
              FilledButton.icon(
                onPressed: _active ? null : _startSession,
                icon: const Icon(Icons.play_arrow),
                label: const Text('Iniciar sesión'),
              ),
              const SizedBox(height: 12),
              OutlinedButton.icon(
                onPressed: _active ? _stopSession : null,
                icon: const Icon(Icons.stop),
                label: const Text('Detener sesión'),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class _StatusRow extends StatelessWidget {
  const _StatusRow({required this.label, required this.value});

  final String label;
  final String value;

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Text(
            '$label: ',
            style: const TextStyle(fontWeight: FontWeight.bold),
          ),
          Text(value),
        ],
      ),
    );
  }
}
