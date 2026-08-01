import 'package:flutter_test/flutter_test.dart';

import 'package:weft/main.dart';
import 'package:weft/src/rust/frb_generated.dart';

void main() {
  setUpAll(() async => await RustLib.init());

  testWidgets('Home renders network session controls', (tester) async {
    await tester.pumpWidget(const WeftApp());

    expect(find.textContaining('weft-core'), findsOneWidget);
    expect(find.text('Iniciar red'), findsOneWidget);
    expect(find.text('Detener'), findsOneWidget);
    expect(find.text('Pedir transmitir'), findsOneWidget);
  });
}
