import 'package:flutter_test/flutter_test.dart';
import 'package:weft/main.dart';
import 'package:weft/src/rust/frb_generated.dart';
import 'package:integration_test/integration_test.dart';

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();
  setUpAll(() async => await RustLib.init());
  testWidgets('Can call rust engine', (WidgetTester tester) async {
    await tester.pumpWidget(const WeftApp());
    expect(find.text('Iniciar sesión'), findsOneWidget);
  });
}
