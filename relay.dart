import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

/// Single-instance relay. Put behind an HTTPS reverse proxy in production.
class MirrorRelay {
  final _rooms = <String, _Room>{};
  final _clients = <WebSocket>{};
  final int maxClients;
  HttpServer? _server;

  MirrorRelay({this.maxClients = 200});
  int get port => _server!.port;

  Future<void> start({String host = '127.0.0.1', int port = 8080}) async {
    _server = await HttpServer.bind(host, port);
    _server!.listen((request) async {
      if (request.uri.path == '/health') {
        request.response.write('ok');
        await request.response.close();
        return;
      }
      if (request.uri.path != '/mirror' ||
          !WebSocketTransformer.isUpgradeRequest(request) ||
          _clients.length >= maxClients) {
        request.response.statusCode = HttpStatus.serviceUnavailable;
        await request.response.close();
        return;
      }
      try {
        final ws = await WebSocketTransformer.upgrade(request,
            compression: CompressionOptions.compressionOff);
        _accept(ws);
      } catch (_) {
        // A malformed upgrade must not terminate the server's request loop.
        request.response.close().ignore();
      }
    });
  }

  void _accept(WebSocket ws) {
    _clients.add(ws);
    ws.pingInterval = const Duration(seconds: 20);
    _Room? room;
    bool receiver = false;
    final timer = Timer(const Duration(seconds: 10), () {
      if (room == null) ws.close(1008, 'Join timeout');
    });
    void reject(String reason) {
      ws.add(jsonEncode({'event': 'error', 'message': reason}));
      ws.close(1008, reason);
    }

    bool cleaned = false;
    void cleanup() {
      if (cleaned) return;
      cleaned = true;
      timer.cancel();
      _clients.remove(ws);
      final current = room;
      if (current == null) return;
      current.expiry?.cancel();
      _rooms.remove(current.code);
      final peer = receiver ? current.sender : current.receiver;
      peer?.close(1000, 'Peer disconnected');
    }

    ws.listen((data) {
      if (room == null) {
        try {
          if (data is! String || data.length > 512) {
            reject('Invalid handshake');
            return;
          }
          final message = jsonDecode(data) as Map<String, dynamic>;
          if (message['action'] == 'create') {
            receiver = true;
            final random = Random.secure();
            String code;
            do {
              code = base64Url.encode(List.generate(12, (_) => random.nextInt(256)))
                  .replaceAll('=', '');
            } while (_rooms.containsKey(code));
            room = _Room(ws, code);
            _rooms[code] = room!;
            room!.expiry = Timer(const Duration(minutes: 10), () {
              if (room!.sender == null) ws.close(1000, 'Session code expired');
            });
            ws.add(jsonEncode({'event': 'created', 'code': code}));
          } else if (message['action'] == 'join') {
            final found = _rooms[message['code']];
            if (found == null || found.sender != null) {
              reject('Session unavailable or code incorrect');
              return;
            }
            room = found;
            found.sender = ws;
            found.expiry?.cancel();
            found.receiver.add(jsonEncode({'event': 'peer'}));
          } else {
            reject('Invalid handshake');
          }
        } catch (_) {
          reject('Invalid handshake');
        }
        return;
      }
      if ((data is String && data.length > 16384) ||
          (data is List<int> && data.length > 262144)) {
        reject('Message too large');
        return;
      }
      // Only the phone can publish video. Control flows in both directions.
      if (receiver && data is! String) return;
      final peer = receiver ? room!.sender : room!.receiver;
      if (peer?.readyState == WebSocket.open) peer!.add(data);
    }, onError: (Object error) => cleanup(), onDone: cleanup,
        cancelOnError: true);
  }

  Future<void> close() async {
    for (final room in _rooms.values) {
      room.expiry?.cancel();
    }
    for (final ws in _clients.toList()) {
      unawaited(ws.close(1001, 'Relay shutting down'));
    }
    await _server?.close(force: true);
  }
}

class _Room {
  final WebSocket receiver;
  final String code;
  WebSocket? sender;
  Timer? expiry;
  _Room(this.receiver, this.code);
}

Future<void> main() async {
  final relay = MirrorRelay();
  await relay.start(
    host: Platform.environment['HOST'] ?? '127.0.0.1',
    port: int.parse(Platform.environment['PORT'] ?? '8080'),
  );
  stdout.writeln('MyMirror relay listening on port ${relay.port}');
}
