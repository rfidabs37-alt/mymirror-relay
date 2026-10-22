import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

/// Single-instance relay. Put behind an HTTPS reverse proxy in production.
class MirrorRelay {
  final _rooms = <String, _Room>{};
  final _clients = <WebSocket>{};
  final _devices = <String, _DirectoryDevice>{};
  final int maxClients;
  HttpServer? _server;

  MirrorRelay({this.maxClients = 200});
  int get port => _server!.port;

  void _write(WebSocket socket, Object data) {
    if (socket.readyState != WebSocket.open) return;
    try {
      socket.add(data);
    } on StateError {
      // A close handshake can leave readyState open after its sink is closed.
      socket.close();
    }
  }

  Future<void> start({String host = '127.0.0.1', int port = 8080}) async {
    _server = await HttpServer.bind(host, port);
    _server!.listen((request) async {
      if (request.uri.path == '/health') {
        request.response.write('ok');
        await request.response.close();
        return;
      }
      if (!['/mirror', '/devices'].contains(request.uri.path) ||
          !WebSocketTransformer.isUpgradeRequest(request) ||
          _clients.length >= maxClients) {
        request.response.statusCode = HttpStatus.serviceUnavailable;
        await request.response.close();
        return;
      }
      try {
        final ws = await WebSocketTransformer.upgrade(request,
            compression: CompressionOptions.compressionOff);
        if (request.uri.path == '/devices') {
          _acceptDirectory(ws);
        } else {
          _accept(ws);
        }
      } catch (_) {
        // A malformed upgrade must not terminate the server's request loop.
        request.response.close().ignore();
      }
    });
  }

  void _broadcastDevices() {
    final phones = _devices.values.where((d) => d.role == 'phone')
        .map((d) => {'id': d.id, 'name': d.name, 'busy': d.busy}).toList();
    final data = jsonEncode({'event': 'devices', 'devices': phones});
    for (final device in _devices.values.where((d) => d.role == 'pc')) {
      _write(device.socket, data);
    }
  }

  void _acceptDirectory(WebSocket ws) {
    _clients.add(ws);
    ws.pingInterval = const Duration(seconds: 20);
    _DirectoryDevice? device;
    final timer = Timer(const Duration(seconds: 10), () {
      if (device == null) ws.close(1008, 'Registration timeout');
    });
    bool cleaned = false;
    void cleanup() {
      if (cleaned) return;
      cleaned = true;
      timer.cancel();
      _clients.remove(ws);
      if (device != null && identical(_devices[device!.id], device)) {
        _devices.remove(device!.id);
        for (final other in _devices.values) {
          if (other.pendingPeer == device!.id) {
            other.pendingPeer = null;
            _write(other.socket, jsonEncode({'event': 'cancelled', 'message': 'Device went offline'}));
          }
        }
        _broadcastDevices();
      }
    }
    void send(Map<String, dynamic> message) => _write(ws, jsonEncode(message));
    ws.listen((data) {
      try {
        if (data is! String || data.length > 4096) {
          ws.close(1008, 'Invalid directory message');
          return;
        }
        final message = jsonDecode(data) as Map<String, dynamic>;
        if (device == null) {
          final id = message['id'];
          final name = message['name'];
          final role = message['role'];
          if (message['action'] != 'register' || id is! String ||
              !RegExp(r'^[a-zA-Z0-9-]{16,64}$').hasMatch(id) ||
              name is! String || name.trim().isEmpty || name.length > 80 ||
              !['phone', 'pc'].contains(role) || _devices.containsKey(id)) {
            ws.close(1008, 'Invalid or duplicate device registration');
            return;
          }
          device = _DirectoryDevice(ws, id, name.trim(), role as String);
          _devices[id] = device!;
          timer.cancel();
          send({'event': 'registered'});
          _broadcastDevices();
          return;
        }
        final current = device!;
        switch (message['action']) {
          case 'request':
            final target = _devices[message['target']];
            final code = message['code'];
            if (current.role != 'pc' || target == null || target.role != 'phone' ||
                target.busy || target.pendingPeer != null || current.pendingPeer != null ||
                code is! String || !_rooms.containsKey(code)) {
              send({'event': 'unavailable', 'message': 'Phone unavailable; refresh and try again'});
              return;
            }
            current.pendingPeer = target.id;
            target.pendingPeer = current.id;
            _write(target.socket, jsonEncode({'event': 'request', 'from': current.id,
              'name': current.name, 'code': code}));
          case 'reject':
          case 'cancel':
            final peer = _devices[current.pendingPeer];
            current.pendingPeer = null;
            if (peer != null && peer.pendingPeer == current.id) {
              peer.pendingPeer = null;
              _write(peer.socket, jsonEncode({'event': 'cancelled',
                'message': message['action'] == 'reject' ? 'Phone declined the request' : 'Request cancelled'}));
            }
          case 'busy':
            final wasBusy = current.busy;
            current.busy = message['value'] == true;
            if (wasBusy && !current.busy) {
              final peer = _devices[current.pendingPeer];
              current.pendingPeer = null;
              if (peer?.pendingPeer == current.id) peer!.pendingPeer = null;
            }
            _broadcastDevices();
          case 'list':
            _broadcastDevices();
        }
      } catch (_) {
        ws.close(1008, 'Invalid directory message');
      }
    }, onError: (Object error) => cleanup(), onDone: cleanup, cancelOnError: true);
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
      _write(ws, jsonEncode({'event': 'error', 'message': reason}));
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
            _write(ws, jsonEncode({'event': 'created', 'code': code}));
          } else if (message['action'] == 'join') {
            final found = _rooms[message['code']];
            if (found == null || found.sender != null) {
              reject('Session unavailable or code incorrect');
              return;
            }
            room = found;
            found.sender = ws;
            found.expiry?.cancel();
            _write(found.receiver, jsonEncode({'event': 'peer'}));
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
      if (peer != null) _write(peer, data as Object);
    }, onError: (Object error) => cleanup(), onDone: cleanup,
        cancelOnError: true);
  }

  Future<void> close() async {
    _devices.clear();
    for (final room in _rooms.values) {
      room.expiry?.cancel();
    }
    for (final ws in _clients.toList()) {
      unawaited(ws.close(1001, 'Relay shutting down'));
    }
    await _server?.close(force: true);
  }
}

class _DirectoryDevice {
  final WebSocket socket;
  final String id;
  final String name;
  final String role;
  bool busy = false;
  String? pendingPeer;
  _DirectoryDevice(this.socket, this.id, this.name, this.role);
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
