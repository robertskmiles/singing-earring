#!/usr/bin/env python3
"""Singing Earring server — WebSocket relay + static file serving."""
import argparse
import asyncio
import json
import os
import random
import socket
from http import HTTPStatus
from pathlib import Path

import websockets
from websockets.http11 import Response

PORT = 3000
PUBLIC_DIR = Path(__file__).parent / "public"
TEST_MODE = False
TEST_CODE = "TEST"
MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript",
    ".css": "text/css",
    ".json": "application/json",
}

# Room storage
rooms: dict = {}

CODE_CHARS = "ABCDEFGHJKMNPQRSTUVWXYZ"


def generate_code():
    while True:
        code = "".join(random.choices(CODE_CHARS, k=4))
        if code not in rooms:
            return code


def broadcast_room_state(room, code):
    singer_list = [{"id": info["id"], "part": info["part"]} for info in room["singers"].values()]
    msg = json.dumps({"t": "r", "code": code, "singers": singer_list})
    targets = list(room["singers"].keys())
    if room["conductor"]:
        targets.append(room["conductor"])
    websockets.broadcast(targets, msg)


def serve_static(connection, request):
    """process_request hook: serve static files, pass WebSocket upgrades through."""
    # Let WebSocket upgrades pass
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None

    path = request.path.split("?")[0]  # Strip query params
    if path == "/":
        path = "/index.html"
    if path == "/conductor":
        path = "/conductor.html"

    file_path = (PUBLIC_DIR / path.lstrip("/")).resolve()

    # Security: prevent path traversal
    if not str(file_path).startswith(str(PUBLIC_DIR.resolve())):
        return connection.respond(HTTPStatus.FORBIDDEN, "Forbidden")

    if not file_path.is_file():
        return connection.respond(HTTPStatus.NOT_FOUND, "Not found")

    # Read and serve the file
    ext = file_path.suffix
    content_type = MIME_TYPES.get(ext, "application/octet-stream")
    body = file_path.read_text() if content_type.startswith("text/") else file_path.read_bytes()

    return Response(
        status_code=200,
        reason_phrase="OK",
        headers=websockets.Headers([
            ("Content-Type", content_type),
            ("Content-Length", str(len(body.encode() if isinstance(body, str) else body))),
            ("Cache-Control", "no-cache"),
        ]),
        body=body.encode() if isinstance(body, str) else body,
    )


async def handler(websocket):
    my_room = None
    my_code = None
    my_role = None

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            t = msg.get("t")

            if t == "create":
                if TEST_MODE:
                    code = TEST_CODE
                    # Reuse existing test room or create fresh
                    if code in rooms:
                        rooms[code]["conductor"] = websocket
                    else:
                        rooms[code] = {"conductor": websocket, "singers": {}, "next_id": 0}
                else:
                    code = generate_code()
                    rooms[code] = {"conductor": websocket, "singers": {}, "next_id": 0}
                my_room = rooms[code]
                my_code = code
                my_role = "conductor"
                await websocket.send(json.dumps({"t": "created", "code": code, "ip": get_local_ip(), "port": PORT}))

            elif t == "join":
                code = (msg.get("code") or "").upper()
                if TEST_MODE and not code:
                    code = TEST_CODE
                room = rooms.get(code)
                if not room:
                    if TEST_MODE:
                        # Auto-create room if conductor hasn't connected yet
                        rooms[code] = {"conductor": None, "singers": {}, "next_id": 0}
                        room = rooms[code]
                    else:
                        await websocket.send(json.dumps({"t": "error", "msg": "Room not found"}))
                        continue
                room["next_id"] = room.get("next_id", 0) + 1
                singer_id = room["next_id"]
                room["singers"][websocket] = {"id": singer_id, "part": msg.get("part", "soprano")}
                my_room = room
                my_code = code
                my_role = "singer"
                await websocket.send(json.dumps({"t": "joined", "code": code, "id": singer_id}))
                broadcast_room_state(room, code)

            elif t in ("n", "syl", "vol") and my_role == "conductor" and my_room:
                raw_msg = json.dumps(msg)
                websockets.broadcast(list(my_room["singers"].keys()), raw_msg)

            elif t == "part" and my_role == "singer" and my_room:
                info = my_room["singers"].get(websocket)
                if info:
                    info["part"] = msg.get("part", info["part"])
                    broadcast_room_state(my_room, my_code)

    finally:
        if my_room and my_code:
            if my_role == "conductor":
                ended = json.dumps({"t": "ended"})
                websockets.broadcast(list(my_room["singers"].keys()), ended)
                rooms.pop(my_code, None)
            elif my_role == "singer":
                my_room["singers"].pop(websocket, None)
                if my_code in rooms:
                    broadcast_room_state(my_room, my_code)
                if not my_room["conductor"] and not my_room["singers"]:
                    rooms.pop(my_code, None)


def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "localhost"


async def main():
    async with websockets.serve(
        handler,
        "0.0.0.0",
        PORT,
        process_request=serve_static,
        compression=None,
    ):
        local_ip = get_local_ip()
        if TEST_MODE:
            print(f"Singing Earring server running in TEST MODE on http://0.0.0.0:{PORT}")
            print(f"  Room code: {TEST_CODE} (fixed)")
            print(f"  Conductor: http://{local_ip}:{PORT}/conductor?test")
            print(f"  Singers:   http://{local_ip}:{PORT}?test")
        else:
            print(f"Singing Earring server running on http://0.0.0.0:{PORT}")
            print(f"  Conductor: http://{local_ip}:{PORT}/conductor")
            print(f"  Singers:   http://{local_ip}:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Singing Earring server")
    parser.add_argument("--test", action="store_true",
                        help="Test mode: fixed room code TEST, auto-join for singers")
    args = parser.parse_args()
    TEST_MODE = args.test
    asyncio.run(main())
