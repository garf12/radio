from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from websocket_manager import ws_manager

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        while True:
            # Keep connection alive; client may send pings
            await ws.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(ws)
