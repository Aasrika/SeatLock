"""WS /ws/events/{event_id} -- thin route, same philosophy as every REST
router in this codebase: no business logic here, just wiring to
app/realtime/hub.py.

Sections via `?sections=A,B`; omitted or empty means "subscribe to every
section in this event" (app/realtime/hub.py resolves that at connect
time). On reconnect, the client opens a brand new socket the same way --
there is no resume-from-where-we-left-off protocol, deliberately: the
client is told to always treat a fresh connection as needing a fresh
snapshot (see hub.py's connect()), never assume its cached view survived
whatever caused the previous socket to close.
"""

from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.realtime.hub import get_hub

router = APIRouter()


@router.websocket("/ws/events/{event_id}")
async def seat_map_ws(websocket: WebSocket, event_id: int, sections: str = "") -> None:
    hub = get_hub()
    section_list = [s for s in sections.split(",") if s]
    subscribed = await hub.connect(websocket, event_id, section_list)
    try:
        while True:
            # This phase's protocol is server-push only -- the client
            # never needs to send anything after connecting. Still
            # awaiting receive() (rather than sleeping) is what lets
            # Starlette raise WebSocketDisconnect promptly when the
            # client actually closes, instead of only noticing on the
            # next outbound send.
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await hub.disconnect(websocket, event_id, subscribed)
