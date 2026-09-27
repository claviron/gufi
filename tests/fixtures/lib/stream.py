import asyncio


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        await receive()
        await send({"type": "lifespan.startup.complete"})
        await receive()
        await send({"type": "lifespan.shutdown.complete"})
        return

    parts = []
    while True:
        message = await receive()
        assert message["type"] == "http.request"
        if message["body"]:
            parts.append(message["body"])
        if not message["more_body"]:
            break

    # Past the body, a receive is how an application watches for its client
    # leaving. It must not answer before the reply is done.
    listener = asyncio.ensure_future(receive())
    await asyncio.sleep(0)
    early = listener.done()

    # Reported in the body: an assertion after the reply has nobody to tell.
    verdict = b";early" if early else b";waiting"
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"|".join(parts) + verdict, "more_body": True})
    await send({"type": "http.response.body", "body": b"", "more_body": False})
    # The reply is complete, so the waiting receive now answers; shutdown waits
    # for this request, so a receive that never answered would hang the test.
    event = await listener
    assert event["type"] == "http.disconnect"
