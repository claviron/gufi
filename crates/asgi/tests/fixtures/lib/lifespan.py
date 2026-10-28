async def app(scope, receive, send):
    assert scope["type"] == "lifespan"
    message = await receive()
    assert message["type"] == "lifespan.startup"
    await send({"type": "lifespan.startup.complete"})
    message = await receive()
    assert message["type"] == "lifespan.shutdown"
    await send({"type": "lifespan.shutdown.complete"})
