async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        await receive()
        await send({"type": "lifespan.startup.complete"})
        await receive()
        await send({"type": "lifespan.shutdown.complete"})
        return

    assert scope["type"] == "http"
    assert scope["method"] == "POST"
    assert scope["path"] == "/hello"
    message = await receive()
    assert message["body"] == b"request"
    await send({
        "type": "http.response.start",
        "status": 201,
        "headers": [(b"content-type", b"text/plain")],
    })
    await send({"type": "http.response.body", "body": b"response"})
