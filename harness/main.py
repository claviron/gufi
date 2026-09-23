"""The application the runner boots when the crate is being exercised.

Small on purpose. What it has to cover is the runner's contract, not FastAPI:
a lifespan that runs on the way up and the way down, and one route that proves
a request reached the interpreter and a response came back. Anything more
would be testing the framework instead.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI

state: dict[str, object] = {"started": False}


@asynccontextmanager
async def lifespan(_: FastAPI):
    # The runner drives ASGI lifespan itself, so both halves of this are part
    # of what is under test: startup must complete before a request is
    # accepted, and shutdown must run before the process leaves.
    state["started"] = True
    yield
    state["started"] = False


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, object]:
    return {"runner": "python", "started": state["started"]}


@app.post("/echo")
async def echo(body: dict[str, object]) -> dict[str, object]:
    # A body in and the same body out: the request path carries more than a
    # method and a URL.
    return {"echo": body}
