# Migration guide: v3.0.0

Version 3.0.0 transitions to `anyio` instead of using `asyncio` directly.

This means

- use the `anyio.run` wrapper, your choice of back-end
- publish and subscribe/unsubscribe methods no longer have a `timeout` argument.

## Use `anyio.run`

The preferred way to start an async program using anyio is to use
`anyio.run(main, backend='asyncio')`.

`anyio.run(main, backend='trio')` is generally a bit slower than asyncio;
this is offset by the fact that anyio needs to do much less work to
provide an environment where error propagation and task cancellation
Just Work.

Also, trio affords better error messages and introspection.

For the underlying philosophy behind Trio, see
https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/ .

## No more timeout arguments

The `publish`, `subscribe` and `unsubscribe` methods no longer have a `timeout` argument.

If you do need to use a timeout on any of these commands, you can use asyncio's 
`wait_for` wrapper, or (even easier) anyio's context manager:

```python
import anyio
import aiomqtt

async def main():
    async with aiomqtt.Client("test.mosquitto.org") as client:
        with anyio.fail_after(3.5):
            await client.publish("temperature/outside", payload=28.4)

anyio.run(main)
