"""Azure managed identity auth for aiokafka -> Event Hubs.

Event Hubs Standard tier exposes a Kafka 1.0+ surface at port 9093 with
SASL_SSL + OAUTHBEARER. This module wires aiokafka's bearer token hook
to Azure Identity so producers authenticate with a managed identity (in
Container Apps) or `az login` creds (in local dev) — no connection strings
or access keys anywhere.

Hard-won lessons baked into this module (DO NOT change without testing):
  1. Token scope must be `https://{namespace}.servicebus.windows.net/.default`,
     NOT a generic `eventhubs.azure.net` scope — Event Hubs validates the
     audience claim against the namespace FQDN.
  2. `AbstractTokenProvider` is a class with an async `token()` method.
     Returning a plain coroutine or callable will not work.
  3. `ssl_context` is a required arg when `security_protocol="SASL_SSL"`;
     aiokafka does NOT default-construct it.
"""

from __future__ import annotations

import asyncio
import ssl

from aiokafka import AIOKafkaProducer
from aiokafka.abc import AbstractTokenProvider
from azure.identity import DefaultAzureCredential


class AzureADTokenProvider(AbstractTokenProvider):
    """aiokafka OAuth bearer token provider backed by Azure managed identity.

    aiokafka requires an async `token()` returning a str. The Azure Identity
    SDK's sync `get_token()` is the most reliable variant (the .aio one has
    known compatibility issues in some environments), so we wrap it in
    `run_in_executor` to keep the event loop unblocked.
    """

    def __init__(
        self,
        credential: DefaultAzureCredential,
        namespace: str,
    ) -> None:
        self._credential = credential
        self._scope = f"https://{namespace}.servicebus.windows.net/.default"

    def _get_token_sync(self) -> str:
        access_token = self._credential.get_token(self._scope)
        return access_token.token

    async def token(self) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._get_token_sync)


async def make_producer(
    credential: DefaultAzureCredential,
    namespace: str,
) -> AIOKafkaProducer:
    """Construct an aiokafka producer wired for Event Hubs.

    Args:
        credential: Azure Identity credential (typically DefaultAzureCredential).
        namespace: Event Hubs namespace name without FQDN suffix,
            e.g. "evhns-gridsense-dev" not "evhns-gridsense-dev.servicebus.windows.net".

    Caller is responsible for `await producer.start()` ... `await producer.stop()`.
    Actually, no — we start it here for convenience. Callers just need to stop it.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=f"{namespace}.servicebus.windows.net:9093",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=AzureADTokenProvider(credential, namespace),
        ssl_context=ssl.create_default_context(),
        linger_ms=200,
        acks="all",
        request_timeout_ms=30000,
    )
    await producer.start()
    return producer
