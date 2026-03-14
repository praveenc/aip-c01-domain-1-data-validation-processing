"""Shared utility for constructing Amazon Bedrock invoke_model request payloads.

Produces correctly structured request dicts for the Bedrock Runtime
``invoke_model`` API, with the ``body`` field serialized as a JSON string
(as required by the API).

Usage::

    from shared.utils.bedrock_helpers import build_bedrock_messages_payload

    payload = build_bedrock_messages_payload(
        messages=[{"role": "user", "content": "Hello"}],
        system="You are a helpful assistant.",
        model_id="anthropic.claude-3-sonnet-20240229-v1:0",
        max_tokens=2048,
        temperature=0.1,
        top_p=0.9,
    )

    # In production with boto3:
    # client = boto3.client("bedrock-runtime")
    # response = client.invoke_model(**payload)
"""

import json
from typing import Any


def build_bedrock_messages_payload(
    messages: list[dict[str, Any]],
    system: str = "",
    model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0",
    max_tokens: int = 2048,
    temperature: float = 0.7,
    top_p: float = 0.9,
    anthropic_version: str = "bedrock-2023-05-31",
) -> dict[str, Any]:
    """Build a complete Bedrock ``invoke_model`` request payload.

    Parameters
    ----------
    messages : list[dict]
        List of message dicts, each with ``role`` (``"user"`` or
        ``"assistant"``) and ``content`` (str) keys.
    system : str, optional
        System prompt that sets the assistant's behaviour.
    model_id : str, optional
        Bedrock model identifier (default: Claude 3 Sonnet).
    max_tokens : int, optional
        Maximum number of tokens in the response.
    temperature : float, optional
        Sampling temperature (0.0–1.0).  Lower values produce more
        deterministic output.
    top_p : float, optional
        Nucleus sampling parameter (0.0–1.0).
    anthropic_version : str, optional
        Anthropic API version string required by Bedrock.

    Returns
    -------
    dict
        A dict with ``modelId``, ``contentType``, ``accept``, and ``body``
        keys.  The ``body`` value is a JSON-serialized string ready for
        ``bedrock-runtime.invoke_model()``.

    Example
    -------
    >>> payload = build_bedrock_messages_payload(
    ...     messages=[{"role": "user", "content": "Summarize this record."}],
    ...     system="You are a clinical assistant.",
    ...     temperature=0.1,
    ... )
    >>> import json
    >>> body = json.loads(payload["body"])
    >>> body["messages"][0]["role"]
    'user'
    """
    body = {
        "anthropic_version": anthropic_version,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": top_p,
        "messages": messages,
    }

    if system:
        body["system"] = system

    return {
        "modelId": model_id,
        "contentType": "application/json",
        "accept": "application/json",
        "body": json.dumps(body),
    }
