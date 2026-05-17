"""Azure OpenAI client wrapper."""

from __future__ import annotations

import json

from openai import AzureOpenAI


def build_client(endpoint: str, api_key: str, api_version: str = "2024-10-21") -> AzureOpenAI:
    """Construct an AzureOpenAI client."""
    return AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version=api_version,
    )


def chat_with_tools(
    client: AzureOpenAI,
    deployment_name: str,
    messages: list[dict],
    tool_schemas: list[dict],
    tool_registry: dict,
    connection: dict,
    max_iters: int = 4,
) -> tuple[str, list[dict]]:
    """Run a chat completion with iterative tool calling.

    Returns:
      (final_assistant_text, tool_call_log)
        - final_assistant_text: the natural-language reply for the user
        - tool_call_log: list of {"name": str, "args": dict, "rows": int} records
          for transparency in the UI
    """
    tool_call_log = []

    for _ in range(max_iters):
        response = client.chat.completions.create(
            model=deployment_name,
            messages=messages,
            tools=tool_schemas,
            tool_choice="auto",
            temperature=0.2,
        )

        msg = response.choices[0].message

        # If the LLM didn't call a tool, we have the final answer
        if not msg.tool_calls:
            return msg.content or "", tool_call_log

        # Append the assistant message that requested tools
        messages.append(
            {
                "role": "assistant",
                "content": msg.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in msg.tool_calls
                ],
            }
        )

        # Execute each tool call and append the result
        for tc in msg.tool_calls:
            name = tc.function.name
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}

            if name not in tool_registry:
                result_text = f"Error: unknown tool {name}"
                tool_call_log.append({"name": name, "args": args, "rows": 0, "error": result_text})
            else:
                try:
                    df = tool_registry[name](connection=connection, **args)
                    result_text = df.to_string(index=False) if not df.empty else "(no rows)"
                    tool_call_log.append({"name": name, "args": args, "rows": len(df)})
                except Exception as e:
                    result_text = f"Error executing {name}: {e}"
                    tool_call_log.append({"name": name, "args": args, "rows": 0, "error": str(e)})

            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result_text,
                }
            )

    # Hit max iterations without a final answer
    return "(Agent reached max tool-call iterations without a final answer.)", tool_call_log
