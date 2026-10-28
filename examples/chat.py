"""Run after `make demo` and installing examples/requirements.txt."""
import os
from openai import OpenAI

client = OpenAI(
    api_key=os.environ.get("GATEWAY_API_KEY", "demo-local-key"),
    base_url=os.environ.get("GATEWAY_BASE_URL", "http://localhost:8080/v1"),
    max_retries=0,
)
for alias in ("default-chat",):
    messages = [{"role": "user", "content": "Hello from the SDK"}]
    response = client.chat.completions.create(
        model=alias, messages=messages, max_tokens=32, temperature=0.5, stop=["END"]
    )
    print(alias, response.choices[0].message.content, response.usage)
    with client.chat.completions.create(
        model=alias, messages=messages, max_tokens=32, stream=True,
        stream_options={"include_usage": True},
    ) as stream:
        for chunk in stream:
            if chunk.choices:
                print(chunk.choices[0].delta.content or "", end="", flush=True)
            if chunk.usage:
                print("\nusage:", chunk.usage)
client.close()
