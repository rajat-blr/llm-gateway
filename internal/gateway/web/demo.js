"use strict";
const byId = id => document.getElementById(id);
const exampleButtons = [...document.querySelectorAll("[data-example]")];
const examples = {
  openai: {model: "default-chat", stream: true, prompt: "Hello from the browser", description: "Stream text through the OpenAI adapter, followed by final token usage."},
  stop: {model: "default-chat", prompt: "Say hello, then write END.", options: {max_tokens: 32, stop: ["END"]}, description: "Stop generation before END appears. The stop sequence is passed to OpenAI; fixture output stays fixed."},
  json: {model: "default-chat", prompt: "Give me a short greeting", options: {max_tokens: 16, temperature: 0.2}, description: "Receive one JSON completion with a 16-token output bound and temperature 0.2. Expand the full response to inspect usage and finish reason."},
  system: {model: "default-chat", prompt: "Explain what an API gateway does", history: [{role: "system", content: "You are a concise assistant. Answer in one sentence."}], description: "Send system instructions and a user message through OpenAI. Inspect the request to see both roles. Fixture mode returns fixed text."},
  conversation: {model: "default-chat", prompt: "Which city did I just mention?", history: [{role: "user", content: "I am planning a visit to Bengaluru."}, {role: "assistant", content: "What would you like to know about Bengaluru?"}], description: "Send a three-message conversation in one request. History travels with the request; the gateway does not store conversation state. Fixture text remains deterministic."},
  models: {method: "GET", description: "List aliases available to your gateway key. This authenticated discovery request does not call a provider."},
  unsupported: {model: "default-chat", prompt: "What is the weather?", options: {tools: []}, expected: {status: 400, code: "invalid_request"}, description: "Try an unsupported tools field. Expect HTTP 400: this gateway supports text completions and rejects unsupported fields explicitly."},
  unavailable: {model: "unavailable-demo-alias", prompt: "Hello", expected: {status: 403, code: "model_not_allowed"}, description: "Request an unavailable alias. Expect HTTP 403 before any provider call. If this alias exists in your own configuration, the result may differ."},
};
let selected = "openai";
let controller;

function requestSpec() {
  const example = examples[selected];
  if (example.method === "GET") return {method: "GET", path: "/v1/models"};
  const body = {
    model: byId("model").value,
    messages: [...(example.history || []), {role: "user", content: byId("prompt").value}],
    stream: !!example.stream,
    ...(example.options || {}),
  };
  if (example.stream) body.stream_options = {include_usage: true};
  return {method: "POST", path: "/v1/chat/completions", body};
}
function refreshRequest() {
  const spec = requestSpec();
  byId("endpoint").textContent = `${spec.method} ${spec.path}`;
  byId("request-preview").textContent = spec.body ? JSON.stringify(spec.body, null, 2) : "GET /v1/models\nNo request body.";
  const discovery = spec.method === "GET";
  for (const id of ["model", "prompt"]) byId(id).disabled = !!controller || discovery;
  byId("run").textContent = discovery ? "List models ↗" : examples[selected].expected ? "Try rejection ↗" : examples[selected].stream ? "Run stream ↗" : "Send request ↗";
}
function selectExample(name) {
  if (controller) return;
  selected = name;
  const example = examples[name];
  if (example.model) byId("model").value = example.model;
  if (example.prompt) byId("prompt").value = example.prompt;
  for (const button of exampleButtons) button.setAttribute("aria-pressed", String(button.dataset.example === name));
  byId("example-description").textContent = example.description;
  byId("status").textContent = "Ready";
  delete byId("status").dataset.state;
  byId("empty-state").hidden = false;
  byId("output").hidden = true;
  byId("output").textContent = "";
  byId("error").hidden = true;
  byId("error").textContent = "";
  byId("response-details").hidden = true;
  byId("response-json").textContent = "";
  for (const id of ["provider", "tokens", "request-id", "elapsed", "http-status", "cache", "first-text"]) byId(id).textContent = "—";
  refreshRequest();
}
function setBusy(busy) {
  byId("run").disabled = busy;
  byId("stop").disabled = !busy;
  byId("api-key").disabled = busy;
  for (const button of exampleButtons) button.disabled = busy;
  refreshRequest();
}
for (const button of exampleButtons) button.addEventListener("click", () => selectExample(button.dataset.example));
for (const id of ["model", "prompt"]) byId(id).addEventListener("input", refreshRequest);
byId("stop").addEventListener("click", () => controller?.abort());
byId("request-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (controller) return;
  const spec = requestSpec();
  const example = examples[selected];
  controller = new AbortController();
  const started = performance.now();
  setBusy(true);
  byId("empty-state").hidden = true;
  byId("output").hidden = false;
  byId("output").textContent = "";
  byId("error").hidden = true;
  byId("error").textContent = "";
  byId("response-details").hidden = true;
  byId("response-json").textContent = "";
  byId("status").textContent = "Connecting";
  byId("status").dataset.state = "running";
  for (const id of ["provider", "tokens", "request-id", "elapsed", "http-status", "cache", "first-text"]) byId(id).textContent = "—";
  let reader;
  try {
    const response = await fetch(spec.path, {
      method: spec.method,
      headers: {Authorization: `Bearer ${byId("api-key").value}`, ...(spec.body ? {"Content-Type": "application/json"} : {})},
      ...(spec.body ? {body: JSON.stringify(spec.body)} : {}),
      signal: controller.signal,
    });
    byId("http-status").textContent = response.status;
    byId("request-id").textContent = response.headers.get("X-Request-ID") || "—";
    byId("provider").textContent = response.headers.get("X-Gateway-Provider") || "—";
    byId("cache").textContent = response.headers.get("X-Cache") || "—";
    if (!response.ok || !example.stream) {
      const payload = await response.json();
      byId("response-json").textContent = JSON.stringify(payload, null, 2);
      byId("response-details").hidden = false;
      if (example.expected && response.status === example.expected.status && payload.error?.code === example.expected.code) {
        byId("output").textContent = JSON.stringify(payload, null, 2);
        byId("status").textContent = "Expected rejection";
      } else {
        if (!response.ok) throw new Error(`${response.status}: ${payload.error?.message || "Request failed"}`);
        if (example.expected) throw new Error(`Expected HTTP ${example.expected.status}, but the request succeeded. Check your alias and example settings.`);
        byId("output").textContent = spec.method === "GET" ? JSON.stringify(payload, null, 2) : payload.choices?.[0]?.message?.content || "";
        if (payload.usage) byId("tokens").textContent = payload.usage.total_tokens;
        byId("status").textContent = "Complete";
      }
    } else {
      byId("status").textContent = "Streaming";
      reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "", done = false;
      while (!done) {
        const result = await reader.read();
        if (result.done) break;
        buffer += decoder.decode(result.value, {stream: true});
        let boundary;
        while ((boundary = buffer.indexOf("\n\n")) !== -1) {
          const block = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const data = block.split("\n").filter(line => line.startsWith("data: ")).map(line => line.slice(6)).join("\n");
          if (!data) continue;
          if (data === "[DONE]") {done = true; break;}
          const chunk = JSON.parse(data);
          if (chunk.error) throw new Error(chunk.error.message || "The stream failed.");
          for (const choice of chunk.choices || []) {
            const text = choice.delta?.content || "";
            if (text && byId("first-text").textContent === "—") byId("first-text").textContent = `${Math.round(performance.now() - started)} ms`;
            byId("output").textContent += text;
          }
          if (chunk.usage) byId("tokens").textContent = chunk.usage.total_tokens;
        }
      }
      if (!done) throw new Error("The stream ended before successful completion.");
      byId("status").textContent = "Complete";
    }
    byId("status").dataset.state = "success";
  } catch (error) {
    const stopped = error.name === "AbortError";
    byId("status").textContent = stopped ? "Stopped" : "Failed";
    byId("status").dataset.state = stopped ? "stopped" : "error";
    if (!stopped) {
      byId("error").textContent = error.message;
      byId("error").hidden = false;
    }
  } finally {
    if (reader) await reader.cancel().catch(() => {});
    byId("elapsed").textContent = `${Math.round(performance.now() - started)} ms`;
    controller = undefined;
    setBusy(false);
  }
});
refreshRequest();
