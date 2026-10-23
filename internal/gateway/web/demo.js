"use strict";
const byId = id => document.getElementById(id);
let controller;
byId("switch-adapter").addEventListener("click", () => {
  const other = byId("model").value === "anthropic-chat" ? "default-chat" : "anthropic-chat";
  byId("model").value = other;
  byId("switch-adapter").textContent = other === "anthropic-chat" ? "default-chat" : "anthropic-chat";
});
byId("stop").addEventListener("click", () => controller?.abort());
byId("request-form").addEventListener("submit", async event => {
  event.preventDefault();
  if (controller) return;
  controller = new AbortController();
  const started = performance.now();
  byId("run").disabled = true;
  byId("stop").disabled = false;
  byId("empty-state").hidden = true;
  byId("output").hidden = false;
  byId("output").textContent = "";
  byId("error").hidden = true;
  byId("error").textContent = "";
  byId("status").textContent = "Connecting";
  byId("status").dataset.state = "running";
  for (const id of ["provider", "tokens", "request-id", "elapsed"]) byId(id).textContent = "—";
  let reader;
  try {
    const response = await fetch("/v1/chat/completions", {
      method: "POST",
      headers: {"Content-Type": "application/json", Authorization: `Bearer ${byId("api-key").value}`},
      body: JSON.stringify({model: byId("model").value, messages: [{role: "user", content: byId("prompt").value}], stream: true, stream_options: {include_usage: true}}),
      signal: controller.signal,
    });
    byId("request-id").textContent = response.headers.get("X-Request-ID") || "—";
    if (!response.ok) {
      const payload = await response.json();
      throw new Error(`${response.status}: ${payload.error?.message || "Request failed"}`);
    }
    byId("provider").textContent = response.headers.get("X-Gateway-Provider") || "—";
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
        for (const choice of chunk.choices || []) byId("output").textContent += choice.delta?.content || "";
        if (chunk.usage) byId("tokens").textContent = chunk.usage.total_tokens;
      }
    }
    if (!done) throw new Error("The stream ended before successful completion.");
    byId("status").textContent = "Complete";
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
    byId("run").disabled = false;
    byId("stop").disabled = true;
  }
});
