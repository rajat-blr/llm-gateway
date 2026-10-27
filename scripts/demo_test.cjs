// Execute the shipped client with a small DOM substitute. This validates request
// construction and response handling; it does not claim browser/layout coverage.
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'internal/gateway/web/index.html'), 'utf8');
const source = fs.readFileSync(path.join(root, 'internal/gateway/web/demo.js'), 'utf8');

function client(fetchImpl) {
  const elements = new Map();
  function element(id) {
    return {value: '', textContent: '', hidden: false, disabled: false, dataset: {}, attributes: {}, listeners: {},
      setAttribute(name, value) { this.attributes[name] = value; },
      addEventListener(name, fn) { this.listeners[name] = fn; },
      async fire(name) { return this.listeners[name]?.({preventDefault() {}}); }, id};
  }
  for (const match of html.matchAll(/\bid="([^"]+)"/g)) {
    assert(!elements.has(match[1]), `duplicate id ${match[1]}`);
    elements.set(match[1], element(match[1]));
  }
  const buttons = [...html.matchAll(/data-example="([^"]+)"/g)].map(match => {
    const button = element(match[1]);
    button.dataset.example = match[1];
    return button;
  });
  elements.get('model').value = 'default-chat';
  elements.get('api-key').value = 'demo-local-key';
  elements.get('prompt').value = 'Hello from the browser';
  const calls = [];
  const context = vm.createContext({document: {
    getElementById(id) { assert(elements.has(id), `missing DOM element ${id}`); return elements.get(id); },
    querySelectorAll(selector) { assert.equal(selector, '[data-example]'); return buttons; },
  }, TextDecoder, AbortController, performance, fetch: async (url, options) => {
    calls.push({url, options});
    return fetchImpl(url, options);
  }});
  vm.runInContext(source, context, {filename: 'demo.js'});
  return {elements, buttons, calls,
    select: async name => {const button = buttons.find(button => button.dataset.example === name); assert(button); await button.fire('click');},
    submit: () => elements.get('request-form').fire('submit'),
    text: id => elements.get(id).textContent,
  };
}
function jsonResponse(value, status = 200) {
  return new Response(JSON.stringify(value), {status, headers: {'Content-Type': 'application/json'}});
}
function eventResponse(text, complete = true) {
  const events = [{choices: [{delta: {role: 'assistant'}}]}, {choices: [{delta: {content: text}}]}, {choices: [], usage: {total_tokens: 8}}];
  const data = events.map(event => `data: ${JSON.stringify(event)}\n\n`).join('') + (complete ? 'data: [DONE]\n\n' : '');
  const bytes = new TextEncoder().encode(data);
  // Split both UTF-8 code points and event delimiters across network reads.
  return new Response(new ReadableStream({start(controller) {for (const byte of bytes) controller.enqueue(Uint8Array.of(byte)); controller.close();}}), {headers: {'X-Gateway-Provider': 'fixture', 'X-Cache': 'BYPASS'}});
}

test('all example requests and edited previews use the shipped form', async () => {
  const app = client(async () => jsonResponse({}));
  assert.equal(app.buttons.length, 8);
  for (const button of app.buttons) {
    const name = button.dataset.example;
    await app.select(name);
    assert.equal(button.attributes['aria-pressed'], 'true');
    if (name === 'models') {
      assert.equal(app.text('endpoint'), 'GET /v1/models');
      assert(app.elements.get('prompt').disabled);
      continue;
    }
    const preview = JSON.parse(app.text('request-preview'));
    assert.equal(preview.model, app.elements.get('model').value);
    assert.equal(preview.messages.at(-1).content, app.elements.get('prompt').value);
    if (name === 'conversation') assert.deepEqual(preview.messages.map(message => message.role), ['user', 'assistant', 'user']);
    if (name === 'system') assert.equal(preview.messages[0].role, 'system');
    if (name === 'json') assert.equal(preview.max_tokens, 16);
    if (name === 'unsupported') assert.deepEqual(preview.tools, []);
    assert(!app.text('request-preview').includes('demo-local-key'));
  }
  await app.select('openai');
  app.elements.get('prompt').value = 'Edited message';
  await app.elements.get('prompt').fire('input');
  assert.equal(JSON.parse(app.text('request-preview')).messages[0].content, 'Edited message');
});

test('SSE handles split UTF-8, usage and completion', async () => {
  const app = client(async () => eventResponse('héllo'));
  await app.submit();
  assert.equal(app.text('output'), 'héllo');
  assert.equal(app.text('status'), 'Complete');
  assert.equal(app.text('tokens'), 8);
  assert.match(app.text('first-text'), /\d+ ms/);
  assert.equal(app.elements.get('run').disabled, false);
  await app.select('json');
  assert.equal(app.text('status'), 'Ready');
  assert.equal(app.text('output'), '');
  assert(app.elements.get('output').hidden);
});

test('an incomplete stream is a failure', async () => {
  const app = client(async () => eventResponse('partial', false));
  await app.submit();
  assert.equal(app.text('status'), 'Failed');
  assert.match(app.text('error'), /before successful completion/);
});

test('expected rejection requires both matching HTTP status and error code', async () => {
  for (const [status, code, expected] of [[400, 'invalid_request', true], [401, 'invalid_api_key', false], [403, 'model_not_allowed', false]]) {
    const app = client(async () => jsonResponse({error: {code, message: code}}, status));
    await app.select('unsupported');
    await app.submit();
    assert.equal(app.text('status'), expected ? 'Expected rejection' : 'Failed');
    assert.equal(app.elements.get('error').hidden, expected);
  }
  const app = client(async () => jsonResponse({choices: [{message: {content: 'unexpected'}}]}));
  await app.select('unavailable');
  await app.submit();
  assert.equal(app.text('status'), 'Failed');
});

test('Stop aborts the request and restores the form', async () => {
  let entered;
  const ready = new Promise(resolve => {entered = resolve;});
  const app = client(async (_, options) => new Promise((_, reject) => {
    options.signal.addEventListener('abort', () => reject(new DOMException('Stopped', 'AbortError')));
    entered();
  }));
  const pending = app.submit();
  await ready;
  assert(app.buttons.every(button => button.disabled));
  assert(app.elements.get('api-key').disabled);
  await app.elements.get('stop').fire('click');
  await pending;
  assert.equal(app.text('status'), 'Stopped');
  assert.equal(app.elements.get('run').disabled, false);
  assert.equal(app.elements.get('stop').disabled, true);
});

test('real gateway runs every demo example', {skip: !process.env.DEMO_BASE_URL}, async t => {
  assert.equal(await (await fetch(new URL('/demo.js', process.env.DEMO_BASE_URL))).text(), source, 'deployed script differs from the tested client');
  assert.equal(await (await fetch(new URL('/', process.env.DEMO_BASE_URL))).text(), html, 'deployed page differs from the tested form');
  const app = client((url, options) => fetch(new URL(url, process.env.DEMO_BASE_URL), options));
  app.elements.get('api-key').value = process.env.GATEWAY_API_KEY || 'demo-local-key';
  for (const button of app.buttons) {
    await t.test(button.dataset.example, async () => {
      await app.select(button.dataset.example);
      await app.submit();
      const rejects = ['unsupported', 'unavailable'].includes(button.dataset.example);
      assert.equal(app.text('status'), rejects ? 'Expected rejection' : 'Complete', app.text('error'));
      if (button.dataset.example === 'models') {
        assert(app.calls.at(-1).options.body === undefined);
        const payload = JSON.parse(app.text('output'));
        assert(payload.data.some(model => model.id === 'default-chat'));
      } else if (!rejects) {
        assert.equal(app.text('output'), 'hello hello hello ');
        assert.equal(app.text('tokens'), 8);
      }
    });
  }
});
