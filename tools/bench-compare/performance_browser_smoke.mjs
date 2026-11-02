import { writeFile } from 'node:fs/promises';
const page = await (await fetch('http://127.0.0.1:9223/json/new?about:blank', { method: 'PUT' })).json();
const socket = new WebSocket(page.webSocketDebuggerUrl);
await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject; });
let next = 0;
const pending = new Map();
const errors = [];
socket.onmessage = ({data}) => {
  const message = JSON.parse(data);
  if (message.method === 'Runtime.exceptionThrown') errors.push(message.params.exceptionDetails);
  if (message.id) {
    const item = pending.get(message.id);
    pending.delete(message.id);
    if (message.error) item.reject(message.error); else item.resolve(message.result);
  }
};
function call(method, params = {}) {
  const id = ++next;
  return new Promise((resolve, reject) => {
    pending.set(id, {resolve, reject});
    socket.send(JSON.stringify({id, method, params}));
  });
}
async function evaluate(expression) {
  const response = await call('Runtime.evaluate', {expression, returnByValue:true, awaitPromise:true});
  if (response.exceptionDetails) throw new Error(JSON.stringify(response.exceptionDetails));
  return response.result.value;
}
async function waitFor(expression) {
  const start = Date.now();
  while (Date.now() - start < 30000) {
    if (await evaluate(`Boolean(${expression})`)) return;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out: ${expression}; body=${await evaluate('document.body.innerText')}`);
}
try {
  await call('Runtime.enable');
  await call('Page.enable');
  await call('Page.addScriptToEvaluateOnNewDocument', {source: `
    window.__sqlWorkerChecks = [];
    const NativeWorker = window.Worker;
    window.Worker = class extends NativeWorker {
      constructor(...args) {
        super(...args);
        const check = {url: String(args[0]), replies: 0};
        window.__sqlWorkerChecks.push(check);
        this.addEventListener('message', () => check.replies++);
      }
    };
  `});
  await call('Page.navigate', {url:'http://127.0.0.1:4174/transpile'});
  await waitFor("document.body.innerText.includes('0.13.5') && document.querySelector('.cm-content')");
  const initialEditors = await evaluate("document.querySelectorAll('.cm-editor').length");
  await evaluate("[...document.querySelectorAll('button')].find(b => b.textContent.trim() === 'Transpile' && b.getAttribute('role') !== 'tab').click()");
  await waitFor("[...document.querySelectorAll('.cm-content')].some(e => e.getAttribute('contenteditable') === 'false' && e.textContent.includes('SELECT'))");
  const transpileOutput = await evaluate("[...document.querySelectorAll('.cm-content')].filter(e => e.getAttribute('contenteditable') === 'false').map(e => e.textContent)");
  await evaluate("[...document.querySelectorAll('[role=tab]')].find(b => b.textContent.includes('Formatter')).dispatchEvent(new MouseEvent('mousedown', {bubbles:true,button:0}))");
  await waitFor("location.pathname === '/format' && document.querySelectorAll('.cm-editor').length > " + initialEditors);
  await evaluate("[...document.querySelectorAll('button')].find(b => b.textContent.trim() === 'Format').click()");
  await waitFor("[...document.querySelectorAll('[role=tabpanel][data-state=active] .cm-content')].some(e => e.getAttribute('contenteditable') === 'false' && e.textContent.includes('SELECT'))");
  await evaluate("[...document.querySelectorAll('[role=tab]')].find(b => b.textContent.trim() === 'Transpile').dispatchEvent(new MouseEvent('mousedown', {bubbles:true,button:0}))");
  await waitFor("location.pathname === '/transpile'");
  const retained = await evaluate("[...document.querySelectorAll('[role=tabpanel][data-state=active] .cm-content')].filter(e => e.getAttribute('contenteditable') === 'false').map(e => e.textContent)");
  const workers = await evaluate('window.__sqlWorkerChecks');
  if (JSON.stringify(retained) !== JSON.stringify(transpileOutput)) throw new Error('Tab output was not retained');
  if (!workers.some(worker => worker.url.includes('sql.worker') && worker.replies >= 3)) throw new Error('SQL worker was not created');
  if (errors.length) throw new Error(JSON.stringify(errors));
  const result = {initialEditors, workers, transpileOutput, retained, runtimeErrors:errors, passed:true};
  await writeFile('tools/bench-compare/results/performance-review/browser-smoke.json', JSON.stringify(result,null,2)+'\n');
  console.log(JSON.stringify(result));
} finally {
  socket.close();
}
