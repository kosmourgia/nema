/* Session storage survives worker termination; browser restart starts a session. */
importScripts("config.js");
const config = globalThis.NEMA_CONFIG;
let state, timer;
let serial = Promise.resolve();
let polling = false;
function atomic(fn) {
  const result = serial.then(fn);
  serial = result.catch(error => console.warn("Nema companion:", String(error)));
  return result;
}
async function boot() {
  if (state) return;
  let {installation} = await chrome.storage.local.get("installation");
  if (!installation) { installation = crypto.randomUUID(); await chrome.storage.local.set({installation}); }
  const stored = await chrome.storage.session.get("state");
  state = stored.state || {installation, session: crypto.randomUUID(), tabs: {}, results: {}, seen: {}};
  state.installation = installation;
  for (const [id, seen] of Object.entries(state.seen)) {
    if (seen === "started" && !state.results[id]) state.results[id] = {
      invocationId: id, status: "outcome-unknown", value: {code: "worker-restarted-during-command"}};
  }
  await save();
}
function save() { return chrome.storage.session.set({state}); }
function selected(url) {
  try { const parsed = new URL(url); return parsed.origin === config.origin && parsed.pathname === "/demo"; }
  catch (_) { return false; }
}
async function remember(message, sender) {
  await boot();
  if (sender.id !== chrome.runtime.id || !sender.tab || sender.frameId !== 0 || !selected(sender.url)) return;
  const key = String(sender.tab.id);
  if (message.type === "document.hello") {
    if (!message.document || typeof message.document.id !== "string" || !sender.documentId) return;
    const frame = await chrome.webNavigation.getFrame({tabId: sender.tab.id, frameId: 0});
    if (!frame || frame.documentId !== sender.documentId) return;
    if (!state.tabs[key] && Object.keys(state.tabs).length >= 32) return;
    state.tabs[key] = {key: state.tabs[key]?.key || crypto.randomUUID(), tabId: sender.tab.id, windowId: sender.tab.windowId,
                       document: {...message.document, url: sender.url, nativeDocumentId: sender.documentId}};
  } else if (message.type === "document.detach" && state.tabs[key]?.document?.id === message.documentId) {
    state.tabs[key].document = null;
  }
  await save();
}
async function execute(command) {
  if (state.seen[command.invocationId]) return;
  state.seen[command.invocationId] = "started";
  await save();
  let result, dispatched = false;
  try {
    const tab = state.tabs[String(command.binding.nativeTabId)];
    if (!tab) throw new Error("stale-tab");
    if (command.operation === "browser.focus") {
      if (command.target.incarnation !== tab.key) throw new Error("stale-tab");
      dispatched = true;
      await chrome.tabs.update(tab.tabId, {active: true});
      await chrome.windows.update(tab.windowId, {focused: true});
      result = {status: "completed", value: {tabId: tab.tabId, requestedFocus: true}};
    } else {
      if (!tab.document || tab.document.id !== command.target.incarnation ||
          tab.document.nativeDocumentId !== command.binding.nativeDocumentId) throw new Error("stale-document");
      dispatched = true;
      result = await Promise.race([
        chrome.tabs.sendMessage(tab.tabId, {type: "document.invoke", command}, {documentId: command.binding.nativeDocumentId}),
        new Promise((_, reject) => setTimeout(() => reject(new Error("document-response-timeout")), 2000))]);
      if (!result || typeof result.status !== "string") throw new Error("missing-document-result");
    }
  } catch (error) { result = {status: dispatched ? "outcome-unknown" : "failed", value: {code: String(error.message || error)}}; }
  state.results[command.invocationId] = {invocationId: command.invocationId, ...result};
  state.seen[command.invocationId] = "terminal";
  for (const id of Object.keys(state.seen).slice(0, -128)) if (!state.results[id]) delete state.seen[id];
  await save();
}
async function poll() {
  if (polling) return;
  polling = true;
  clearTimeout(timer);
  try {
    await atomic(async () => {
      await boot();
      const liveIds = new Set((await chrome.tabs.query({})).map(tab => String(tab.id)));
      for (const id of Object.keys(state.tabs)) if (!liveIds.has(id)) delete state.tabs[id];
      const payload = {version: 1, installation: state.installation, session: state.session,
                       extensionId: chrome.runtime.id, tabs: Object.values(state.tabs), results: []};
      for (const result of Object.values(state.results)) {
        if (new TextEncoder().encode(JSON.stringify({...payload, results: [...payload.results, result]})).length > 60000) break;
        payload.results.push(result);
      }
      const response = await fetch(config.origin + "/exchange", {
        method: "POST", headers: {"Authorization": "Bearer " + config.token, "Content-Type": "application/json"},
        body: JSON.stringify(payload), signal: AbortSignal.timeout(2500), cache: "no-store"});
      if (!response.ok) throw new Error("bridge status " + response.status);
      const message = await response.json();
      for (const id of message.acknowledged) delete state.results[id];
      await save();
      await Promise.all(message.commands.map(execute));
    });
  } catch (error) { console.warn("Nema transport unavailable:", String(error)); }
  finally {
    polling = false;
    if (state && Object.keys(state.tabs).length) timer = setTimeout(poll, 250);
  }
}
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (!["document.hello", "document.detach"].includes(message?.type)) return;
  atomic(() => remember(message, sender)).then(() => { sendResponse({ok: true}); poll(); },
    error => sendResponse({ok: false, error: String(error)}));
  return true;
});
chrome.tabs.onRemoved.addListener(tabId => { atomic(async () => {
  await boot(); delete state.tabs[String(tabId)]; await save(); }).then(poll); });
chrome.webNavigation.onCommitted.addListener(details => {
  if (details.frameId !== 0) return;
  atomic(async () => { await boot(); const tab = state.tabs[String(details.tabId)];
    if (tab && tab.document?.nativeDocumentId !== details.documentId) { tab.document = null; await save(); }
  }).then(poll);
});
chrome.alarms.onAlarm.addListener(alarm => { if (alarm.name === "nema-reconnect") poll(); });
chrome.runtime.onStartup.addListener(poll);
chrome.runtime.onInstalled.addListener(() => { chrome.alarms.create("nema-reconnect", {periodInMinutes: 0.5}); poll(); });
poll();
