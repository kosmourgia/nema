/* The page agent receives no bearer secret and controls only one DOM region. */
(() => {
  if (location.origin !== globalThis.NEMA_PAGE.origin || location.pathname !== "/demo") return;
  const region = document.getElementById("nema-demo-region");
  if (!region) return;
  let documentId = crypto.randomUUID();
  const results = new Map();
  let attached = true;
  async function hello() {
    if (!attached) return;
    try {
      await chrome.runtime.sendMessage({type: "document.hello", document: {id: documentId, url: location.href, region: "nema-demo-region"}});
      document.getElementById("nema-zygon-status").textContent = "Companion attached · document " + documentId;
    } catch (_) { /* Transport loss does not recreate the document. */ }
  }
  chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
    if (sender.id !== chrome.runtime.id || message.type !== "document.invoke") return;
    const command = message.command;
    if (!attached || command.target.incarnation !== documentId) {
      sendResponse({status: "failed", value: {code: "stale-document"}}); return;
    }
    if (results.has(command.invocationId)) { sendResponse(results.get(command.invocationId)); return; }
    let result;
    const value = command.arguments?.value;
    if (["browser.annotate", "browser.replace"].includes(command.operation) &&
        (typeof value !== "string" || new TextEncoder().encode(value).length > 8192)) {
      result = {status: "failed", value: {code: "bounded-string-required"}};
    } else {
      switch (command.operation) {
        case "browser.read": break;
        case "browser.annotate": region.dataset.nemaAnnotation = value; region.title = value; break;
        case "browser.replace": region.textContent = value; break;
        default: result = {status: "failed", value: {code: "unsupported-operation"}};
      }
      result ??= {status: "completed", value: {text: region.textContent, annotation: region.dataset.nemaAnnotation, documentId, url: location.href}};
    }
    results.set(command.invocationId, result);
    if (results.size > 64) results.delete(results.keys().next().value);
    sendResponse(result);
  });
  addEventListener("pagehide", () => { attached = false; chrome.runtime.sendMessage({type: "document.detach", documentId}).catch(() => {}); });
  addEventListener("pageshow", event => {
    if (event.persisted) {
      // Native BFCache document can survive, but the ended attachment binding
      // is replaced with a new incarnation and its native documentId retained.
      documentId = crypto.randomUUID(); results.clear(); attached = true; hello();
    }
  });
  hello();
  setInterval(hello, 2000);
})();
