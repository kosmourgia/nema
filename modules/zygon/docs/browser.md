# Browser companion

The browser species attaches to one explicitly configured local demo page. The
bridge owns its private HTTP listener and Unix-socket connection. It does not
own the browser, its tabs, or their lifetime. Stopping the bridge marks presence
unknown; it does not close Chromium. The acceptance harness separately owns the
test browser it launches and cleans that browser up after the experiment.

## Run the real experiment

From the feature worktree:

```sh
PYTHONPATH=modules/zygon python -m zygon.demo_browser --runtime .nema/b-demo
PYTHONPATH=modules/zygon python -m zygon.demo_browser --runtime .nema/b-flix --with-flix
```

The first command starts a private registry, bridge, isolated Chromium profile,
and two independent Unix clients. `--socket PATH` joins an existing registry;
`--headful` uses the guest display. The harness uses inherited DevTools pipes
only for starting/navigating/closing its test tabs and lifecycle checks. Browser
actions being demonstrated travel through actual extension messaging. The
product exposes no DevTools endpoint and no arbitrary eval operation.

The `--with-flix` variant runs `modules/zygon/flix/run` against the same registry,
targets the actual document with `browser.annotate`, checks the resulting DOM
annotation through the companion, and compares the Flix terminal record ID with
the independent terminal client's retained invocation. Flix build prerequisites
and topology queries are documented in the module's main README.

## Manual attached browser

Start the module registry using the module launcher, then run:

```sh
PYTHONPATH=modules/zygon python -m zygon.browser \
  --socket .nema/zygon/zygon.sock --runtime .nema/browser
```

The bridge prints `demoUrl` and the generated private `extension` directory.
Open an isolated browser profile with that unpacked directory:

```sh
chromium --user-data-dir="$PWD/.nema/browser-profile" \
  --disable-extensions-except="$PWD/.nema/browser/extension" \
  --load-extension="$PWD/.nema/browser/extension"
```

Alternatively, use **Load unpacked** in `chrome://extensions` in that isolated
profile. Visit the printed local `demoUrl`. Only this exact origin and `/demo`
path are eligible. The page status line displays its attachment incarnation.
Use the module CLI's `inspect`, `invoke`, and `subscribe` commands against the
same socket. Select the document's exact `id` and `incarnation` from `inspect`.
Invoking `browser.annotate` with `{"value":"hello from a client"}` changes the
region's `data-nema-annotation` and tooltip. `browser.replace` changes its text;
`browser.read` returns text, annotation, document incarnation, and native URL.
`browser.focus` is advertised on the tab, while `browser.enumerate` is advertised
on the extension. Focus completion means the browser accepted the focus request;
it does not assert a compositor's visual focus state.

Ctrl-C the bridge to detach. The page and browser remain running. Restart the
same bridge runtime to reconnect using its saved bearer secret, endpoint, and
registry resume tokens. A port collision is an explicit startup failure; the
bridge does not silently change an already configured endpoint.

## Identity and lifecycle

| Entity | Identity | Incarnation / evidence of continuity |
|---|---|---|
| Extension installation | UUID in private `chrome.storage.local` | Browser-session UUID in `chrome.storage.session` |
| Selected tab | Installation + browser session + native tab ID | Tab UUID in session storage |
| Demo surface | Tab identity + `:demo` | Content attachment UUID and exact native Chrome `documentId` |
| Worker execution | No separate claim of durable worker process identity | Reconstructed from session storage after worker termination |

Navigation keeps the demo surface ID only within the same tab, retires its old
binding, and registers a new document incarnation. The worker sends actions via
`tabs.sendMessage(..., {documentId})`; the content script separately checks the
attachment incarnation. Neither a reused native tab ID nor a replacement page
can receive a command targeting the previous incarnation. Closing/reopening a
tab produces a new tab identity. Full Chromium restart preserves the installed
extension identity only; browser session, tabs, and documents are new.

BFCache can preserve a native document while suspending it. A `pagehide` ends
the attachment binding. Restoring a cached document creates a new attachment
incarnation and retains the native document identity separately. A lost transport
does not imply a lost document: an eight-second operational silence threshold
closes the provider connection and reports unknown presence. Authenticated fresh
snapshots establish tab closure and document replacement.

## Transport and bounded behavior

The bridge binds only `127.0.0.1` on an ephemeral port. `/exchange` requires a
random 256-bit bearer secret. The secret lives in mode-0600 private bootstrap
state and the generated worker config; content scripts and the demo page never
receive it. HTTP Host must name the exact loopback endpoint. Supplied Origin
must be an extension origin. No CORS grant to ordinary pages, shell, arbitrary
URL fetch, or arbitrary JavaScript execution is exposed. No native-messaging
host registration or normal-profile change is needed.

HTTP JSON bodies and response pages are limited to 65,536 bytes; headers to
8 KiB; active HTTP requests to 32; selected tabs to 32; outstanding bridge
commands to 64. Partial HTTP reads expire after three seconds. Commands and
results are paged by encoded byte size. DOM argument strings are limited to
8 KiB. The registry retains cursor catch-up independently of browser polling.

Taking a command from the bridge queue is a single dispatch attempt. Losing the
HTTP response, provider connection, or worker mid-command yields an explicit
uncertain outcome or the registry deadline. The adapter never silently retries
an arbitrary mutation. Worker session storage claims a command before executing
it; a worker restarted after that claim reports outcome-unknown. Different
commands run concurrently; a document response wait is bounded. Cancellation can
confirm `cancelled` while a command is still queued. A synchronous DOM mutation
already dispatched cannot honestly be cancelled retroactively.

Late and duplicate native replies remain observations under the extension
source even when their original document was retired. They cannot replace the
registry's terminal winner. Exact authenticated HTTP body bytes are retained
as separate `browser.transport.body` chunks with a body ID, offset, total
length, and monotonic receive observation. Authentication headers are excluded;
these records claim body capture only. Parsed native metadata and unknown fields
remain inspectable independently of typed registry transitions.

## Observed verification

On Arch Linux ARM with Chromium `153.0.8010.36`, the recovered worktree's real
headless demo completed these checks:

```text
real-chromium-started: Chrome/153.0.8010.36, inherited-debug-pipe
extension-tab-document-registered
annotation-round-trip-shared-transition: completed
read-replace-focus: completed / completed / completed
worker-reconnected: documentContinuity=true
transport-loss-reconnect: preservedDocumentIncarnation=true
bridge-restart-same-browser: preservedStableId=true
navigation-rejects-stale-document: stale-incarnation
tab-close-reopen: new surface and incarnation
browser-restart: stable installation ID, new session incarnation
detach-preserves-browser: browserStillRunning=true
all-browser-demo-invocations-terminal: passed=true
```

Deterministic tests use actual Unix sockets and the authenticated HTTP listener,
with explicitly synthetic browser payloads:

```sh
PYTHONPATH=modules/zygon python -m unittest discover \
  -s modules/zygon/tests -p test_browser.py -v
```

Those protocol tests are separate from the real Chromium harness. They cover
authentication, origin/host rejection, byte limits, native unknown fields,
duplicate registration/results, late results after navigation, queued
cancellation, no replay, registry/bridge restart, bounded batching, selected
page validation, and a stalled partial HTTP client.

## Primary browser references

The implementation uses JSON-serializable extension messages and validates
content-script input as described by Chrome's
[extension messaging documentation](https://developer.chrome.com/docs/extensions/develop/concepts/messaging).

Worker state is persisted outside worker globals and reconstructed after
termination, following Chrome's
[service-worker lifecycle documentation](https://developer.chrome.com/docs/extensions/develop/concepts/service-workers/lifecycle).

Chrome's alternative native-messaging transport requires a host manifest and
length-prefixed stdio frames. This demo deliberately chooses the authenticated
loopback endpoint instead; see
[native messaging](https://developer.chrome.com/docs/extensions/develop/concepts/native-messaging).
