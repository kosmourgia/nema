# Zygon local seam v1

Independent module; reuses Nema's version/id/method/params and retained cursor
conventions, without importing its fork runtime. Private Unix socket 0600 in
runtime directory 0700. UTF-8 NDJSON objects bounded to 65536 bytes including LF.

Requests `{version:1,id:string,method:string,params:object}`; replies echo id and
contain `result` or `error:{code,message}`. Unknown members survive raw capture.

* register: `{registration,resumeToken?:string}` → `{registration,resumeToken}`.
  Registration: `{id,incarnation,kind,medium,mode,operations:[string],parent}`;
  parent is null or `{id,incarnation}`. Optional metadata/binding/geometry/focus/
  visibility and unknown fields are retained. Mode owned/attached/observer.
  Existing stable IDs require the original owner connection or private resume
  token. Same-incarnation duplicates are idempotent; new incarnation invalidates
  old children and settles pending work as outcome-unknown. Ended incarnations
  cannot be revived. Detach from a live host closes its companion connection,
  preserving unknown presence; explicit unregister ends the binding.
* update: `{id,incarnation,patch}`; identity/kind/mode/medium/parent immutable.
* unregister: `{id,incarnation,reason}`; owner-only binding termination.
* inspect: `{}` → `{cursor,registrations,invocations}`. Registrations have
  `presence:online|unknown|ended`, `active:bool`. Bounded snapshots fail explicitly
  when too large; retained records remain pageable.
* subscribe: `{cursor:"0",limit?:1..128}` → `{cursor,records,more}`; bounded
  catch-up pages (polling), no unbounded observer queues. Cursor decimal string
  is local ingestion order, never global causal sequence.
* invoke: `{requester:{id,incarnation},target:{id,incarnation},operation,
  arguments:{},correlation,timeoutMs?:1..300000}` → invocation immediately.
  Default deadline 30000ms. Invocation adds `id,status,recordId,deadlineNs`.
  Provider gets `{version:1,method:"invoke",params:invocation}`. Correlation scope
  requester id/incarnation; exact repeat returns prior receipt without dispatch;
  conflicting reuse fails. Target must be online and advertise namespaced op.
* result: `{invocationId,target:{id,incarnation},status,value?}` from provider.
  Status started/completed/failed/cancelled/outcome-unknown. First terminal wins.
* cancel: `{invocationId,requester:{id,incarnation}}` → invocation. Cooperative
  notification `{version:1,method:"cancel",params:invocation}` to provider; a
  cancellation request alone cannot establish cancelled.
* observe: `{source,incarnation,kind,body,parents?:[]}` → `{recordId}`. Owner-only
  producer observation. `capture.chunk` body retains `{stream,streamSeq:"decimal",
  bytesBase64,monotonicNs:"decimal"}`. Bytes are independent of text projections.

Record seam: `{schema:"nema.lab.record/v1",id,source,incarnation,seq:"decimal",
kind,parents:[],body:{}}`. Sequence is producer-local, parents known causal IDs.
Original protocol bytes are stored separately from typed transitions. Accepted
invocations are committed before dispatch. Disconnect/restart/deadline settle
uncertain outcomes explicitly and never retry an arbitrary command.

Python API: `await Server(runtime_dir).start()`, `.socket_path`, `await close()`;
`await Client.connect(socket_path)`, `await client.call(method,params)`,
`await close()`. `client.on_invoke` and `.on_cancel` are async callbacks given
invocation objects; response reading is independent. `ProtocolError` exposes
code/message. `wait_result(id,timeout=35,on_record=None,cursor='0')` tails records.

Implementation ownership: root core/CLI/demo/schema/docs/progress; process agent
processes.py/demo_child.py/attached.py/tests/test_process.py/docs/process.md;
browser agent browser.py/demo_browser.py/extension/**/tests/test_browser.py/
docs/browser.md; protocol-test agent tests/test_protocol.py/fixtures/protocol/**/
docs/adversarial.md and flix/**/docs/flix.md. No same-file concurrent edits.
