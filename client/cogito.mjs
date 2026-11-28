// cogito client, draft 0. Zero dependencies, fetch only.
// for await (const f of cogito("/v1/cogito", { messages })) { ... }
const ACCEPT = {
  bin: "application/vnd.flisk.cogito.bin, application/vnd.flisk.cogito+ndjson;q=0.9, text/event-stream;q=0.5",
  ndjson: "application/vnd.flisk.cogito+ndjson, text/event-stream;q=0.5",
  sse: "text/event-stream",
};
const LADDER = ["bin", "ndjson", "sse"];

async function* lines(body) {
  const dec = new TextDecoder(); let buf = "";
  for await (const chunk of body) {
    buf += dec.decode(chunk, { stream: true });
    let i; while ((i = buf.indexOf("\n")) >= 0) { const l = buf.slice(0, i); buf = buf.slice(i + 1); if (l.trim()) yield l; }
  }
  if (buf.trim()) yield buf;
}
async function* parse(res) {
  const type = res.headers.get("content-type") || "";
  if (type.startsWith("text/event-stream")) {
    for await (const l of lines(res.body)) if (l.startsWith("data:")) yield JSON.parse(l.slice(5).trim());
  } else if (type.includes("cogito.bin")) {
    const T = { 1: "t", 2: "s", 6: "h", 3: "p", 4: "u", 7: "e", 8: "r" };
    const dec = new TextDecoder(); let buf = new Uint8Array(0);
    for await (const c of res.body) {
      const n = new Uint8Array(buf.length + c.length); n.set(buf); n.set(c, buf.length); buf = n;
      while (buf.length >= 2) {
        let len = 0, shift = 0, i = 1;
        while (i < buf.length) { const b = buf[i++]; len |= (b & 127) << shift; shift += 7; if (!(b & 128)) break; else if (i >= buf.length) { i = -1; break; } }
        if (i < 0 || i + len > buf.length) break;
        const type = buf[0], s = dec.decode(buf.subarray(i, i + len)); buf = buf.subarray(i + len);
        if (type === 5) { yield { d: 1 }; continue; }
        yield { [T[type]]: type === 1 || type === 2 || type === 8 ? s : JSON.parse(s) };
      }
    }
  } else { for await (const l of lines(res.body)) yield JSON.parse(l); }
}

export async function* cogito(url, body, { level = 1, start = "ndjson", helloTimeoutMs = 5000, fetchImpl = fetch, signal } = {}) {
  for (let rung = LADDER.indexOf(start); rung < LADDER.length; rung++) {
    const ctl = new AbortController(); signal?.addEventListener("abort", () => ctl.abort());
    const timer = setTimeout(() => ctl.abort(), helloTimeoutMs);
    let res;
    try {
      res = await fetchImpl(url, { method: "POST", signal: ctl.signal, body: JSON.stringify(body),
        headers: { "content-type": "application/json", accept: ACCEPT[LADDER[rung]], "cogito-level": String(level) } });
    } catch (e) { clearTimeout(timer); if (signal?.aborted) throw e; continue; }
    if (res.status === 406 || res.status === 415) { clearTimeout(timer); continue; }
    if (!res.ok) { clearTimeout(timer); throw new Error(`cogito: HTTP ${res.status}`); }
    let first = true;
    try {
      for await (const frame of parse(res)) { if (first) { clearTimeout(timer); first = false; } yield frame; if (frame.d || frame.e) return; }
    } catch (e) { if (first && !signal?.aborted) { clearTimeout(timer); continue; } throw e; }
    clearTimeout(timer);
    if (!first) throw new Error("cogito: stream ended without a done frame");
  }
  throw new Error("cogito: no profile worked");
}
