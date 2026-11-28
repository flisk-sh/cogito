import { test, expect } from "bun:test";
import { cogito } from "./cogito.mjs";
const nd = (xs) => xs.map((x) => JSON.stringify(x)).join("\n") + "\n";
const frames = [{ h: { v: 1, id: "r1", lvl: 1, ret: "none" } }, { s: "thinking" }, { t: "Hi" }, { t: "!" }, { d: 1 }];
const server = (handler) => Bun.serve({ port: 0, fetch: handler });
const collect = async (it) => { const o = []; for await (const f of it) o.push(f); return o; };

test("ndjson", async () => {
  const s = server(() => new Response(nd(frames), { headers: { "content-type": "application/vnd.flisk.cogito+ndjson" } }));
  expect(await collect(cogito(`http://localhost:${s.port}/`, {}))).toEqual(frames); s.stop();
});
test("falls back from 415 to sse", async () => {
  const s = server((req) => req.headers.get("accept").includes("cogito+ndjson")
    ? new Response("no", { status: 415 })
    : new Response(frames.map((f) => `data: ${JSON.stringify(f)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } }));
  expect(await collect(cogito(`http://localhost:${s.port}/`, {}))).toEqual(frames); s.stop();
});
test("bin profile", async () => {
  const enc = new TextEncoder(), parts = [];
  const put = (t, str) => { const b = enc.encode(str); parts.push(Uint8Array.from([t, b.length, ...b])); };
  put(6, JSON.stringify(frames[0].h)); put(2, "thinking"); put(1, "Hi"); put(1, "!"); parts.push(Uint8Array.from([5, 0]));
  const s = server(() => new Response(new Blob(parts), { headers: { "content-type": "application/vnd.flisk.cogito.bin" } }));
  expect(await collect(cogito(`http://localhost:${s.port}/`, {}, { start: "bin" }))).toEqual(frames); s.stop();
});
test("bin profile waits for the length byte after a lone frame type", async () => {
  const hello = new TextEncoder().encode(JSON.stringify(frames[0].h));
  const chunks = [[6], [hello.length, ...hello, 1], [2, 72, 105, 5], [0]];
  const fetchImpl = Object.assign(async () => new Response(new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(Uint8Array.from(chunk));
      controller.close();
    },
  }), { headers: { "content-type": "application/vnd.flisk.cogito.bin" } }), { preconnect() {} });

  expect(await collect(cogito("https://example.test/", {}, { start: "bin", fetchImpl })))
    .toEqual([frames[0], { t: "Hi" }, { d: 1 }]);
});
test("incomplete stream throws", async () => {
  const s = server(() => new Response(nd(frames.slice(0, 3)), { headers: { "content-type": "application/vnd.flisk.cogito+ndjson" } }));
  await expect(collect(cogito(`http://localhost:${s.port}/`, {}))).rejects.toThrow("without a done");
  s.stop();
});
