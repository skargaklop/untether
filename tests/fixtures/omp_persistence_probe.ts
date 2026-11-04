// Public-SDK cold-load evidence only: no agent session, providers or model calls.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

const [mode, packageRoot, root, exactFile] = process.argv.slice(2);
// Resolve the installed package's exported ./session/* surface, not its broad
// index (which also exports agent/model orchestration).
const metadata = JSON.parse(readFileSync(path.join(packageRoot, "package.json"), "utf8"));
assert.equal(metadata.name, "@oh-my-pi/pi-coding-agent");
assert.equal(metadata.version, "18.1.6");
const exportPattern = metadata.exports["./session/*"].import;
assert.equal(exportPattern, "./src/session/*.ts");
const entry = path.join(packageRoot, exportPattern.replace("*", "session-manager"));
const { SessionManager } = await import(pathToFileURL(entry).href);
const messages = [
  { role: "user", content: "Offline historical request", timestamp: 1735689601000 },
  {
    role: "toolResult", toolCallId: "offline-tool-1", toolName: "read",
    content: [{ type: "text", text: "Historical tool output" }],
    isError: false, timestamp: 1735689602000,
  },
  { role: "user", content: "Follow-up after tool output", timestamp: 1735689603000 },
];
const sessionDir = path.join(root, "sessions");
const sessionDirArg = process.env.OMP_SESSION_DIR || sessionDir;
let manager;
let sourceId: string | undefined;
if (mode === "write") {
  const memory = SessionManager.inMemory(root);
  try {
    sourceId = memory.getSessionId();
    for (const message of messages) memory.appendMessage(message);
    manager = await memory.persistCopy({ sessionDir, suppressBreadcrumb: true });
  } finally {
    await memory.flush();
    await memory.close();
  }
} else {
  assert.equal(mode, "read");
  assert.ok(exactFile);
  manager = await SessionManager.open(exactFile, sessionDirArg, undefined, { suppressBreadcrumb: true });
}
try {
  const file = manager.getSessionFile();
  assert.ok(file);
  assert.equal(path.dirname(file), sessionDirArg);
  if (exactFile) assert.equal(file, exactFile);
  const id = manager.getSessionId();
  assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  if (sourceId) assert.notEqual(id, sourceId);
  assert.equal(manager.getHeader()?.id, id);
  assert.equal(manager.getHeader()?.version, 3);
  const entries = manager.getEntries();
  const expectedCwd = process.env.OMP_EXPECT_CWD;
  if (expectedCwd) assert.equal(manager.getHeader()?.cwd, expectedCwd);
  else assert.equal(manager.getHeader()?.cwd, root);
  assert.equal(entries.length, messages.length);
  assert.deepEqual(entries.map((e: any) => e.message), messages);
  for (let i = 0; i < entries.length; i++) {
    assert.equal(entries[i].type, "message");
    assert.equal(entries[i].parentId, i === 0 ? null : entries[i - 1].id);
  }
  await manager.flush();
  console.log(JSON.stringify({ id, file, version: 3, entries, packageVersion: metadata.version }));
} finally {
  await manager.close();
}
