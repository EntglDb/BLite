// Dedicated Web Worker for OPFS synchronous file I/O.
// Receives commands from the main thread via postMessage, executes them
// using the FileSystemSyncAccessHandle API, and posts results back.
//
// This Worker is spawned by blite-opfs-bridge.mjs.

let _handles = new Map(); // dbName -> { accessHandle, pageSize }

// ─── Base64 helpers ──────────────────────────────────────────────────────

function toBase64(uint8Array) {
    const CHUNK_SIZE = 8192;
    const chunks = [];
    for (let i = 0; i < uint8Array.length; i += CHUNK_SIZE) {
        chunks.push(String.fromCharCode.apply(null, uint8Array.subarray(i, i + CHUNK_SIZE)));
    }
    return btoa(chunks.join(""));
}

function fromBase64(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
}

// ─── Command handler ─────────────────────────────────────────────────────

self.onmessage = async (e) => {
    const { id, cmd, args } = e.data;
    try {
        let result;
        switch (cmd) {
            case "open": {
                const root = await navigator.storage.getDirectory();
                const fileHandle = await root.getFileHandle(args.dbName + ".blite", { create: true });
                const accessHandle = await fileHandle.createSyncAccessHandle();
                _handles.set(args.dbName, { accessHandle, pageSize: args.pageSize });
                result = accessHandle.getSize();
                break;
            }

            case "readPage": {
                const entry = _handles.get(args.dbName);
                if (!entry) throw new Error(`OPFS not open for '${args.dbName}'.`);
                const buf = new Uint8Array(entry.pageSize);
                const offset = args.pageId * entry.pageSize;
                const fileSize = entry.accessHandle.getSize();
                if (offset < fileSize) {
                    entry.accessHandle.read(buf, { at: offset });
                }
                result = toBase64(buf);
                break;
            }

            case "writePage": {
                const entry = _handles.get(args.dbName);
                if (!entry) throw new Error(`OPFS not open for '${args.dbName}'.`);
                const data = fromBase64(args.base64Data);
                const offset = args.pageId * entry.pageSize;
                entry.accessHandle.write(data.subarray(0, entry.pageSize), { at: offset });
                result = null;
                break;
            }

            case "flush": {
                const entry = _handles.get(args.dbName);
                if (entry) entry.accessHandle.flush();
                result = null;
                break;
            }

            case "getSize": {
                const entry = _handles.get(args.dbName);
                result = entry ? entry.accessHandle.getSize() : 0;
                break;
            }

            case "truncate": {
                const entry = _handles.get(args.dbName);
                if (entry) {
                    entry.accessHandle.truncate(0);
                    entry.accessHandle.flush();
                }
                result = null;
                break;
            }

            case "close": {
                const entry = _handles.get(args.dbName);
                if (entry) {
                    try { entry.accessHandle.close(); } catch (_) { /* best-effort */ }
                    _handles.delete(args.dbName);
                }
                result = null;
                break;
            }

            default:
                throw new Error(`Unknown OPFS worker command: ${cmd}`);
        }
        self.postMessage({ id, result });
    } catch (err) {
        self.postMessage({ id, error: err.message });
    }
};
