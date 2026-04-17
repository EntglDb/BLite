// Main-thread bridge for OPFS operations via a dedicated Web Worker.
// Exposes async functions that the C# [JSImport] layer can call.
// The Worker runs createSyncAccessHandle which requires a Worker context;
// this bridge makes OPFS usable from the Blazor main thread.
//
// Data is exchanged as base64 strings for [JSImport] compatibility.

let _worker = null;
let _nextId = 0;
let _pending = new Map(); // id -> { resolve, reject }

function ensureWorker() {
    if (_worker) return;
    // Resolve relative to this module so the path is correct regardless of the page route.
    const workerUrl = new URL("./blite-opfs-worker.mjs", import.meta.url);
    _worker = new Worker(workerUrl, { type: "module" });
    _worker.onmessage = (e) => {
        const { id, result, error } = e.data;
        const p = _pending.get(id);
        if (!p) return;
        _pending.delete(id);
        if (error) {
            p.reject(new Error(error));
        } else {
            p.resolve(result);
        }
    };
    _worker.onerror = (e) => {
        const msg = e.message || "OPFS Worker error";
        for (const [, p] of _pending) {
            p.reject(new Error(msg));
        }
        _pending.clear();
    };
}

function send(cmd, args) {
    ensureWorker();
    return new Promise((resolve, reject) => {
        const id = _nextId++;
        _pending.set(id, { resolve, reject });
        _worker.postMessage({ id, cmd, args });
    });
}

// ─── Public API (called from C# via [JSImport]) ─────────────────────────

/**
 * Opens (or creates) an OPFS file for the given database name.
 * @param {string} dbName
 * @param {number} pageSize
 * @returns {Promise<number>} The current file size in bytes.
 */
export function opfsWorkerOpen(dbName, pageSize) {
    return send("open", { dbName, pageSize });
}

/**
 * Reads a single page and returns its data as a base64 string.
 * @param {string} dbName
 * @param {number} pageId
 * @returns {Promise<string>} Base64-encoded page data.
 */
export function opfsWorkerReadPage(dbName, pageId) {
    return send("readPage", { dbName, pageId });
}

/**
 * Writes a single page from base64-encoded data.
 * @param {string} dbName
 * @param {number} pageId
 * @param {string} base64Data
 * @returns {Promise<void>}
 */
export function opfsWorkerWritePage(dbName, pageId, base64Data) {
    return send("writePage", { dbName, pageId, base64Data });
}

/**
 * Flushes pending writes in the Worker.
 * @param {string} dbName
 * @returns {Promise<void>}
 */
export function opfsWorkerFlush(dbName) {
    return send("flush", { dbName });
}

/**
 * Returns the current file size in bytes.
 * @param {string} dbName
 * @returns {Promise<number>}
 */
export function opfsWorkerGetSize(dbName) {
    return send("getSize", { dbName });
}

/**
 * Truncates the OPFS file to zero length.
 * @param {string} dbName
 * @returns {Promise<void>}
 */
export function opfsWorkerTruncate(dbName) {
    return send("truncate", { dbName });
}

/**
 * Closes the OPFS file handle in the Worker.
 * @param {string} dbName
 * @returns {Promise<void>}
 */
export function opfsWorkerClose(dbName) {
    return send("close", { dbName });
}

/**
 * Checks whether OPFS via Worker is available in this browser.
 * @returns {boolean}
 */
export function opfsWorkerIsAvailable() {
    return typeof Worker !== "undefined"
        && typeof navigator !== "undefined"
        && typeof navigator.storage !== "undefined"
        && typeof navigator.storage.getDirectory === "function";
}
