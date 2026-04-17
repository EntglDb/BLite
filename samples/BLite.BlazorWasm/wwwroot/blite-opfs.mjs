// BLite OPFS (Origin Private File System) interop module.
// Provides high-performance synchronous page I/O via SyncAccessHandle.
// This module is loaded by the .NET WASM runtime via [JSImport].

let _handles = new Map(); // dbName -> { root, fileHandle, accessHandle }

/**
 * Opens (or creates) an OPFS file for the given database name.
 * Must be called from a dedicated Worker thread for SyncAccessHandle.
 * @param {string} dbName - Logical database name (becomes the OPFS file name).
 * @param {number} pageSize - Fixed page size in bytes.
 * @returns {Promise<number>} The current file size in bytes.
 */
export async function opfsOpen(dbName, pageSize) {
    const root = await navigator.storage.getDirectory();
    const fileHandle = await root.getFileHandle(dbName + ".blite", { create: true });
    const accessHandle = await fileHandle.createSyncAccessHandle();
    _handles.set(dbName, { root, fileHandle, accessHandle, pageSize });
    return accessHandle.getSize();
}

/**
 * Reads a full page into the provided buffer.
 * @param {string} dbName
 * @param {number} pageId
 * @param {Uint8Array} destination - Must be at least pageSize bytes.
 * @returns {number} Bytes read.
 */
export function opfsReadPage(dbName, pageId, destination) {
    const entry = _handles.get(dbName);
    if (!entry) throw new Error(`OPFS handle not open for '${dbName}'.`);
    const offset = pageId * entry.pageSize;
    const fileSize = entry.accessHandle.getSize();
    if (offset >= fileSize) {
        // Page beyond file end — return zeroes
        destination.fill(0, 0, entry.pageSize);
        return 0;
    }
    return entry.accessHandle.read(destination.subarray(0, entry.pageSize), { at: offset });
}

/**
 * Writes a full page from the provided buffer.
 * @param {string} dbName
 * @param {number} pageId
 * @param {Uint8Array} source - Must be at least pageSize bytes.
 */
export function opfsWritePage(dbName, pageId, source) {
    const entry = _handles.get(dbName);
    if (!entry) throw new Error(`OPFS handle not open for '${dbName}'.`);
    const offset = pageId * entry.pageSize;
    entry.accessHandle.write(source.subarray(0, entry.pageSize), { at: offset });
}

/**
 * Flushes pending writes to the OPFS file.
 * @param {string} dbName
 */
export function opfsFlush(dbName) {
    const entry = _handles.get(dbName);
    if (!entry) throw new Error(`OPFS handle not open for '${dbName}'.`);
    entry.accessHandle.flush();
}

/**
 * Returns the current file size in bytes.
 * @param {string} dbName
 * @returns {number}
 */
export function opfsGetSize(dbName) {
    const entry = _handles.get(dbName);
    if (!entry) return 0;
    return entry.accessHandle.getSize();
}

/**
 * Truncates the OPFS file to zero length.
 * @param {string} dbName
 */
export function opfsTruncate(dbName) {
    const entry = _handles.get(dbName);
    if (!entry) throw new Error(`OPFS handle not open for '${dbName}'.`);
    entry.accessHandle.truncate(0);
    entry.accessHandle.flush();
}

/**
 * Closes the SyncAccessHandle and releases the OPFS file.
 * @param {string} dbName
 */
export function opfsClose(dbName) {
    const entry = _handles.get(dbName);
    if (!entry) return;
    try {
        entry.accessHandle.close();
    } finally {
        _handles.delete(dbName);
    }
}

/**
 * Checks whether the OPFS SyncAccessHandle API is available.
 * Returns true only when running in a context that supports
 * createSyncAccessHandle (typically a dedicated Web Worker).
 * @returns {boolean}
 */
export function opfsIsAvailable() {
    // createSyncAccessHandle is only usable inside a dedicated Web Worker.
    // The API may exist on FileSystemFileHandle.prototype even on the main
    // thread (Chrome), but calling it outside a Worker throws TypeError.
    if (typeof globalThis.DedicatedWorkerGlobalScope === "undefined") return false;
    return typeof navigator !== "undefined"
        && typeof navigator.storage !== "undefined"
        && typeof navigator.storage.getDirectory === "function"
        && typeof FileSystemFileHandle !== "undefined"
        && typeof FileSystemFileHandle.prototype.createSyncAccessHandle === "function";
}
