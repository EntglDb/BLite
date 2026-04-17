// BLite IndexedDB interop module.
// Provides persistent page storage using the browser's IndexedDB API.
// All operations are async (IndexedDB has no synchronous API).
// Binary data is exchanged as base64 strings for JSImport compatibility.

const PAGES_STORE = "pages";
const META_STORE = "meta";
const WAL_STORE_NAME = "wal";

let _databases = new Map(); // dbName -> IDBDatabase

// ─── Base64 helpers ──────────────────────────────────────────────────────

function toBase64(uint8Array) {
    // Use chunked String.fromCharCode to avoid stack overflow on large arrays.
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

/**
 * Opens (or creates) an IndexedDB database for the given logical database name.
 * @param {string} dbName - Logical database name.
 * @returns {Promise<number>} The stored nextPageId (0 if fresh database).
 */
export function idbOpen(dbName) {
    return new Promise((resolve, reject) => {
        // Do NOT hardcode a version — open at the current version so we never
        // get a VersionError if idbWalOpen previously bumped the version.
        const request = indexedDB.open("blite_" + dbName);

        request.onupgradeneeded = (event) => {
            const db = event.target.result;
            if (!db.objectStoreNames.contains(PAGES_STORE)) {
                db.createObjectStore(PAGES_STORE); // keyed by pageId (number)
            }
            if (!db.objectStoreNames.contains(META_STORE)) {
                db.createObjectStore(META_STORE); // keyed by string keys
            }
            if (!db.objectStoreNames.contains(WAL_STORE_NAME)) {
                db.createObjectStore(WAL_STORE_NAME, { autoIncrement: true });
            }
        };

        request.onsuccess = (event) => {
            const db = event.target.result;
            _databases.set(dbName, db);

            // Read nextPageId from meta store
            const tx = db.transaction(META_STORE, "readonly");
            const store = tx.objectStore(META_STORE);
            const getReq = store.get("nextPageId");
            getReq.onsuccess = () => {
                resolve(getReq.result || 0);
            };
            getReq.onerror = () => resolve(0);
        };

        request.onerror = () => reject(new Error(`Failed to open IndexedDB for '${dbName}': ${request.error}`));
    });
}

/**
 * Reads a page by its ID.
 * @param {string} dbName
 * @param {number} pageId
 * @param {number} pageSize
 * @returns {Promise<string>} Base64-encoded page data (zero-filled if not found).
 */
export function idbReadPage(dbName, pageId, pageSize) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        const tx = db.transaction(PAGES_STORE, "readonly");
        const store = tx.objectStore(PAGES_STORE);
        const request = store.get(pageId);

        request.onsuccess = () => {
            if (request.result) {
                resolve(toBase64(new Uint8Array(request.result)));
            } else {
                resolve(toBase64(new Uint8Array(pageSize))); // zeroed
            }
        };
        request.onerror = () => reject(new Error(`Failed to read page ${pageId}: ${request.error}`));
    });
}

/**
 * Writes a page.
 * @param {string} dbName
 * @param {number} pageId
 * @param {string} base64Data - Base64-encoded page data.
 * @returns {Promise<void>}
 */
export function idbWritePage(dbName, pageId, base64Data) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        const data = fromBase64(base64Data);
        const tx = db.transaction(PAGES_STORE, "readwrite");
        const store = tx.objectStore(PAGES_STORE);
        store.put(data.buffer, pageId);

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(new Error(`Failed to write page ${pageId}: ${tx.error}`));
    });
}

/**
 * Persists the nextPageId counter.
 * @param {string} dbName
 * @param {number} nextPageId
 * @returns {Promise<void>}
 */
export function idbSaveNextPageId(dbName, nextPageId) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        const tx = db.transaction(META_STORE, "readwrite");
        const store = tx.objectStore(META_STORE);
        store.put(nextPageId, "nextPageId");

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(new Error(`Failed to save nextPageId: ${tx.error}`));
    });
}

/**
 * Deletes a page from the store.
 * @param {string} dbName
 * @param {number} pageId
 * @returns {Promise<void>}
 */
export function idbDeletePage(dbName, pageId) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        const tx = db.transaction(PAGES_STORE, "readwrite");
        const store = tx.objectStore(PAGES_STORE);
        store.delete(pageId);

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(new Error(`Failed to delete page ${pageId}: ${tx.error}`));
    });
}

/**
 * Closes the IndexedDB connection.
 * @param {string} dbName
 */
export function idbClose(dbName) {
    const db = _databases.get(dbName);
    if (db) {
        db.close();
        _databases.delete(dbName);
    }
}

/**
 * Checks whether IndexedDB is available.
 * @returns {boolean}
 */
export function idbIsAvailable() {
    return typeof indexedDB !== "undefined";
}

// ─── WAL support ──────────────────────────────────────────────────────────

/**
 * Opens (or creates) a WAL object store inside the same IndexedDB database.
 * Must be called after idbOpen. If the store doesn't exist, we bump the version.
 * @param {string} dbName
 * @returns {Promise<void>}
 */
export function idbWalOpen(dbName) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        if (db.objectStoreNames.contains(WAL_STORE_NAME)) {
            resolve();
            return;
        }

        // Need to reopen with a version bump to add the WAL store
        const newVersion = db.version + 1;
        db.close();

        const request = indexedDB.open("blite_" + dbName, newVersion);
        request.onupgradeneeded = (event) => {
            const upgradedDb = event.target.result;
            if (!upgradedDb.objectStoreNames.contains(WAL_STORE_NAME)) {
                upgradedDb.createObjectStore(WAL_STORE_NAME, { autoIncrement: true });
            }
        };
        request.onsuccess = (event) => {
            _databases.set(dbName, event.target.result);
            resolve();
        };
        request.onerror = () => reject(new Error(`Failed to open WAL store for '${dbName}': ${request.error}`));
    });
}

/**
 * Appends a WAL record to IndexedDB.
 * @param {string} dbName
 * @param {string} base64RecordData - Base64-encoded WAL record bytes.
 * @returns {Promise<void>}
 */
export function idbWalAppend(dbName, base64RecordData) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        const data = fromBase64(base64RecordData);
        const tx = db.transaction(WAL_STORE_NAME, "readwrite");
        const store = tx.objectStore(WAL_STORE_NAME);
        store.add(data.buffer);

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(new Error(`Failed to append WAL record: ${tx.error}`));
    });
}

/**
 * Reads all WAL records from IndexedDB.
 * Returns a JSON string of base64-encoded record arrays for JSImport compatibility.
 * @param {string} dbName
 * @returns {Promise<string>} JSON array of base64 strings, e.g. '["AQAA...","AgAA..."]'
 */
export function idbWalReadAll(dbName) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        if (!db.objectStoreNames.contains(WAL_STORE_NAME)) {
            resolve("[]");
            return;
        }

        const tx = db.transaction(WAL_STORE_NAME, "readonly");
        const store = tx.objectStore(WAL_STORE_NAME);
        const request = store.getAll();

        request.onsuccess = () => {
            const results = (request.result || []).map(buf => toBase64(new Uint8Array(buf)));
            resolve(JSON.stringify(results));
        };
        request.onerror = () => reject(new Error(`Failed to read WAL records: ${request.error}`));
    });
}

/**
 * Clears all WAL records from IndexedDB (truncate).
 * @param {string} dbName
 * @returns {Promise<void>}
 */
export function idbWalClear(dbName) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        if (!db.objectStoreNames.contains(WAL_STORE_NAME)) {
            resolve();
            return;
        }

        const tx = db.transaction(WAL_STORE_NAME, "readwrite");
        const store = tx.objectStore(WAL_STORE_NAME);
        store.clear();

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(new Error(`Failed to clear WAL records: ${tx.error}`));
    });
}

/**
 * Returns the approximate total size of WAL records in bytes.
 * @param {string} dbName
 * @returns {Promise<number>}
 */
export function idbWalGetSize(dbName) {
    return new Promise((resolve, reject) => {
        const db = _databases.get(dbName);
        if (!db) { reject(new Error(`IndexedDB not open for '${dbName}'.`)); return; }

        if (!db.objectStoreNames.contains(WAL_STORE_NAME)) {
            resolve(0);
            return;
        }

        const tx = db.transaction(WAL_STORE_NAME, "readonly");
        const store = tx.objectStore(WAL_STORE_NAME);
        const request = store.getAll();

        request.onsuccess = () => {
            let size = 0;
            for (const buf of (request.result || [])) {
                size += buf.byteLength;
            }
            resolve(size);
        };
        request.onerror = () => reject(new Error(`Failed to compute WAL size: ${request.error}`));
    });
}
