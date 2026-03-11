import * as fs from "node:fs";
import * as path from "node:path";
import { getRuntime } from "./runtime.js";
import {
  PERSISTENCE_DIR_NAME,
  REQID_STORE_FILENAME,
  REQID_FLUSH_DEBOUNCE_MS,
  REQID_MAX_ENTRIES,
} from "./const.js";

export interface PersistentReqIdStore {
  set(chatId: string, reqId: string): void;
  get(chatId: string): Promise<string | undefined>;
  getSync(chatId: string): string | undefined;
  delete(chatId: string): void;
  warmup(onError?: (error: unknown) => void): Promise<number>;
  flush(): Promise<void>;
}

export function createPersistentReqIdStore(accountId: string): PersistentReqIdStore {
  const memoryCache = new Map<string, string>();
  let flushTimer: NodeJS.Timeout | null = null;
  let isDirty = false;

  const getStoreDir = () => {
    const storagePath = getRuntime().state.resolveStateDir();
    const dir = path.join(storagePath, PERSISTENCE_DIR_NAME, accountId);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    return dir;
  };

  const getStorePath = () => path.join(getStoreDir(), REQID_STORE_FILENAME);

  const scheduleFlush = () => {
    isDirty = true;
    if (flushTimer) return;
    flushTimer = setTimeout(async () => {
      await flush();
    }, REQID_FLUSH_DEBOUNCE_MS);
  };

  const flush = async () => {
    if (!isDirty) return;
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    try {
      const data = JSON.stringify(Object.fromEntries(memoryCache));
      const filePath = getStorePath();
      const tmpPath = `${filePath}.tmp`;
      fs.writeFileSync(tmpPath, data);
      fs.renameSync(tmpPath, filePath);
      isDirty = false;
    } catch (error) {
      console.error(`[53aihub] Failed to flush reqId store for ${accountId}:`, error);
    }
  };

  return {
    set(chatId: string, reqId: string) {
      memoryCache.set(chatId, reqId);
      if (memoryCache.size > REQID_MAX_ENTRIES) {
        const firstKey = memoryCache.keys().next().value;
        if (firstKey !== undefined) memoryCache.delete(firstKey);
      }
      scheduleFlush();
    },
    async get(chatId: string) {
      return memoryCache.get(chatId);
    },
    getSync(chatId: string) {
      return memoryCache.get(chatId);
    },
    delete(chatId: string) {
      if (memoryCache.delete(chatId)) {
        scheduleFlush();
      }
    },
    async warmup(onError) {
      try {
        const filePath = getStorePath();
        if (fs.existsSync(filePath)) {
          const content = fs.readFileSync(filePath, "utf-8");
          const data = JSON.parse(content);
          memoryCache.clear();
          for (const [k, v] of Object.entries(data)) {
            memoryCache.set(k, v as string);
          }
          return memoryCache.size;
        }
      } catch (error) {
        onError?.(error);
      }
      return 0;
    },
    flush,
  };
}
