import type { WebSocket } from "ws";
import type { MessageState } from "./interface.js";
import { createPersistentReqIdStore, type PersistentReqIdStore } from "./reqid-store.js";
import {
  MESSAGE_STATE_TTL_MS,
  MESSAGE_STATE_CLEANUP_INTERVAL_MS,
  MESSAGE_STATE_MAX_SIZE,
} from "./const.js";

const wsClientInstances = new Map<string, WebSocket>();

export function getWebSocket(accountId: string): WebSocket | null {
  return wsClientInstances.get(accountId) ?? null;
}

export function setWebSocket(accountId: string, client: WebSocket): void {
  wsClientInstances.set(accountId, client);
}

export function deleteWebSocket(accountId: string): void {
  wsClientInstances.delete(accountId);
}

interface MessageStateEntry {
  state: MessageState;
  createdAt: number;
}

const messageStates = new Map<string, MessageStateEntry>();

let cleanupTimer: ReturnType<typeof setInterval> | null = null;

export function startMessageStateCleanup(): void {
  if (cleanupTimer) return;

  cleanupTimer = setInterval(() => {
    pruneMessageStates();
  }, MESSAGE_STATE_CLEANUP_INTERVAL_MS);

  if (cleanupTimer && typeof cleanupTimer === "object" && "unref" in cleanupTimer) {
    cleanupTimer.unref();
  }
}

export function stopMessageStateCleanup(): void {
  if (cleanupTimer) {
    clearInterval(cleanupTimer);
    cleanupTimer = null;
  }
}

function pruneMessageStates(): void {
  const now = Date.now();

  for (const [key, entry] of messageStates) {
    if (now - entry.createdAt >= MESSAGE_STATE_TTL_MS) {
      messageStates.delete(key);
    }
  }

  if (messageStates.size > MESSAGE_STATE_MAX_SIZE) {
    const sorted = [...messageStates.entries()].sort((a, b) => a[1].createdAt - b[1].createdAt);
    const toRemove = sorted.slice(0, messageStates.size - MESSAGE_STATE_MAX_SIZE);
    for (const [key] of toRemove) {
      messageStates.delete(key);
    }
  }
}

export function setMessageState(messageId: string, state: MessageState): void {
  messageStates.set(messageId, {
    state,
    createdAt: Date.now(),
  });
}

export function getMessageState(messageId: string): MessageState | undefined {
  const entry = messageStates.get(messageId);
  if (!entry) return undefined;

  if (Date.now() - entry.createdAt >= MESSAGE_STATE_TTL_MS) {
    messageStates.delete(messageId);
    return undefined;
  }
  return entry.state;
}

export function deleteMessageState(messageId: string): void {
  messageStates.delete(messageId);
}

export function clearAllMessageStates(): void {
  messageStates.clear();
}

// ReqId Persistence Management
const reqIdStores = new Map<string, PersistentReqIdStore>();

function getOrCreateReqIdStore(accountId: string): PersistentReqIdStore {
  let store = reqIdStores.get(accountId);
  if (!store) {
    store = createPersistentReqIdStore(accountId);
    reqIdStores.set(accountId, store);
  }
  return store;
}

export function setLastMsgIdForChat(chatId: string, msgId: string, accountId = "default"): void {
  getOrCreateReqIdStore(accountId).set(chatId, msgId);
}

export function getLastMsgIdForChat(chatId: string, accountId = "default"): string | undefined {
  return getOrCreateReqIdStore(accountId).getSync(chatId);
}

export async function warmupReqIdStore(accountId = "default", log?: (msg: string) => void): Promise<number> {
  const store = getOrCreateReqIdStore(accountId);
  return store.warmup((err) => log?.(`ReqId warmup error: ${String(err)}`));
}

export async function flushReqIdStore(accountId = "default"): Promise<void> {
  const store = reqIdStores.get(accountId);
  if (store) await store.flush();
}

export async function cleanupAccount(accountId: string): Promise<void> {
  const wsClient = wsClientInstances.get(accountId);
  if (wsClient) {
    try {
      wsClient.close();
    } catch {
      // ignore
    }
    wsClientInstances.delete(accountId);
  }
  const store = reqIdStores.get(accountId);
  if (store) await store.flush();
}

export async function cleanupAll(): Promise<void> {
  stopMessageStateCleanup();

  for (const [accountId, wsClient] of wsClientInstances) {
    try {
      wsClient.close();
    } catch {
      // ignore
    }
  }
  wsClientInstances.clear();
  
  for (const [, store] of reqIdStores) {
    await store.flush();
  }

  clearAllMessageStates();
}
