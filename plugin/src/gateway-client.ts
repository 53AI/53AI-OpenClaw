import { createHash, randomUUID } from "node:crypto";
import { existsSync, lstatSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import WebSocket from "ws";

import type { ControlAction, SessionMessage, SessionSummary, TimelineEvent } from "./models";

export type GatewayConfig = {
  baseUrl: string;
  botId: string;
  secret: string;
  hostKind?: string;
  requestTimeoutMs: number;
  streamReconnectMs: number;
  runtimeRoot?: string;
  exposeRawThinking?: boolean;
  preferResponsesApi?: boolean;
  modelOverride?: string;
};

export type GatewaySession = SessionSummary;
export type GatewayEvent = TimelineEvent;
export type GatewayCronScheduler = {
  enabled?: boolean;
  storePath?: string;
  jobCount?: number;
  nextWakeAt?: string;
  lastError?: string;
};
export type GatewayCronTask = {
  id: string;
  name: string;
  enabled: boolean;
  status?: string;
  agentId?: string;
  schedule?: string;
  nextRunAt?: string;
  lastRunAt?: string;
  payloadKind?: string;
};
export type GatewayRuntimeInfo = {
  modelPrimary?: string;
  enabledSkills: string[];
  cronScheduler?: GatewayCronScheduler;
  cronTasks?: GatewayCronTask[];
};
export type GatewayMessageAttachment = {
  type?: string;
  mimeType?: string;
  fileName?: string;
  content: string;
};
export type GatewaySendMessageOptions = {
  attachments?: GatewayMessageAttachment[];
};
export type GatewayHealthSnapshot = {
  ok?: boolean;
  status: "ok" | "degraded" | "error" | "unknown";
  checkedAt?: string;
  durationMs?: number;
  lastError?: string;
};
export type GatewayPagination = {
  limit: number;
  offset: number;
  total?: number;
  hasMore: boolean;
  nextOffset?: number;
};
export type GatewaySessionPage = {
  sessions: GatewaySession[];
  pagination: GatewayPagination;
};

type GatewayFrame =
  | {
      type: "event";
      event: string;
      seq?: number;
      payload?: Record<string, unknown>;
    }
  | {
      type: "res";
      id: string;
      ok: boolean;
      payload?: unknown;
      error?: {
        code?: string;
        message?: string;
        details?: unknown;
      };
    };

export type GatewayClient = {
  getRuntimeInfo(): Promise<GatewayRuntimeInfo>;
  getHealth(): Promise<GatewayHealthSnapshot>;
  listSessions(limit?: number): Promise<GatewaySession[]>;
  listSessionPage(options?: { limit?: number; offset?: number }): Promise<GatewaySessionPage>;
  createSession(title: string, initialPrompt?: string): Promise<GatewaySession>;
  getSession(sessionId: string): Promise<GatewaySession>;
  getSessionMessages(sessionId: string, limit?: number): Promise<SessionMessage[]>;
  sendMessage(sessionId: string, content: string, options?: GatewaySendMessageOptions): Promise<void>;
  controlSession(sessionId: string, action: ControlAction, title?: string): Promise<void>;
  listEvents(sessionId: string, afterSeq?: number): Promise<GatewayEvent[]>;
  subscribe(
    sessionId: string,
    afterSeq: number,
    handlers: {
      onEvent: (event: GatewayEvent) => void;
      onDisconnect: (error?: Error) => void;
    }
  ): () => void;
  stop(): Promise<void>;
};

type GatewayRequestOptions = {
  timeoutMs?: number | null;
};

const OPENCLAW_ABORT_ACK_TIMEOUT_MS = 1_000;

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: NodeJS.Timeout | null;
};

type SessionSubscription = {
  handlers: Set<{
    onEvent: (event: GatewayEvent) => void;
    onDisconnect: (error?: Error) => void;
  }>;
  lastSeq: number;
  openedAtMs: number;
  activeRunIds: Set<string>;
  renderedAssistantTextByRun: Map<string, string>;
  skippedSessionAssistantText: Set<string>;
  renderedThinkingTextByRun: Map<string, string>;
  toolCallDataById: Map<string, Record<string, unknown>>;
  lastToolCallDataByName: Map<string, Record<string, unknown>>;
};

const DEFAULT_SCOPES = ["operator.read", "operator.write"];
const MAX_DELTA_CHUNK_CHARS = 240;
const MIN_DELTA_CHUNK_CHARS = 80;
const SESSION_LIST_PAGE_LIMIT = 50;
const SESSION_LIST_MAX_PAGES = 100;
const CHAT_HISTORY_PAGE_LIMIT = 200;
const CHAT_HISTORY_GATEWAY_MAX_LIMIT = 1000;
const CHAT_HISTORY_MAX_PAGES = 100;
const CRON_LIST_PAGE_LIMIT = 50;
const CRON_LIST_MAX_PAGES = 100;
const OPENCLAW_STOP_SETTLE_TIMEOUT_MS = 3_000;
const GATEWAY_PROTOCOL_MIN = 3;
const GATEWAY_PROTOCOL_MAX = 4;
const WEBSOCKET_CONNECTING = 0;

type RpcTransport = {
  request(method: string, params: Record<string, unknown>, options?: GatewayRequestOptions): Promise<any>;
  onEvent(listener: (frame: Extract<GatewayFrame, { type: "event" }>) => void): void;
  onDisconnect(listener: (error?: Error) => void): void;
  stop(): Promise<void>;
};

export function createGatewayClient(config: Partial<GatewayConfig>): GatewayClient {
  const resolved = resolveGatewayConfig(config);
  const hostKind = resolved.hostKind ?? "openclaw";
  const transport = createTransport(resolved);
  const subscriptions = new Map<string, SessionSubscription>();
  const responsesRuns = new Map<string, AbortController>();

  const ensureSubscribed = async (sessionId: string) => {
    await transport.request("sessions.subscribe", {});
    await transport.request("sessions.messages.subscribe", { key: sessionId });
  };

  const emitEvents = (sessionId: string, events: GatewayEvent[]) => {
    const subscription = subscriptions.get(sessionId);
    if (!subscription) {
      logOpenClawDuplicateTrace("gateway.emit.no_subscription", { sessionId, eventCount: events.length });
      return;
    }

    for (const event of events) {
      if (event.seq <= subscription.lastSeq) {
        logOpenClawDuplicateTrace("gateway.emit.skip_old_seq", {
          sessionId,
          lastSeq: subscription.lastSeq,
          event: summarizeGatewayEventForDuplicateTrace(event)
        });
        continue;
      }
      subscription.lastSeq = Math.max(subscription.lastSeq, event.seq);
      logOpenClawDuplicateTrace("gateway.emit.forward", {
        sessionId,
        lastSeq: subscription.lastSeq,
        event: summarizeGatewayEventForDuplicateTrace(event)
      });
      for (const handler of subscription.handlers) {
        handler.onEvent(event);
      }
    }
  };

  const subscribeToSession = (
    sessionId: string,
    afterSeq: number,
    handlers: {
      onEvent: (event: GatewayEvent) => void;
      onDisconnect: (error?: Error) => void;
    }
  ) => {
    const subscription = subscriptions.get(sessionId) ?? {
      handlers: new Set(),
      lastSeq: afterSeq,
      openedAtMs: Date.now(),
      activeRunIds: new Set<string>(),
      renderedAssistantTextByRun: new Map<string, string>(),
      skippedSessionAssistantText: new Set<string>(),
      renderedThinkingTextByRun: new Map<string, string>(),
      toolCallDataById: new Map<string, Record<string, unknown>>(),
      lastToolCallDataByName: new Map<string, Record<string, unknown>>()
    };
    subscription.lastSeq = Math.max(subscription.lastSeq, afterSeq);
    subscription.handlers.add(handlers);
    subscriptions.set(sessionId, subscription);

    void ensureSubscribed(sessionId).catch((error) => {
      handlers.onDisconnect(error instanceof Error ? error : new Error(String(error)));
    });

    return () => {
      const existing = subscriptions.get(sessionId);
      if (!existing) {
        return;
      }
      existing.handlers.delete(handlers);
      if (existing.handlers.size === 0) {
        subscriptions.delete(sessionId);
        void transport
          .request("sessions.messages.unsubscribe", { key: sessionId }, { timeoutMs: 2_000 })
          .catch(() => {});
      }
    };
  };

  const createOpenClawStopSettledWait = (sessionId: string, startedAtMs: number) => {
    let done = false;
    let close: (() => void) | undefined;
    let timer: NodeJS.Timeout;
    let resolvePromise: () => void = () => {};
    const timeoutMs = Math.min(resolved.requestTimeoutMs, OPENCLAW_STOP_SETTLE_TIMEOUT_MS);

    const finish = () => {
      if (done) {
        return;
      }
      done = true;
      clearTimeout(timer);
      close?.();
      resolvePromise();
    };

    const promise = new Promise<void>((resolve) => {
      resolvePromise = resolve;
      timer = setTimeout(finish, timeoutMs);
      close = subscribeToSession(sessionId, subscriptions.get(sessionId)?.lastSeq ?? 0, {
        onEvent: (event) => {
          if (isOpenClawStopSettledEvent(event, startedAtMs)) {
            finish();
          }
        },
        onDisconnect: () => {
          finish();
        }
      });
    });

    return {
      promise,
      cancel: finish
    };
  };

  transport.onEvent((frame) => {
    const sessionId = extractSessionKey(frame.payload);
    if (!sessionId) {
      return;
    }
    const subscription = subscriptions.get(sessionId);
    if (!subscription) {
      return;
    }

    if (isStaleChatFrame(frame, subscription)) {
      return;
    }

    const events = mapGatewayFrameToEvents(frame, subscription.lastSeq, subscription, resolved.exposeRawThinking);
    logOpenClawDuplicateTrace("gateway.frame.mapped", {
      sessionId,
      frameEvent: frame.event,
      frameSeq: frame.seq,
      lastSeq: subscription.lastSeq,
      payloadSeq: toRecord(frame.payload).seq,
      payloadState: toRecord(frame.payload).state,
      runId: toRecord(frame.payload).runId,
      events: events.map(summarizeGatewayEventForDuplicateTrace)
    });
    updateActiveRunTracking(frame, subscription);
    emitEvents(sessionId, events);
  });

  transport.onDisconnect((error) => {
    for (const subscription of subscriptions.values()) {
      for (const handler of subscription.handlers) {
        handler.onDisconnect(error);
      }
    }
  });

  return {
    async getRuntimeInfo() {
      const [runtimeInfo, cronInfo] = await Promise.all([readRuntimeInfo(transport), readCronInfo(transport)]);
      return {
        ...runtimeInfo,
        ...cronInfo
      };
    },
    async getHealth() {
      try {
        const payload = await transport.request("health", {}, { timeoutMs: 2_000 });
        return extractGatewayHealth(payload);
      } catch (error) {
        return {
          status: "unknown",
          lastError: error instanceof Error ? error.message : String(error)
        };
      }
    },
    async listSessions(limit = SESSION_LIST_PAGE_LIMIT) {
      return readSessionListPages(transport, hostKind, limit);
    },
    async listSessionPage(options = {}) {
      return readSessionListPage(transport, hostKind, {
        limit: options.limit ?? SESSION_LIST_PAGE_LIMIT,
        offset: options.offset ?? 0
      });
    },
    async createSession(title) {
      const payload = await transport.request("sessions.create", {
        label: title,
        agentId: "main"
      });
      return {
        ...normalizeSession(extractCreatedSession(payload), title, hostKind),
        title
      };
    },
    async getSession(sessionId) {
      const match = await findSessionById(transport, hostKind, sessionId);
      return match ?? fallbackSession(sessionId, hostKind);
    },
    async getSessionMessages(sessionId, limit = CHAT_HISTORY_PAGE_LIMIT * CHAT_HISTORY_MAX_PAGES) {
      const rawMessages = await readChatHistoryPages(transport, sessionId, limit);
      return extractMessagesFromRaw(sessionId, rawMessages);
    },
    async sendMessage(sessionId, content, options = {}) {
      if (resolved.preferResponsesApi) {
        try {
          await startResponsesApiRun({
            config: resolved,
            sessionId,
            content,
            activeRuns: responsesRuns,
            nextSeq: () => (subscriptions.get(sessionId)?.lastSeq ?? 0) + 1,
            emit: (event) => emitEvents(sessionId, [event])
          });
          return;
        } catch (error) {
          if (!isResponsesApiUnavailableError(error)) {
            throw error;
          }
        }
      }

      await transport.request("chat.send", {
        sessionKey: sessionId,
        message: content,
        ...(options.attachments?.length ? { attachments: options.attachments } : {}),
        deliver: false,
        idempotencyKey: randomUUID()
      });
    },
    async controlSession(sessionId, action, title) {
      if (action === "stop") {
        const activeRun = responsesRuns.get(sessionId);
        if (activeRun) {
          activeRun.abort();
          responsesRuns.delete(sessionId);
          emitEvents(sessionId, [
            {
              id: `${sessionId}:responses:interrupted:${Date.now()}`,
              sessionId,
              seq: (subscriptions.get(sessionId)?.lastSeq ?? 0) + 1,
              kind: "run.interrupted",
              payload: { transport: "responses-http", reason: "aborted by user" },
              createdAt: new Date().toISOString()
            }
          ]);
          return;
        }

        const stopSettled = createOpenClawStopSettledWait(sessionId, Date.now());
        try {
          await transport.request(
            "chat.abort",
            {
              sessionKey: sessionId
            },
            { timeoutMs: Math.min(resolved.requestTimeoutMs, OPENCLAW_ABORT_ACK_TIMEOUT_MS) }
          );
        } catch (error) {
          if (!isGatewayRequestTimeoutFor(error, "chat.abort")) {
            stopSettled.cancel();
            throw error;
          }
          emitEvents(sessionId, [
            {
              id: `${sessionId}:abort-timeout:interrupted:${Date.now()}`,
              sessionId,
              seq: (subscriptions.get(sessionId)?.lastSeq ?? 0) + 1,
              kind: "run.interrupted",
              payload: { transport: "gateway-ws", reason: "chat.abort submitted but ack timed out" },
              createdAt: new Date().toISOString()
            }
          ]);
        }
        await stopSettled.promise;
        return;
      }
      if (action === "rename") {
        await transport.request("sessions.patch", {
          key: sessionId,
          label: title ?? "Renamed session"
        });
        return;
      }
      if (action === "archive") {
        return;
      }
      if (action === "retry") {
        const messages = await this.getSessionMessages(sessionId);
        const lastUserMessage = [...messages].reverse().find((message) => message.role === "user");
        if (!lastUserMessage?.content.trim()) {
          throw new Error("cannot retry a session without a previous user message");
        }
        await this.sendMessage(sessionId, lastUserMessage.content);
      }
    },
    async listEvents(sessionId, afterSeq = 0) {
      const rawMessages = await readChatHistoryPages(transport, sessionId);
      return synthesizeEventsFromHistoryMessages(sessionId, rawMessages, afterSeq);
    },
    subscribe(sessionId, afterSeq, handlers) {
      return subscribeToSession(sessionId, afterSeq, handlers);
    },
    async stop() {
      subscriptions.clear();
      for (const run of responsesRuns.values()) {
        run.abort();
      }
      responsesRuns.clear();
      await transport.stop();
    }
  };
}

export function resolveGatewayConfig(config: Partial<GatewayConfig>): GatewayConfig {
  return {
    baseUrl: normalizeGatewayUrl(String(config.baseUrl ?? "")),
    botId: String(config.botId ?? ""),
    secret: String(config.secret ?? ""),
    requestTimeoutMs: Number(config.requestTimeoutMs ?? 15_000),
    streamReconnectMs: Number(config.streamReconnectMs ?? 2_000),
    hostKind: typeof config.hostKind === "string" ? config.hostKind : undefined,
    runtimeRoot: typeof config.runtimeRoot === "string" ? config.runtimeRoot : undefined,
    exposeRawThinking: config.exposeRawThinking !== false,
    preferResponsesApi: config.preferResponsesApi === true,
    modelOverride: typeof config.modelOverride === "string" ? config.modelOverride.trim() : ""
  };
}

function createTransport(config: GatewayConfig): RpcTransport {
  const officialModulePath = resolveOfficialGatewayClientModule(config.runtimeRoot);
  const useOfficial = shouldUseOfficialGatewayClient(config);
  console.info(`[gateway-transport] baseUrl=${config.baseUrl} officialModulePath=${officialModulePath} useOfficial=${useOfficial} portCheck=${new URL(config.baseUrl).port === "28789"}`);
  if (officialModulePath && useOfficial) {
    console.info(`[gateway-transport] USING official transport: ${officialModulePath}`);
    return createOfficialTransport(config, officialModulePath);
  }
  console.info(`[gateway-transport] USING RpcSocketClient (officialModulePath=${!!officialModulePath} useOfficial=${useOfficial})`);
  return new RpcSocketClient(config);
}

function shouldUseOfficialGatewayClient(config: GatewayConfig): boolean {
  try {
    const url = new URL(config.baseUrl);
    const port = url.port;
    const result = port === "28789";
    console.info(`[gateway-shouldUseOfficial] url=${config.baseUrl} parsedPort=${port} is28789=${result}`);
    return result;
  } catch (err) {
    console.info(`[gateway-shouldUseOfficial] url=${config.baseUrl} PARSE_ERROR: ${err instanceof Error ? err.message : String(err)}`);
    return false;
  }
}

function resolveOfficialGatewayClientModule(runtimeRoot?: string): string | null {
  const candidates = [
    runtimeRoot ? join(runtimeRoot, "node_modules", "openclaw", "dist") : null,
    runtimeRoot ? resolve(runtimeRoot, "..", "..", "..", "node_modules", "openclaw", "dist") : null,
    process.env.HOME
      ? join(process.env.HOME, "Library", "Application Support", "QClaw", "openclaw", "node_modules", "openclaw", "dist")
      : null
  ].filter((value): value is string => Boolean(value));

  for (const candidate of candidates) {
    const exists = existsSync(candidate);
    console.info(`[gateway-resolveModule] candidate=${candidate} exists=${exists}`);
    if (!exists) {
      continue;
    }
    const file = readdirSync(candidate).find((entry) => /^client-.*\.js$/.test(entry));
    console.info(`[gateway-resolveModule]  foundFile=${file}`);
    if (file) {
      const fullPath = join(candidate, file);
      console.info(`[gateway-resolveModule] RETURNING ${fullPath}`);
      return fullPath;
    }
  }

  console.info(`[gateway-resolveModule] RETURNING null (no module found)`);
  return null;
}

function createOfficialTransport(config: GatewayConfig, modulePath: string): RpcTransport {
  const listeners = new Set<(frame: Extract<GatewayFrame, { type: "event" }>) => void>();
  const disconnectListeners = new Set<(error?: Error) => void>();
  let clientPromise: Promise<any> | null = null;
  let stopped = false;

  const ensureClient = async () => {
    if (clientPromise) {
      return await clientPromise;
    }

    clientPromise = (async () => {
      console.info(`[gateway-official] importing module: ${modulePath}`);
      const module = (await import(pathToFileURL(modulePath).href)) as Record<string, unknown>;
      const exportKeys = Object.keys(module);
      console.info(`[gateway-official] module loaded, exports=${exportKeys.length} keys=${exportKeys.slice(0,15).join(",")}`);
      const GatewayClientCtor = resolveGatewayClientCtor(module);
      console.info(`[gateway-official] resolveGatewayClientCtor type=${typeof GatewayClientCtor} name=${(GatewayClientCtor as any)?.name} proto=${GatewayClientCtor?.prototype?.constructor?.name}`);

      // Log detailed export info if no constructor found
      if (!GatewayClientCtor) {
        const typeSummary = exportKeys.map(k => `${k}:${typeof module[k]}`).join(", ");
        console.info(`[gateway-official] NO CTOR FOUND. All exports: ${typeSummary}`);
        throw new Error(`could not locate GatewayClient export in ${modulePath}`);
      }

      return await new Promise<any>((resolve, reject) => {
        console.info(`[gateway-official] constructing GatewayClient with url=${config.baseUrl}`);
        let client: any;
        try {
          client = new GatewayClientCtor({
          url: config.baseUrl,
          token: config.secret,
          clientName: "cli",
          clientDisplayName: "Claw Control Center",
          clientVersion: "claw-control-center",
          mode: "cli",
          scopes: DEFAULT_SCOPES,
          requestTimeoutMs: config.requestTimeoutMs,
          onHelloOk: () => resolve(client),
          onEvent: (event: { event: string; seq?: number; payload?: Record<string, unknown> }) => {
            listeners.forEach((listener) =>
              listener({
                type: "event",
                event: event.event,
                seq: event.seq,
                payload: event.payload
              })
            );
          },
          onConnectError: (error: Error) => {
            clientPromise = null;
            reject(error);
          },
          onClose: (_code: number, reason: string, error?: Error) => {
            clientPromise = null;
            if (stopped) {
              return;
            }
            const disconnectError =
              error instanceof Error ? error : reason ? new Error(reason) : new Error("gateway client closed");
            disconnectListeners.forEach((listener) => listener(disconnectError));
          }
        });

        client.start();
        console.info(`[gateway-official] client.start() called successfully`);
      } catch (constructErr) {
        const errMsg = constructErr instanceof Error ? constructErr.message : String(constructErr);
        const errStack = constructErr instanceof Error ? constructErr.stack : "";
        console.info(`[gateway-official] NEW/START FAILED: ${errMsg}`);
        console.info(`[gateway-official] NEW/START FAILED stack: ${errStack}`);
        console.info(`[gateway-official] GatewayClientCtor type=${typeof GatewayClientCtor} name=${(GatewayClientCtor as any)?.name}`);
        console.info(`[gateway-official] GatewayClientCtor prototype=${Object.prototype.toString.call(GatewayClientCtor)}`);
        reject(constructErr instanceof Error ? constructErr : new Error(String(constructErr)));
        return;
      }
      });
    })();

    return await clientPromise;
  };

  return {
    async request(method, params, options) {
      const client = await ensureClient();
      return await client.request(method, params, options);
    },
    onEvent(listener) {
      listeners.add(listener);
    },
    onDisconnect(listener) {
      disconnectListeners.add(listener);
    },
    async stop() {
      stopped = true;
      if (!clientPromise) {
        return;
      }
      const client = await clientPromise.catch(() => null);
      clientPromise = null;
      if (client && typeof client.stopAndWait === "function") {
        await client.stopAndWait({ timeoutMs: 1_000 }).catch(() => client.stop());
        return;
      }
      if (client && typeof client.stop === "function") {
        client.stop();
      }
    }
  };
}

function resolveGatewayClientCtor(module: Record<string, unknown>): (new (opts: any) => any) | null {
  const directCandidates = [module.GatewayClient, module.default, module.t];
  for (let i = 0; i < directCandidates.length; i++) {
    const candidate = directCandidates[i];
    const names = ["GatewayClient", "default", "t"];
    console.info(`[gateway-resolveCtor] direct[${names[i]}] type=${typeof candidate} isFunction=${typeof candidate === "function"}`);
    if (typeof candidate === "function") {
      return candidate as new (opts: any) => any;
    }
  }

  const allValues = Object.values(module);
  for (let i = 0; i < allValues.length; i++) {
    const candidate = allValues[i];
    if (typeof candidate === "function") {
      console.info(`[gateway-resolveCtor] fallback[${i}] key=${Object.keys(module)[i]} name=${(candidate as any).name} type=${typeof candidate}`);
      return candidate as new (opts: any) => any;
    }
  }

  console.info(`[gateway-resolveCtor] NO FUNCTION FOUND in ${Object.keys(module).length} exports`);
  return null;
}

class ResponsesApiUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ResponsesApiUnavailableError";
  }
}

type StartResponsesApiRunInput = {
  config: GatewayConfig;
  sessionId: string;
  content: string;
  activeRuns: Map<string, AbortController>;
  nextSeq: () => number;
  emit: (event: GatewayEvent) => void;
};

async function startResponsesApiRun(input: StartResponsesApiRunInput): Promise<void> {
  const controller = new AbortController();
  const runId = `responses:${randomUUID()}`;
  const response = await fetch(responsesApiUrl(input.config.baseUrl), {
    method: "POST",
    signal: controller.signal,
    headers: responsesApiHeaders(input.config, input.sessionId),
    body: JSON.stringify({
      model: "openclaw",
      stream: true,
      input: input.content,
      user: input.sessionId,
      metadata: {
        source: "claw-control-center",
        sessionId: input.sessionId
      }
    })
  }).catch((error) => {
    throw new ResponsesApiUnavailableError(error instanceof Error ? error.message : String(error));
  });

  if (response.status === 404 || response.status === 405 || response.status === 501) {
    await response.body?.cancel().catch(() => {});
    throw new ResponsesApiUnavailableError(`responses api unavailable: HTTP ${response.status}`);
  }
  if (!response.ok) {
    const errorText = await response.text().catch(() => "");
    throw new Error(`responses api failed: HTTP ${response.status}${errorText ? ` ${errorText}` : ""}`);
  }

  input.activeRuns.get(input.sessionId)?.abort();
  input.activeRuns.set(input.sessionId, controller);
  void consumeResponsesApiStream({
    response,
    controller,
    runId,
    sessionId: input.sessionId,
    exposeRawThinking: input.config.exposeRawThinking !== false,
    nextSeq: input.nextSeq,
    emit: input.emit
  }).finally(() => {
    if (input.activeRuns.get(input.sessionId) === controller) {
      input.activeRuns.delete(input.sessionId);
    }
  });
}

function responsesApiUrl(baseUrl: string): string {
  const parsed = new URL(baseUrl);
  if (parsed.protocol === "ws:") {
    parsed.protocol = "http:";
  } else if (parsed.protocol === "wss:") {
    parsed.protocol = "https:";
  }
  parsed.pathname = "/v1/responses";
  parsed.search = "";
  parsed.hash = "";
  return parsed.toString();
}

function responsesApiHeaders(config: GatewayConfig, sessionId: string): Record<string, string> {
  const headers: Record<string, string> = {
    "Accept": "text/event-stream",
    "Content-Type": "application/json",
    "x-openclaw-agent-id": "main",
    "x-openclaw-session-key": sessionId,
    "x-openclaw-message-channel": "claw-control-center"
  };
  if (config.secret) {
    headers.Authorization = `Bearer ${config.secret}`;
  }
  if (config.modelOverride) {
    headers["x-openclaw-model"] = config.modelOverride;
  }
  return headers;
}

function isResponsesApiUnavailableError(error: unknown): boolean {
  return error instanceof ResponsesApiUnavailableError;
}

type ConsumeResponsesApiStreamInput = {
  response: Response;
  controller: AbortController;
  runId: string;
  sessionId: string;
  exposeRawThinking: boolean;
  nextSeq: () => number;
  emit: (event: GatewayEvent) => void;
};

async function consumeResponsesApiStream(input: ConsumeResponsesApiStreamInput): Promise<void> {
  let renderedText = "";
  let completed = false;
  let started = false;
  let finalMessageEmitted = false;

  const emit = (kind: string, payload: Record<string, unknown>) => {
    input.emit({
      id: `${input.sessionId}:responses:${kind}:${input.nextSeq()}`,
      sessionId: input.sessionId,
      seq: input.nextSeq(),
      kind,
      payload: {
        ...payload,
        transport: "responses-http",
        runId: input.runId
      },
      createdAt: new Date().toISOString()
    });
  };

  try {
    for await (const sse of iterSseEvents(input.response)) {
      if (sse.done) {
        break;
      }
      const data = parseSseData(sse.data);
      const type = sse.event || readStringFromUnknown(data, ["type"]) || "";
      if (!type) {
        continue;
      }

      if (type === "response.created" || type === "response.in_progress") {
        if (!started) {
          emit("run.started", { eventType: type, responseId: readStringFromUnknown(data, ["response", "id"]) });
          started = true;
        }
        continue;
      }

      if (isReasoningSseEvent(type)) {
        const thinking = extractResponsesTextDelta(data);
        if (thinking.trim()) {
          emit(
            "assistant.thinking",
            buildThinkingPayload(thinking, input.exposeRawThinking, {
              eventType: type,
              state: type.endsWith(".done") ? "final" : "delta",
              mode: type.endsWith(".done") ? "replace" : "append",
              replace: type.endsWith(".done")
            })
          );
        }
        continue;
      }

      if (type === "response.output_text.delta") {
        const text = extractResponsesTextDelta(data);
        if (text.trim()) {
          for (const chunk of splitDeltaText(text)) {
            renderedText += chunk;
            emit("assistant.delta", {
              content: chunk,
              state: "delta",
              mode: "append",
              replace: false,
              eventType: type
            });
          }
        }
        continue;
      }

      if (type === "response.output_text.done") {
        const text = extractResponsesTextDelta(data) || renderedText;
        if (text.trim()) {
          finalMessageEmitted = true;
          emit("assistant.message", {
            content: normalizeFinalAssistantContent(text, renderedText),
            state: "final",
            mode: "replace",
            replace: true,
            eventType: type
          });
        }
        continue;
      }

      const toolEvent = mapResponsesToolEvent(input.sessionId, input.nextSeq(), type, data);
      if (toolEvent) {
        input.emit(toolEvent);
        continue;
      }

      if (type === "response.completed") {
        completed = true;
        if (!finalMessageEmitted) {
          const text = extractResponseOutputText(data) || renderedText;
          if (text.trim()) {
            emit("assistant.message", {
              content: normalizeFinalAssistantContent(text, renderedText),
              state: "final",
              mode: "replace",
              replace: true,
              eventType: type
            });
          }
        }
        emit("run.completed", {
          eventType: type,
          responseId: readStringFromUnknown(data, ["response", "id"]) ?? readStringFromUnknown(data, ["id"]),
          usage: readUnknownPath(data, ["response", "usage"]) ?? readUnknownPath(data, ["usage"])
        });
        continue;
      }

      if (type === "response.failed") {
        completed = true;
        emit("run.failed", {
          eventType: type,
          error:
            readStringFromUnknown(data, ["error", "message"]) ??
            readStringFromUnknown(data, ["response", "error", "message"]) ??
            "responses api stream failed"
        });
      }
    }

    if (!completed && !input.controller.signal.aborted) {
      emit("run.completed", { eventType: "response.stream.done" });
    }
  } catch (error) {
    if (input.controller.signal.aborted) {
      return;
    }
    emit("run.failed", {
      eventType: "response.stream.error",
      error: error instanceof Error ? error.message : String(error)
    });
  }
}

type SseEvent = {
  event: string;
  data: string;
  done: boolean;
};

async function* iterSseEvents(response: Response): AsyncGenerator<SseEvent> {
  const body = response.body;
  if (!body) {
    return;
  }

  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });

    let separatorIndex = findSseSeparator(buffer);
    while (separatorIndex >= 0) {
      const rawEvent = buffer.slice(0, separatorIndex);
      buffer = buffer.slice(buffer[separatorIndex] === "\r" ? separatorIndex + 4 : separatorIndex + 2);
      const event = parseSseEvent(rawEvent);
      if (event) {
        yield event;
      }
      separatorIndex = findSseSeparator(buffer);
    }
  }

  buffer += decoder.decode();
  const trailing = parseSseEvent(buffer);
  if (trailing) {
    yield trailing;
  }
}

function findSseSeparator(buffer: string): number {
  const lf = buffer.indexOf("\n\n");
  const crlf = buffer.indexOf("\r\n\r\n");
  if (lf < 0) {
    return crlf;
  }
  if (crlf < 0) {
    return lf;
  }
  return Math.min(lf, crlf);
}

function parseSseEvent(raw: string): SseEvent | null {
  const lines = raw.split(/\r?\n/);
  let event = "";
  const data: string[] = [];
  for (const line of lines) {
    if (!line || line.startsWith(":")) {
      continue;
    }
    if (line.startsWith("event:")) {
      event = line.slice("event:".length).trim();
    } else if (line.startsWith("data:")) {
      data.push(line.slice("data:".length).trimStart());
    }
  }
  const joinedData = data.join("\n");
  if (!event && !joinedData) {
    return null;
  }
  return {
    event,
    data: joinedData,
    done: joinedData === "[DONE]"
  };
}

function parseSseData(data: string): unknown {
  if (!data || data === "[DONE]") {
    return {};
  }
  try {
    return JSON.parse(data);
  } catch {
    return { text: data };
  }
}

function isReasoningSseEvent(type: string): boolean {
  const normalized = type.toLowerCase();
  return normalized.includes("reasoning") || normalized.includes("thinking");
}

function extractResponsesTextDelta(data: unknown): string {
  return (
    readStringFromUnknown(data, ["delta"]) ??
    readStringFromUnknown(data, ["text"]) ??
    readStringFromUnknown(data, ["content"]) ??
    readStringFromUnknown(data, ["item", "content"]) ??
    readStringFromUnknown(data, ["part", "text"]) ??
    ""
  );
}

function extractResponseOutputText(data: unknown): string {
  const direct =
    readStringFromUnknown(data, ["output_text"]) ??
    readStringFromUnknown(data, ["response", "output_text"]) ??
    readStringFromUnknown(data, ["text"]) ??
    readStringFromUnknown(data, ["response", "text"]);
  if (direct) {
    return direct;
  }

  const output = readUnknownPath(data, ["response", "output"]) ?? readUnknownPath(data, ["output"]);
  if (!Array.isArray(output)) {
    return "";
  }

  return output
    .flatMap((item) => {
      const content = readUnknownPath(item, ["content"]);
      if (!Array.isArray(content)) {
        return [];
      }
      return content.flatMap((part) => {
        const text = readStringFromUnknown(part, ["text"]) ?? readStringFromUnknown(part, ["content"]);
        return text ? [text] : [];
      });
    })
    .join("");
}

function mapResponsesToolEvent(
  sessionId: string,
  seq: number,
  eventType: string,
  data: unknown
): GatewayEvent | null {
  const item = toRecord(readUnknownPath(data, ["item"]) ?? readUnknownPath(data, ["output_item"]));
  const type = typeof item.type === "string" ? item.type : "";
  if (!type.includes("function_call")) {
    return null;
  }

  const name = typeof item.name === "string" ? item.name : "function";
  const kind = eventType.endsWith(".done") ? "tool.result" : "tool.call";
  return {
    id: `${sessionId}:responses:${kind}:${seq}`,
    sessionId,
    seq,
    kind,
    payload: {
      data: {
        name,
        args: item.arguments,
        result: item.output
      },
      eventType,
      transport: "responses-http"
    },
    createdAt: new Date().toISOString()
  };
}

function readStringFromUnknown(value: unknown, path: string[]): string | undefined {
  const current = readUnknownPath(value, path);
  return typeof current === "string" && current.trim() ? current : undefined;
}

function isGatewayRequestTimeoutFor(error: unknown, method: string): boolean {
  return error instanceof Error && error.message === `gateway request timeout for ${method}`;
}

function readUnknownPath(value: unknown, path: string[]): unknown {
  let current = value;
  for (const segment of path) {
    if (!current || typeof current !== "object" || !(segment in current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}

function createGatewayFrameError(error: Extract<GatewayFrame, { type: "res" }>["error"]): Error {
  const message = error?.message ?? "gateway request failed";
  const details = error?.details !== undefined ? `: ${JSON.stringify(error.details)}` : "";
  return new Error(`${message}${details}`);
}

function createGatewayHandshakeError(error: Error): Error {
  if (!/protocol mismatch/i.test(error.message)) {
    return error;
  }
  return new Error(`Gateway protocol negotiation failed: ${error.message}`);
}

function createUnsupportedGatewayProtocolError(expectedProtocol: number, originalError: Error): Error {
  return new Error(
    `Gateway protocol ${expectedProtocol} is not supported by this client; supported range is ` +
      `${GATEWAY_PROTOCOL_MIN}-${GATEWAY_PROTOCOL_MAX}. Original error: ${originalError.message}`
  );
}

function extractExpectedGatewayProtocol(
  error: Extract<GatewayFrame, { type: "res" }>["error"],
  fallbackMessage?: string
): number | null {
  const fromDetails = readExpectedProtocolValue(error?.details);
  if (fromDetails !== null) {
    return fromDetails;
  }

  return readExpectedProtocolFromText([error?.message, fallbackMessage].filter(Boolean).join(" "));
}

function readExpectedProtocolValue(value: unknown, depth = 0): number | null {
  if (value === null || value === undefined || depth > 4) {
    return null;
  }

  if (typeof value === "string") {
    const fromText = readExpectedProtocolFromText(value);
    if (fromText !== null) {
      return fromText;
    }
    try {
      return readExpectedProtocolValue(JSON.parse(value), depth + 1);
    } catch {
      return null;
    }
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const match = readExpectedProtocolValue(item, depth + 1);
      if (match !== null) {
        return match;
      }
    }
    return null;
  }

  if (typeof value !== "object") {
    return null;
  }

  const record = value as Record<string, unknown>;
  for (const key of ["expectedProtocol", "expected_protocol"]) {
    const match = coerceGatewayProtocol(record[key]);
    if (match !== null) {
      return match;
    }
  }

  for (const item of Object.values(record)) {
    const match = readExpectedProtocolValue(item, depth + 1);
    if (match !== null) {
      return match;
    }
  }
  return null;
}

function readExpectedProtocolFromText(value: string): number | null {
  const match = value.match(/["']?expectedProtocol["']?\s*[:=]\s*["']?(\d+)["']?/i);
  return match ? coerceGatewayProtocol(match[1]) : null;
}

function coerceGatewayProtocol(value: unknown): number | null {
  const protocol = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
  return Number.isInteger(protocol) ? protocol : null;
}

function isSupportedGatewayProtocol(protocol: number): boolean {
  return protocol >= GATEWAY_PROTOCOL_MIN && protocol <= GATEWAY_PROTOCOL_MAX;
}

class RpcSocketClient {
  private readonly listeners = new Set<(frame: Extract<GatewayFrame, { type: "event" }>) => void>();
  private readonly disconnectListeners = new Set<(error?: Error) => void>();
  private readonly pending = new Map<string, PendingRequest>();
  private socket: WebSocket | null = null;
  private connectPromise: Promise<void> | null = null;
  private ready = false;
  private challengeNonce: string | null = null;
  private connectProtocolOverride: number | null = null;
  private connectProtocolRetryUsed = false;
  private stopped = false;

  constructor(private readonly config: GatewayConfig) {}

  onEvent(listener: (frame: Extract<GatewayFrame, { type: "event" }>) => void) {
    this.listeners.add(listener);
  }

  onDisconnect(listener: (error?: Error) => void) {
    this.disconnectListeners.add(listener);
  }

  async request(method: string, params: Record<string, unknown>, options?: GatewayRequestOptions): Promise<any> {
    await this.ensureConnected();
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      throw new Error("gateway websocket is not connected");
    }

    const id = randomUUID();
    const frame = {
      type: "req",
      id,
      method,
      params
    };

    const timeoutMs =
      options?.timeoutMs === null ? null : Math.max(1, Math.floor(options?.timeoutMs ?? this.config.requestTimeoutMs));

    const response = new Promise<unknown>((resolve, reject) => {
      const timeout =
        timeoutMs === null
          ? null
          : setTimeout(() => {
              this.pending.delete(id);
              reject(new Error(`gateway request timeout for ${method}`));
            }, timeoutMs);
      this.pending.set(id, { resolve, reject, timeout });
    });

    this.socket.send(JSON.stringify(frame));
    return await response;
  }

  async stop(): Promise<void> {
    this.stopped = true;
    const socket = this.socket;
    this.socket = null;
    this.ready = false;
    this.challengeNonce = null;
    this.connectPromise = null;
    if (!socket) {
      return;
    }

    await new Promise<void>((resolve) => {
      socket.once("close", () => resolve());
      socket.close();
    });
  }

  private async ensureConnected(): Promise<void> {
    if (this.ready && this.socket?.readyState === WebSocket.OPEN) {
      return;
    }
    if (this.connectPromise) {
      await this.connectPromise;
      return;
    }
    this.connectPromise = this.connect();
    try {
      await this.connectPromise;
    } finally {
      this.connectPromise = null;
    }
  }

  private async connect(): Promise<void> {
    if (this.stopped) {
      throw new Error("gateway client stopped");
    }

    await new Promise<void>((resolve, reject) => {
      const socket = new WebSocket(this.config.baseUrl);
      this.socket = socket;
      this.ready = false;
      this.challengeNonce = null;
      this.connectProtocolRetryUsed = false;
      let settled = false;

      const finishError = (error: Error) => {
        if (settled) {
          return;
        }
        settled = true;
        if (this.socket === socket) {
          this.socket = null;
        }
        if (socket.readyState === WEBSOCKET_CONNECTING || socket.readyState === WebSocket.OPEN) {
          socket.close();
        }
        this.ready = false;
        this.challengeNonce = null;
        reject(error);
      };

      const finishReady = () => {
        if (settled) {
          return;
        }
        settled = true;
        resolve();
      };

      socket.on("open", () => {});
      socket.on("message", (raw) => {
        try {
          const frame = JSON.parse(raw.toString()) as GatewayFrame;
          this.handleFrame(frame, finishReady, finishError);
        } catch (error) {
          finishError(error instanceof Error ? error : new Error(String(error)));
        }
      });
      socket.on("error", (error) => {
        finishError(error instanceof Error ? error : new Error(String(error)));
      });
      socket.on("close", (_code, reason) => {
        const error = reason.length > 0 ? new Error(reason.toString()) : undefined;
        this.flushPending(error ?? new Error("gateway websocket closed"));
        this.ready = false;
        this.challengeNonce = null;
        if (this.socket === socket) {
          this.socket = null;
        }
        if (!settled) {
          finishError(error ?? new Error("gateway websocket closed before handshake completed"));
        }
        for (const listener of this.disconnectListeners) {
          listener(error);
        }
      });
    });
  }

  private handleFrame(
    frame: GatewayFrame,
    markReady: () => void,
    rejectConnect: (error: Error) => void
  ) {
    if (frame.type === "event") {
      if (frame.event === "connect.challenge") {
        const nonce = typeof frame.payload?.nonce === "string" ? frame.payload.nonce : null;
        if (!nonce) {
          rejectConnect(new Error("gateway connect challenge missing nonce"));
          return;
        }
        this.challengeNonce = nonce;
        this.sendConnect();
        return;
      }

      for (const listener of this.listeners) {
        listener(frame);
      }
      return;
    }

    const pending = this.pending.get(frame.id);
    if (!pending) {
      return;
    }

    this.pending.delete(frame.id);
    if (pending.timeout) {
      clearTimeout(pending.timeout);
    }

    if (!frame.ok) {
      const error = createGatewayFrameError(frame.error);
      if (frame.id === "connect-handshake") {
        const expectedProtocol = extractExpectedGatewayProtocol(frame.error, error.message);
        if (expectedProtocol !== null && !isSupportedGatewayProtocol(expectedProtocol)) {
          rejectConnect(createUnsupportedGatewayProtocolError(expectedProtocol, error));
          return;
        }

        if (expectedProtocol !== null && !this.connectProtocolRetryUsed) {
          this.connectProtocolOverride = expectedProtocol;
          this.connectProtocolRetryUsed = true;
          this.sendConnect();
          return;
        }

        rejectConnect(createGatewayHandshakeError(error));
        return;
      }
      pending.reject(error);
      return;
    }

    if (frame.id === "connect-handshake") {
      this.ready = true;
      pending.resolve(frame.payload);
      markReady();
      return;
    }

    pending.resolve(frame.payload);
  }

  private sendConnect() {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN || !this.challengeNonce) {
      return;
    }

    const minProtocol = this.connectProtocolOverride ?? GATEWAY_PROTOCOL_MIN;
    const maxProtocol = this.connectProtocolOverride ?? GATEWAY_PROTOCOL_MAX;
    const frame = {
      type: "req",
      id: "connect-handshake",
      method: "connect",
      params: {
        minProtocol,
        maxProtocol,
        client: {
          id: "gateway-client",
          displayName: "Claw Control Center",
          version: "claw-control-center",
          platform: process.platform,
          mode: "backend",
          instanceId: "claw-control-center"
        },
        role: "operator",
        scopes: DEFAULT_SCOPES,
        auth: {
          token: this.config.secret
        }
      }
    };

    const timeout = setTimeout(() => {
      this.pending.delete("connect-handshake");
    }, this.config.requestTimeoutMs);
    this.pending.set("connect-handshake", {
      resolve: () => {},
      reject: () => {},
      timeout
    });
    this.socket.send(JSON.stringify(frame));
  }

  private flushPending(error: Error) {
    for (const [id, pending] of this.pending.entries()) {
      if (pending.timeout) {
        clearTimeout(pending.timeout);
      }
      this.pending.delete(id);
      pending.reject(error);
    }
  }
}

function normalizeGatewayUrl(raw: string): string {
  const value = raw.trim();
  if (!value) {
    return "";
  }

  const parsed = new URL(value.includes("://") ? value : `ws://${value}`);
  if (parsed.protocol === "http:") {
    parsed.protocol = "ws:";
  }
  if (parsed.protocol === "https:") {
    parsed.protocol = "wss:";
  }
  if (!parsed.pathname || parsed.pathname === "/") {
    parsed.pathname = "/";
  }
  return parsed.toString().replace(/\/$/, "");
}

function extractSessions(payload: unknown, hostKind = "openclaw"): GatewaySession[] {
  return readSessionArray(payload).map((session) => normalizeSession(session, "Untitled session", hostKind));
}

async function readSessionListPage(
  transport: RpcTransport,
  hostKind: string,
  options: { limit: number; offset: number }
): Promise<GatewaySessionPage> {
  const limit = clampPositiveInteger(options.limit, SESSION_LIST_PAGE_LIMIT);
  const offset = Math.max(0, Math.floor(options.offset));
  const fetchLimit = offset + limit;
  const payload = await transport.request("sessions.list", {
    limit: fetchLimit,
    includeGlobal: true,
    includeUnknown: true,
    includeDerivedTitles: true,
    includeLastMessage: true
  });
  const fetchedSessions = extractSessions(payload, hostKind);
  const sessions = fetchedSessions.slice(offset, offset + limit);
  return {
    sessions,
    pagination: extractPagination(payload, {
      limit,
      offset,
      itemCount: sessions.length
    })
  };
}

async function readSessionListPages(
  transport: RpcTransport,
  hostKind: string,
  maxItems: number
): Promise<GatewaySession[]> {
  const target = clampPositiveInteger(maxItems, SESSION_LIST_PAGE_LIMIT);
  const sessions: GatewaySession[] = [];
  const seen = new Set<string>();
  let offset = 0;

  for (let page = 0; page < SESSION_LIST_MAX_PAGES && sessions.length < target; page += 1) {
    const pageLimit = Math.min(SESSION_LIST_PAGE_LIMIT, target - sessions.length);
    const result = await readSessionListPage(transport, hostKind, { limit: pageLimit, offset });
    const before = sessions.length;
    for (const session of result.sessions) {
      if (!seen.has(session.id)) {
        seen.add(session.id);
        sessions.push(session);
      }
    }
    if (!result.pagination.hasMore || sessions.length >= target) {
      break;
    }
    const nextOffset = result.pagination.nextOffset;
    if (nextOffset !== undefined && nextOffset > offset) {
      offset = nextOffset;
      continue;
    }
    if (result.sessions.length > 0) {
      offset += result.sessions.length;
      continue;
    }
    if (sessions.length === before) {
      break;
    }
  }

  return sessions;
}

async function findSessionById(
  transport: RpcTransport,
  hostKind: string,
  sessionId: string
): Promise<GatewaySession | undefined> {
  let offset = 0;
  for (let page = 0; page < SESSION_LIST_MAX_PAGES; page += 1) {
    const result = await readSessionListPage(transport, hostKind, {
      limit: SESSION_LIST_PAGE_LIMIT,
      offset
    });
    const match = result.sessions.find((session) => session.id === sessionId);
    if (match || !result.pagination.hasMore) {
      return match;
    }
    const nextOffset = result.pagination.nextOffset;
    if (nextOffset !== undefined && nextOffset > offset) {
      offset = nextOffset;
      continue;
    }
    if (result.sessions.length > 0) {
      offset += result.sessions.length;
      continue;
    }
    break;
  }
  return undefined;
}

function readSessionArray(payload: unknown): unknown[] {
  const record = toRecord(payload);
  const candidates = [record.sessions, record.items, record.data];
  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }
  const nested = toRecord(record.data);
  if (Array.isArray(nested.sessions)) {
    return nested.sessions;
  }
  return [];
}

async function readRuntimeInfo(transport: RpcTransport): Promise<GatewayRuntimeInfo> {
  try {
    const payload = await transport.request("skills.status", { agentId: "main" }, { timeoutMs: 2_000 });
    return extractRuntimeInfo(payload);
  } catch {
    return {
      enabledSkills: []
    };
  }
}

async function readCronInfo(
  transport: RpcTransport
): Promise<Pick<GatewayRuntimeInfo, "cronScheduler" | "cronTasks">> {
  const [statusResult, listResult] = await Promise.allSettled([
    transport.request("cron.status", {}, { timeoutMs: 2_000 }),
    readCronListPages(transport)
  ]);
  const statusPayload = statusResult.status === "fulfilled" ? statusResult.value : undefined;
  const listPayload = listResult.status === "fulfilled" ? listResult.value : undefined;

  if (!statusPayload && !listPayload) {
    return {};
  }

  const cronTasks = extractCronTasks(listPayload);
  const cronScheduler = extractCronScheduler(statusPayload, listPayload, cronTasks.length);
  return {
    ...(cronScheduler ? { cronScheduler } : {}),
    cronTasks
  };
}

async function readCronListPages(transport: RpcTransport): Promise<unknown> {
  const jobs: unknown[] = [];
  let offset = 0;
  let lastPayload: unknown = undefined;

  for (let page = 0; page < CRON_LIST_MAX_PAGES; page += 1) {
    const payload = await transport.request(
      "cron.list",
      {
        includeDisabled: true,
        limit: CRON_LIST_PAGE_LIMIT,
        offset
      },
      { timeoutMs: 2_000 }
    );
    lastPayload = payload;

    const pageJobs = readCronJobArray(payload);
    jobs.push(...pageJobs);

    const record = toRecord(payload);
    const nextOffset = numberProperty(record, ["nextOffset"]);
    const total = numberProperty(record, ["total", "count"]);
    const hasMore = booleanProperty(record, ["hasMore"]) ?? (total !== undefined && jobs.length < total);

    if (!hasMore) {
      break;
    }
    if (nextOffset !== undefined && nextOffset > offset) {
      offset = nextOffset;
      continue;
    }
    if (pageJobs.length > 0) {
      offset += pageJobs.length;
      continue;
    }
    break;
  }

  return {
    ...toRecord(lastPayload),
    jobs
  };
}

function extractRuntimeInfo(payload: unknown): GatewayRuntimeInfo {
  const record = toRecord(payload);
  const modelPrimary =
    readStringFromUnknown(record, ["modelPrimary"]) ??
    readStringFromUnknown(record, ["model", "primary"]) ??
    readStringFromUnknown(record, ["models", "primary"]) ??
    readStringFromUnknown(record, ["agent", "model", "primary"]) ??
    readStringFromUnknown(record, ["defaults", "model", "primary"]) ??
    (typeof record.model === "string" && record.model.trim() ? record.model.trim() : undefined);
  const enabledSkills = collectRuntimeSkills(
    record.skills ?? record.enabledSkills ?? record.items ?? record.entries ?? readUnknownPath(record, ["data", "skills"])
  );

  return {
    ...(modelPrimary ? { modelPrimary } : {}),
    enabledSkills
  };
}

function extractGatewayHealth(payload: unknown): GatewayHealthSnapshot {
  const record = toRecord(payload);
  const ok = booleanProperty(record, ["ok", "healthy"]);
  const rawStatus = stringProperty(record, ["status", "state"]);
  const status = normalizeGatewayHealthStatus(ok, rawStatus);
  const checkedAt = optionalIsoString(record.ts ?? record.timestamp ?? record.checkedAt ?? record.time);
  const durationMs = numberProperty(record, ["durationMs", "latencyMs"]);
  const lastError =
    readStringFromUnknown(record, ["lastError"]) ??
    readStringFromUnknown(record, ["error"]) ??
    readStringFromUnknown(record, ["error", "message"]);

  return {
    ...(ok !== undefined ? { ok } : {}),
    status,
    ...(checkedAt ? { checkedAt } : {}),
    ...(durationMs !== undefined ? { durationMs } : {}),
    ...(lastError ? { lastError } : {})
  };
}

function normalizeGatewayHealthStatus(
  ok: boolean | undefined,
  rawStatus: string | undefined
): GatewayHealthSnapshot["status"] {
  const normalized = rawStatus?.toLowerCase();
  if (normalized === "ok" || normalized === "healthy" || normalized === "ready") {
    return "ok";
  }
  if (normalized === "degraded" || normalized === "warning" || normalized === "warn") {
    return "degraded";
  }
  if (normalized === "error" || normalized === "failed" || normalized === "unhealthy") {
    return "error";
  }
  if (ok === true) {
    return "ok";
  }
  if (ok === false) {
    return "error";
  }
  return "unknown";
}

function extractCronScheduler(
  statusPayload: unknown,
  listPayload: unknown,
  observedTaskCount: number
): GatewayCronScheduler | undefined {
  const status = toRecord(statusPayload);
  const list = toRecord(listPayload);
  const enabled =
    booleanProperty(status, ["enabled", "running"]) ??
    booleanProperty(status, ["schedulerEnabled"]) ??
    booleanProperty(list, ["enabled"]);
  const storePath =
    stringProperty(status, ["storePath", "path"]) ??
    readStringFromUnknown(status, ["store", "path"]) ??
    readStringFromUnknown(list, ["store", "path"]);
  const jobCount =
    numberProperty(status, ["jobCount", "jobs", "count", "total"]) ??
    numberProperty(list, ["total", "count", "jobCount"]) ??
    observedTaskCount;
  const nextWakeAt =
    optionalIsoString(readUnknownPath(status, ["nextWakeAt"])) ??
    optionalIsoString(readUnknownPath(status, ["nextWakeAtMs"])) ??
    optionalIsoString(readUnknownPath(status, ["nextRunAt"])) ??
    optionalIsoString(readUnknownPath(status, ["nextRunAtMs"]));
  const lastError =
    readStringFromUnknown(status, ["lastError"]) ??
    readStringFromUnknown(status, ["error"]) ??
    readStringFromUnknown(list, ["error"]);

  if (
    enabled === undefined &&
    !storePath &&
    jobCount === undefined &&
    !nextWakeAt &&
    !lastError &&
    observedTaskCount === 0
  ) {
    return undefined;
  }

  return {
    ...(enabled !== undefined ? { enabled } : {}),
    ...(storePath ? { storePath } : {}),
    ...(jobCount !== undefined ? { jobCount } : {}),
    ...(nextWakeAt ? { nextWakeAt } : {}),
    ...(lastError ? { lastError } : {})
  };
}

function extractCronTasks(payload: unknown): GatewayCronTask[] {
  const jobs = readCronJobArray(payload);
  return jobs.map((job, index) => normalizeCronTask(job, index));
}

function readCronJobArray(payload: unknown): unknown[] {
  const record = toRecord(payload);
  const candidates = [
    record.jobs,
    record.tasks,
    record.items,
    readUnknownPath(record, ["data", "jobs"]),
    readUnknownPath(record, ["data", "tasks"]),
    Array.isArray(payload) ? payload : undefined
  ];
  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }
  return [];
}

function normalizeCronTask(raw: unknown, index: number): GatewayCronTask {
  const job = toRecord(raw);
  const id = stringProperty(job, ["id", "jobId", "key", "name"]) ?? `cron-${index + 1}`;
  const name = stringProperty(job, ["name", "title", "description"]) ?? id;
  const status = stringProperty(job, ["status", "state"]);
  const enabled = booleanProperty(job, ["enabled"]) ?? status?.toLowerCase() !== "disabled";
  const agentId = stringProperty(job, ["agentId", "agent", "owner"]);
  const schedule = formatCronSchedule(job.schedule ?? job.cron ?? job.expr ?? job.everyMs ?? job.at ?? job.atMs);
  const nextRunAt =
    optionalIsoString(readUnknownPath(job, ["nextRunAt"])) ??
    optionalIsoString(readUnknownPath(job, ["nextRunAtMs"])) ??
    optionalIsoString(readUnknownPath(job, ["nextAt"])) ??
    optionalIsoString(readUnknownPath(job, ["nextAtMs"])) ??
    optionalIsoString(readUnknownPath(job, ["nextWakeAtMs"]));
  const lastRunAt =
    optionalIsoString(readUnknownPath(job, ["lastRunAt"])) ??
    optionalIsoString(readUnknownPath(job, ["lastRunAtMs"])) ??
    optionalIsoString(readUnknownPath(job, ["lastAt"])) ??
    optionalIsoString(readUnknownPath(job, ["lastAtMs"]));
  const payloadKind =
    readStringFromUnknown(job, ["payloadKind"]) ??
    readStringFromUnknown(job, ["payload", "kind"]) ??
    readStringFromUnknown(job, ["task", "kind"]);

  return {
    id,
    name,
    enabled,
    ...(status ? { status } : {}),
    ...(agentId ? { agentId } : {}),
    ...(schedule ? { schedule } : {}),
    ...(nextRunAt ? { nextRunAt } : {}),
    ...(lastRunAt ? { lastRunAt } : {}),
    ...(payloadKind ? { payloadKind } : {})
  };
}

function formatCronSchedule(value: unknown): string | undefined {
  if (typeof value === "string" && value.trim()) {
    return value.trim();
  }
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    return `every ${formatDurationMs(value)}`;
  }
  if (!value || typeof value !== "object") {
    return undefined;
  }

  const schedule = value as Record<string, unknown>;
  const kind = stringProperty(schedule, ["kind", "type"]);
  if (kind === "cron") {
    const expr = stringProperty(schedule, ["expr", "cron", "expression"]);
    const timezone = stringProperty(schedule, ["timezone", "timeZone", "tz"]);
    return [`cron${expr ? ` ${expr}` : ""}`, timezone].filter(Boolean).join(" · ");
  }
  if (kind === "every") {
    const everyMs = numberProperty(schedule, ["everyMs", "intervalMs", "ms"]);
    return everyMs ? `every ${formatDurationMs(everyMs)}` : "every";
  }
  if (kind === "at") {
    const at = optionalIsoString(schedule.at ?? schedule.atMs);
    return at ? `at ${at}` : "at";
  }

  const expr = stringProperty(schedule, ["expr", "cron", "expression"]);
  if (expr) {
    return expr;
  }
  const everyMs = numberProperty(schedule, ["everyMs", "intervalMs", "ms"]);
  if (everyMs) {
    return `every ${formatDurationMs(everyMs)}`;
  }
  const at = optionalIsoString(schedule.at ?? schedule.atMs);
  return at ? `at ${at}` : undefined;
}

function formatDurationMs(value: number): string {
  if (value % 86_400_000 === 0) {
    return `${value / 86_400_000}d`;
  }
  if (value % 3_600_000 === 0) {
    return `${value / 3_600_000}h`;
  }
  if (value % 60_000 === 0) {
    return `${value / 60_000}m`;
  }
  if (value % 1_000 === 0) {
    return `${value / 1_000}s`;
  }
  return `${value}ms`;
}

function collectRuntimeSkills(value: unknown): string[] {
  const skills: string[] = [];
  const seen = new Set<string>();
  const pushSkill = (skill: string) => {
    const normalized = skill.trim();
    if (!normalized || seen.has(normalized)) {
      return;
    }
    seen.add(normalized);
    skills.push(normalized);
  };

  const visit = (entry: unknown, mapKey?: string) => {
    if (typeof entry === "string") {
      pushSkill(entry);
      return;
    }
    if (Array.isArray(entry)) {
      for (const item of entry) {
        visit(item);
      }
      return;
    }
    if (!entry || typeof entry !== "object") {
      return;
    }

    const record = entry as Record<string, unknown>;
    const nested = record.skills ?? record.items ?? record.entries;
    if (nested && !("enabled" in record) && !("status" in record) && !("available" in record)) {
      visit(nested);
      return;
    }

    const name = stringProperty(record, ["id", "name", "key", "slug"]) ?? mapKey;
    if (name && isRuntimeSkillEnabled(record)) {
      pushSkill(name);
    }
  };

  if (Array.isArray(value)) {
    value.forEach((entry) => visit(entry));
    return skills;
  }

  if (value && typeof value === "object") {
    for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
      visit(entry, key);
    }
    return skills;
  }

  visit(value);
  return skills;
}

function isRuntimeSkillEnabled(record: Record<string, unknown>): boolean {
  if ("enabled" in record) {
    return record.enabled === true;
  }
  if (record.disabled === true) {
    return false;
  }
  if (record.available === true || record.ready === true) {
    return true;
  }
  if (typeof record.status === "string") {
    return ["enabled", "available", "ready", "running", "active"].includes(record.status.toLowerCase());
  }
  return true;
}

function stringProperty(record: Record<string, unknown>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return undefined;
}

function numberProperty(record: Record<string, unknown>, keys: string[]): number | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    if (typeof value === "string" && value.trim()) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        return parsed;
      }
    }
  }
  return undefined;
}

function booleanProperty(record: Record<string, unknown>, keys: string[]): boolean | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "boolean") {
      return value;
    }
  }
  return undefined;
}

function extractPagination(
  payload: unknown,
  fallback: {
    limit: number;
    offset: number;
    itemCount: number;
  }
): GatewayPagination {
  const record = toRecord(payload);
  const limit = numberProperty(record, ["limit", "pageSize"]) ?? fallback.limit;
  const offset = numberProperty(record, ["offset", "skip"]) ?? fallback.offset;
  const total = numberProperty(record, ["total", "count"]);
  const nextOffset = numberProperty(record, ["nextOffset", "next"]);
  const explicitHasMore = booleanProperty(record, ["hasMore", "more"]);
  const inferredHasMore =
    total !== undefined
      ? offset + fallback.itemCount < total
      : fallback.itemCount >= limit && fallback.itemCount > 0;
  const hasMore = explicitHasMore ?? inferredHasMore;
  const fallbackNextOffset = offset + fallback.itemCount;

  return {
    limit,
    offset,
    ...(total !== undefined ? { total } : {}),
    hasMore,
    ...(hasMore ? { nextOffset: nextOffset && nextOffset > offset ? nextOffset : fallbackNextOffset } : {})
  };
}

function clampPositiveInteger(value: number, fallback: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return fallback;
  }
  return Math.max(1, Math.floor(value));
}

function extractCreatedSession(payload: unknown): Record<string, unknown> {
  return typeof payload === "object" && payload ? (payload as Record<string, unknown>) : {};
}

function extractMessages(sessionId: string, payload: unknown): SessionMessage[] {
  return extractMessagesFromRaw(sessionId, readHistoryMessageArray(payload));
}

function extractMessagesFromRaw(sessionId: string, rawMessages: unknown[]): SessionMessage[] {
  return rawMessages
    .map((message, index) => normalizeMessage(sessionId, message, index))
    .filter((message): message is SessionMessage => message !== null);
}

async function readChatHistoryPages(
  transport: RpcTransport,
  sessionId: string,
  maxItems = CHAT_HISTORY_PAGE_LIMIT * CHAT_HISTORY_MAX_PAGES
): Promise<unknown[]> {
  const target = Math.min(
    CHAT_HISTORY_GATEWAY_MAX_LIMIT,
    clampPositiveInteger(maxItems, CHAT_HISTORY_PAGE_LIMIT * CHAT_HISTORY_MAX_PAGES)
  );
  const pages: unknown[][] = [];
  const seen = new Set<string>();
  let offset = 0;
  let collected = 0;

  for (let page = 0; page < CHAT_HISTORY_MAX_PAGES && collected < target; page += 1) {
    const pageLimit = Math.min(CHAT_HISTORY_PAGE_LIMIT, target - collected);
    const fetchLimit = offset + pageLimit;
    const payload = await transport.request("chat.history", {
      sessionKey: sessionId,
      limit: fetchLimit
    });
    const fetchedMessages = readHistoryMessageArray(payload);
    const pageMessages = sliceLatestWindowPage(fetchedMessages, pageLimit, offset);
    const before = collected;
    const uniquePageMessages: unknown[] = [];
    for (const message of pageMessages) {
      const key = messageIdentity(message);
      if (!seen.has(key)) {
        seen.add(key);
        uniquePageMessages.push(message);
      }
    }
    if (uniquePageMessages.length > 0) {
      pages.push(uniquePageMessages);
      collected += uniquePageMessages.length;
    }

    const pagination = extractPagination(payload, {
      limit: pageLimit,
      offset,
      itemCount: pageMessages.length
    });
    if (!pagination.hasMore || collected >= target) {
      break;
    }
    if (pageMessages.length > 0) {
      offset += pageMessages.length;
      continue;
    }
    if (collected === before) {
      break;
    }
  }

  return pages.reverse().flat();
}

function sliceLatestWindowPage<T>(items: T[], limit: number, offset: number): T[] {
  const end = Math.max(0, items.length - offset);
  const start = Math.max(0, end - limit);
  return items.slice(start, end);
}

function readHistoryMessageArray(payload: unknown): unknown[] {
  const record = toRecord(payload);
  const candidates = [record.messages, record.items, record.data];
  for (const candidate of candidates) {
    if (Array.isArray(candidate)) {
      return candidate;
    }
  }
  const nested = toRecord(record.data);
  if (Array.isArray(nested.messages)) {
    return nested.messages;
  }
  return [];
}

function messageIdentity(message: unknown): string {
  const record = toRecord(message);
  const rawMeta = toRecord(record.__openclaw);
  return JSON.stringify({
    role: record.role,
    seq: rawMeta.seq,
    timestamp: record.timestamp,
    content: record.content
  });
}

function normalizeSession(payload: unknown, fallbackTitle = "Untitled session", hostKind = "openclaw"): GatewaySession {
  const entry = payload && typeof payload === "object" ? (payload as Record<string, unknown>) : {};
  const key = String(entry.key ?? entry.sessionKey ?? entry.sessionId ?? randomUUID());
  const updatedAtMs = Number(entry.updatedAt ?? entry.startedAt ?? Date.now());
  const createdAt = toIsoString(entry.startedAt ?? entry.updatedAt ?? Date.now());
  const updatedAt = toIsoString(updatedAtMs);
  const title = readableSessionTitle(entry, fallbackTitle);

  return {
    id: key,
    title,
    status: normalizeStatus(entry.status),
    hostKind,
    runnerCommand: "openclaw-gateway",
    createdAt,
    updatedAt,
    lastEventSeq: Number(entry.messageSeq ?? 0)
  };
}

function readableSessionTitle(entry: Record<string, unknown>, fallbackTitle: string): string {
  const candidates = [entry.label, entry.displayName, entry.key];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate;
    }
  }
  return fallbackTitle;
}

function normalizeMessage(sessionId: string, payload: unknown, index = 0): SessionMessage | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const message = payload as Record<string, unknown>;
  const role = typeof message.role === "string" ? message.role : "assistant";
  const typedTextSegments = extractTextSegments(message.content);
  const content = extractTextContent(message.content) ?? (typeof message.content === "string" ? message.content : "");
  if (!content.trim()) {
    return null;
  }

  const rawMeta =
    message.__openclaw && typeof message.__openclaw === "object"
      ? { ...(message.__openclaw as Record<string, unknown>) }
      : {};
  const seq =
    numberProperty(rawMeta, ["seq", "messageSeq", "message_seq"]) ??
    numberProperty(message, ["seq", "messageSeq", "message_seq"]) ??
    0;
  const messageIdentity = seq > 0
    ? String(seq)
    : stringProperty(message, ["id", "messageId", "message_id"]) ??
      fallbackMessageIdentity(role, content, message.timestamp, index);
  const existingPayload = toRecord(message.payload);
  const existingMetadata = toRecord(message.metadata);
  const existingData = toRecord(message.data);
  const responseId = stringProperty(message, ["responseId", "response_id"]);
  const runId = stringProperty(message, ["runId", "run_id"]);
  const typedTranscriptMeta = {
    ...(typedTextSegments.length > 0
      ? {
          openclaw_typed_text_segments: typedTextSegments,
          openclaw_typed_text_segment_count: typedTextSegments.length
        }
      : {}),
    ...(responseId ? { responseId, response_id: responseId } : {}),
    ...(runId ? { runId, run_id: runId } : {})
  };
  const seqMeta =
    seq > 0
      ? {
          rawSeq: seq,
          seq,
          messageSeq: seq,
          message_seq: seq
        }
      : {};

  const hasTypedTranscript = Object.keys(typedTranscriptMeta).length > 0;
  const hasSeq = seq > 0;

  return {
    id: `${sessionId}:${role}:${messageIdentity}`,
    sessionId,
    role,
    content,
    createdAt: toIsoString(message.timestamp ?? Date.now()),
    ...(hasSeq
      ? {
          seq,
          messageSeq: seq,
          message_seq: seq
        }
      : {}),
    ...(Object.keys(existingPayload).length > 0 || hasTypedTranscript
      ? {
          payload: {
            ...existingPayload,
            ...typedTranscriptMeta
          }
        }
      : {}),
    ...(Object.keys(existingMetadata).length > 0
      ? {
          metadata: {
            ...existingMetadata
          }
        }
      : {}),
    ...(Object.keys(existingData).length > 0
      ? {
          data: {
            ...existingData
          }
        }
      : {}),
    ...(Object.keys(rawMeta).length > 0 || hasSeq
      ? {
          __openclaw: {
            ...rawMeta,
            ...(hasSeq ? { seq } : {})
          }
        }
      : {})
  };
}

function fallbackMessageIdentity(role: string, content: string, timestamp: unknown, index: number): string {
  return createHash("sha1")
    .update(`${role}\0${String(timestamp ?? "")}\0${index}\0${content}`)
    .digest("hex")
    .slice(0, 16);
}

function synthesizeEventsFromHistory(sessionId: string, payload: unknown, afterSeq: number): GatewayEvent[] {
  return synthesizeEventsFromHistoryMessages(sessionId, readHistoryMessageArray(payload), afterSeq);
}

function synthesizeEventsFromHistoryMessages(
  sessionId: string,
  rawMessages: unknown[],
  afterSeq: number
): GatewayEvent[] {
  const expandsCompoundHistory = rawMessages.some(shouldExpandHistoryMessage);
  return rawMessages
    .flatMap((message) => {
      if (!message || typeof message !== "object") {
        return [];
      }

      const entry = message as Record<string, unknown>;
      const role = typeof entry.role === "string" ? entry.role : "";
      const rawSeq = readHistoryMessageSeq(entry);
      const createdAt = toIsoString(entry.timestamp ?? Date.now());

      if (role === "assistant") {
        const events: GatewayEvent[] = [];
        let localIndex = 0;
        const thinking = extractThinkingContent(entry.content);
        if (thinking?.trim()) {
          events.push({
            id: `${sessionId}:history:${rawSeq}:thinking`,
            sessionId,
            seq: historyEventSeq(rawSeq, localIndex, expandsCompoundHistory),
            kind: "assistant.thinking",
            payload: buildThinkingPayload(thinking, true, {
              state: "final",
              mode: "replace",
              replace: true,
              rawSeq
            }),
            createdAt
          });
          localIndex += 1;
        }

        for (const toolCall of extractToolCalls(entry.content)) {
          events.push({
            id: `${sessionId}:history:${rawSeq}:tool-call:${toolCall.id}`,
            sessionId,
            seq: historyEventSeq(rawSeq, localIndex, expandsCompoundHistory),
            kind: "tool.call",
            payload: {
              data: {
                phase: "call",
                name: toolCall.name,
                toolCallId: toolCall.id,
                ...(toolCall.args !== undefined ? { args: toolCall.args } : {}),
                ...(toolCall.meta ? { meta: toolCall.meta } : {})
              },
              rawSeq
            },
            createdAt
          });
          localIndex += 1;
        }

        const content = extractTextContent(entry.content) ?? "";
        if (content.trim()) {
          events.push({
            id: `${sessionId}:history:${rawSeq}:assistant`,
            sessionId,
            seq: historyEventSeq(rawSeq, localIndex, expandsCompoundHistory),
            kind: "assistant.message",
            payload: {
              content
            },
            createdAt
          });
        }

        return events;
      }

      if (isToolResultRole(role)) {
        const events: GatewayEvent[] = [
          {
            id: `${sessionId}:history:${rawSeq}:tool-result:${readHistoryToolResultId(entry)}`,
            sessionId,
            seq: historyEventSeq(rawSeq, 0, expandsCompoundHistory),
            kind: "tool.result",
            payload: buildHistoryToolResultPayload(entry, rawSeq),
            createdAt
          }
        ];
        const outputFilesEvent = buildHistoryOutputFilesEvent(sessionId, rawSeq, createdAt, entry, expandsCompoundHistory);
        if (outputFilesEvent) {
          events.push(outputFilesEvent);
        }
        return events;
      }

      return [];
    })
    .filter((event) => event.seq > afterSeq)
    .sort((left, right) => left.seq - right.seq);
}

function shouldExpandHistoryMessage(message: unknown): boolean {
  if (!message || typeof message !== "object") {
    return false;
  }
  const entry = message as Record<string, unknown>;
  const role = typeof entry.role === "string" ? entry.role : "";
  if (isToolResultRole(role)) {
    return true;
  }
  if (role !== "assistant") {
    return false;
  }
  return Boolean(extractThinkingContent(entry.content)?.trim() || extractToolCalls(entry.content).length > 0);
}

function readHistoryMessageSeq(entry: Record<string, unknown>): number {
  const rawMeta =
    entry.__openclaw && typeof entry.__openclaw === "object" ? (entry.__openclaw as Record<string, unknown>) : {};
  const seq = typeof rawMeta.seq === "number" ? rawMeta.seq : Number(rawMeta.seq);
  return Number.isFinite(seq) ? seq : 0;
}

function historyEventSeq(rawSeq: number, localIndex: number, expanded: boolean): number {
  if (!expanded) {
    return rawSeq;
  }
  if (rawSeq > 0) {
    const composite = rawSeq * 10 + localIndex;
    if (composite === Infinity || !Number.isSafeInteger(composite)) {
      return rawSeq + localIndex;
    }
    return composite;
  }
  return localIndex;
}

function isToolResultRole(role: string): boolean {
  return ["toolresult", "tool_result", "tool"].includes(role.toLowerCase());
}

type HistoryToolCall = {
  id: string;
  name: string;
  args?: unknown;
  meta?: string;
};

function extractToolCalls(content: unknown): HistoryToolCall[] {
  if (!Array.isArray(content)) {
    return [];
  }

  return content.flatMap((item, index) => {
    if (!item || typeof item !== "object") {
      return [];
    }
    const record = item as Record<string, unknown>;
    const type = typeof record.type === "string" ? record.type.toLowerCase().replace(/[_-]/g, "") : "";
    if (type !== "toolcall" && type !== "functioncall") {
      return [];
    }

    const name =
      stringProperty(record, ["name", "toolName", "functionName"]) ??
      readStringFromUnknown(record, ["function", "name"]) ??
      "tool";
    const args = normalizeToolArguments(
      record.arguments ?? record.args ?? record.input ?? record.parameters ?? readUnknownPath(record, ["function", "arguments"])
    );
    const id =
      stringProperty(record, ["id", "toolCallId", "callId"]) ??
      readStringFromUnknown(record, ["function", "id"]) ??
      `${name}:${index}`;

    return [
      {
        id,
        name,
        ...(args !== undefined ? { args } : {}),
        ...(summarizeToolCallMeta(name, args) ? { meta: summarizeToolCallMeta(name, args) } : {})
      }
    ];
  });
}

function normalizeToolArguments(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return trimmed;
  }
}

function normalizeCommandText(value: string): string {
  return value.replace(/\r\n/g, "\n").replace(/\\"/g, "\"").replace(/\\'/g, "'").trim();
}

function isExecToolName(name: string): boolean {
  const normalized = name.replace(/[\s-]+/g, "_").toLowerCase();
  return normalized === "exec" || normalized === "used_exec" || normalized === "bash" || normalized === "shell" || normalized === "run_command";
}

function normalizeToolName(value: string): string {
  const normalized = value.trim();
  return isExecToolName(normalized) ? "exec" : normalized;
}

function readToolCallId(record: Record<string, unknown>): string | undefined {
  return stringProperty(record, ["toolCallId", "tool_call_id", "callId", "call_id", "id"]);
}

function parseToolArgumentsRecord(value: unknown): Record<string, unknown> {
  const normalized = normalizeToolArguments(value);
  return toRecord(normalized);
}

function firstToolArgumentsRecord(...values: unknown[]): Record<string, unknown> {
  for (const value of values) {
    const record = parseToolArgumentsRecord(value);
    if (Object.keys(record).length > 0) {
      return record;
    }
  }
  return {};
}

function readExecCommandFromRecords(...records: Record<string, unknown>[]): string {
  for (const record of records) {
    const command = stringProperty(record, ["command", "cmd", "script", "shell", "commandLine", "command_line", "code"]);
    if (command) {
      return normalizeCommandText(command);
    }
  }
  return "";
}

function firstToolResultValue(...values: unknown[]): unknown {
  for (const value of values) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    return value;
  }
  return undefined;
}

function readTextFromToolResultValue(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (Array.isArray(value)) {
    return value
      .map((entry) => {
        const record = toRecord(entry);
        return stringProperty(record, ["text", "content", "output", "result"]) ?? "";
      })
      .filter(Boolean)
      .join("\n")
      .trim();
  }
  const record = toRecord(value);
  return (
    stringProperty(record, ["output", "content", "result", "aggregated"]) ??
    stringProperty(toRecord(record.details), ["aggregated", "output", "content", "result"]) ??
    ""
  ).trim();
}

function normalizeToolResultValue(value: unknown): unknown {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  const record = toRecord(value);
  const text = readTextFromToolResultValue(value);
  if (!text) {
    return value;
  }
  if (typeof value === "string" || Array.isArray(value)) {
    return { output: text };
  }
  if (record.output === undefined && record.content === undefined && record.result === undefined) {
    return {
      ...record,
      output: text
    };
  }
  return value;
}

function normalizeSessionToolPayload(
  payload: Record<string, unknown>,
  subscription?: SessionSubscription
): Record<string, unknown> {
  const data = toRecord(payload.data);
  const fn = toRecord(data.function ?? payload.function);
  const rawName =
    stringProperty(data, ["name", "toolName", "tool_name", "tool", "displayName", "display_name", "title"]) ??
    stringProperty(payload, ["name", "toolName", "tool_name", "tool"]) ??
    stringProperty(fn, ["name"]) ??
    "tool";
  const name = normalizeToolName(rawName);
  const inherited = lookupSessionToolCallData(data, name, subscription);
  const inheritedArgs = toRecord(inherited.args);
  const args = firstToolArgumentsRecord(
    data.args,
    data.arguments,
    data.input,
    data.parameters,
    data.toolInput,
    data.tool_input,
    payload.args,
    payload.arguments,
    payload.input,
    payload.parameters,
    payload.toolInput,
    payload.tool_input,
    fn.arguments,
    inherited.args
  );
  const command = readExecCommandFromRecords(args, data, payload, inheritedArgs, inherited);
  const normalizedArgs =
    command && isExecToolName(name)
      ? {
          ...args,
          command
        }
      : args;
  const toolCallId =
    readToolCallId(data) ||
    readToolCallId(payload) ||
    readToolCallId(fn) ||
    readToolCallId(inherited);
  const phase = String(payload.phase ?? data.phase ?? "");
  const shouldNormalizeResult =
    ["result", "done", "output"].includes(phase) ||
    data.result !== undefined ||
    data.output !== undefined ||
    data.content !== undefined ||
    payload.result !== undefined ||
    payload.output !== undefined ||
    payload.content !== undefined;
  const normalizedResult = shouldNormalizeResult
    ? normalizeToolResultValue(
        firstToolResultValue(
          data.result,
          data.output,
          data.content,
          data.details,
          payload.result,
          payload.output,
          payload.content,
          payload.details
        )
      )
    : undefined;

  return {
    ...payload,
    data: {
      ...data,
      name,
      ...(toolCallId ? { toolCallId } : {}),
      ...(Object.keys(normalizedArgs).length > 0 ? { args: normalizedArgs } : {}),
      ...(command && isExecToolName(name) ? { command } : {}),
      ...(normalizedResult !== undefined ? { result: normalizedResult } : {})
    }
  };
}

function lookupSessionToolCallData(
  data: Record<string, unknown>,
  name: string,
  subscription?: SessionSubscription
): Record<string, unknown> {
  if (!subscription) {
    return {};
  }
  const callId = readToolCallId(data);
  if (callId) {
    const byId = subscription.toolCallDataById.get(callId);
    if (byId) {
      return byId;
    }
  }
  return subscription.lastToolCallDataByName.get(name) ?? {};
}

function rememberSessionToolCallData(
  eventKind: "tool.call" | "tool.result",
  data: Record<string, unknown>,
  subscription?: SessionSubscription
) {
  if (!subscription || eventKind !== "tool.call") {
    return;
  }
  const name = stringProperty(data, ["name", "toolName", "tool_name", "tool"]) ?? "tool";
  const callId = readToolCallId(data);
  if (callId) {
    subscription.toolCallDataById.set(callId, data);
  }
  subscription.lastToolCallDataByName.set(name, data);
}

function summarizeToolCallMeta(name: string, args: unknown): string | undefined {
  const normalizedName = name.toLowerCase();
  const record = toRecord(args);
  const query = stringProperty(record, ["query", "q"]);
  const url = stringProperty(record, ["url", "href"]);
  const count = numberProperty(record, ["count", "limit", "topK", "top_k"]);
  const maxChars = numberProperty(record, ["maxChars", "max_chars"]);

  if (normalizedName.includes("search") && query) {
    return count ? `for "${query}" (top ${count})` : `for "${query}"`;
  }
  if (normalizedName.includes("fetch") && url) {
    return maxChars ? `from ${url} (max ${maxChars} chars)` : `from ${url}`;
  }
  if (query) {
    return query;
  }
  return url;
}

function readHistoryToolResultId(entry: Record<string, unknown>): string {
  return stringProperty(entry, ["toolCallId", "callId", "id"]) ?? "unknown";
}

function buildHistoryToolResultPayload(entry: Record<string, unknown>, rawSeq: number): Record<string, unknown> {
  const content = extractTextContent(entry.content) ?? (typeof entry.content === "string" ? entry.content : "");
  const details = parseJsonObject(content) ?? content;
  const detailsRecord = toRecord(details);
  const name =
    stringProperty(entry, ["toolName", "name", "tool"]) ??
    stringProperty(detailsRecord, ["tool", "name"]) ??
    "tool";
  const toolCallId = readHistoryToolResultId(entry);
  const isError =
    entry.isError === true ||
    String(detailsRecord.status ?? "").toLowerCase() === "error" ||
    Boolean(detailsRecord.error);

  return {
    data: {
      phase: "result",
      name,
      toolCallId,
      isError,
      result: {
        content,
        details
      }
    },
    rawSeq
  };
}

function buildHistoryOutputFilesEvent(
  sessionId: string,
  rawSeq: number,
  createdAt: string,
  entry: Record<string, unknown>,
  expanded: boolean
): GatewayEvent | null {
  const file = readHistoryOutputFileFromToolResult(entry);
  if (!file) {
    return null;
  }

  return {
    id: `${sessionId}:history:${rawSeq}:output-files:${file.id}`,
    sessionId,
    seq: historyEventSeq(rawSeq, 1, expanded),
    kind: "process.step",
    payload: {
      object: "process.step",
      process_step: {
        step_code: "output_files",
        name: "生成文件",
        status: "completed",
        message: "生成了 1 个文件",
        data: {
          files: [file],
          contract_version: "v1",
          media_attachments: [
            {
              ...file,
              kind: resolveHistoryMediaKind(String(file.mime_type ?? ""), String(file.file_name ?? ""))
            }
          ],
          media_contract_version: "v1"
        },
        timestamp: Math.floor(new Date(createdAt).getTime() / 1000)
      },
      rawSeq
    },
    createdAt
  };
}

function readHistoryOutputFileFromToolResult(entry: Record<string, unknown>): Record<string, unknown> | null {
  const content = extractTextContent(entry.content) ?? (typeof entry.content === "string" ? entry.content : "");
  const match = content.match(/Successfully wrote\s+\d+\s+bytes\s+to\s+([^\s`'"<>]+(?:\.[^\s`'"<>]+)?)/i);
  if (!match?.[1]) {
    return null;
  }
  const absolutePath = resolve(match[1]);
  const workspaceDir = findHistoryOutputWorkspaceDir(absolutePath);
  if (!workspaceDir) {
    return null;
  }
  try {
    const stat = lstatSync(absolutePath);
    if (!stat.isFile() || stat.size <= 0 || stat.size > 10 * 1024 * 1024) {
      return null;
    }
    const bytes = readFileSync(absolutePath);
    const fileName = relative(workspaceDir, absolutePath).split("\\").join("/") || absolutePath.split("/").at(-1) || "file";
    return {
      id: `local-history-${createShortHash(`${absolutePath}:${stat.mtimeMs}:${stat.size}`)}`,
      file_name: fileName,
      mime_type: inferHistoryMimeType(fileName),
      size: stat.size,
      base64: bytes.toString("base64")
    };
  } catch {
    return null;
  }
}

function findHistoryOutputWorkspaceDir(filePath: string): string | null {
  const home = process.env.HOME ? resolve(process.env.HOME) : "";
  const candidates = [
    home ? resolve(home, ".qclaw", "workspace") : "",
    home ? resolve(home, ".openclaw", "workspace") : ""
  ].filter(Boolean);
  for (const workspaceDir of candidates) {
    const rel = relative(workspaceDir, filePath);
    if (rel && rel !== "." && rel !== ".." && !rel.startsWith("../") && !rel.startsWith("..\\")) {
      return workspaceDir;
    }
  }
  return null;
}

function createShortHash(value: string): string {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
  }
  return hash.toString(16);
}

function inferHistoryMimeType(fileName: string): string {
  const ext = fileName.split(".").pop()?.toLowerCase() ?? "";
  if (ext === "txt") return "text/plain";
  if (ext === "md" || ext === "markdown") return "text/markdown";
  if (ext === "csv") return "text/csv";
  if (ext === "json") return "application/json";
  if (ext === "png") return "image/png";
  if (ext === "jpg" || ext === "jpeg") return "image/jpeg";
  if (ext === "gif") return "image/gif";
  if (ext === "webp") return "image/webp";
  if (ext === "pdf") return "application/pdf";
  return "application/octet-stream";
}

function resolveHistoryMediaKind(mimeType = "", fileName = ""): string {
  const lower = mimeType.toLowerCase();
  if (lower.startsWith("image/")) return "image";
  if (lower.startsWith("audio/")) return "audio";
  if (lower.startsWith("video/")) return "video";
  if (lower.startsWith("text/")) return "text";
  const ext = fileName.split(".").pop()?.toLowerCase() ?? "";
  if (["txt", "md", "csv", "json", "log"].includes(ext)) return "text";
  return "file";
}

function parseJsonObject(value: string): unknown | undefined {
  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return undefined;
  }
}

function mapGatewayFrameToEvents(
  frame: Extract<GatewayFrame, { type: "event" }>,
  lastSeq: number,
  subscription?: SessionSubscription,
  exposeRawThinking = false
): GatewayEvent[] {
  const sessionId = extractSessionKey(frame.payload);
  const payload = toRecord(frame.payload);
  const message = toRecord(payload.message);
  const messageMeta = toRecord(message.__openclaw);
  const session = toRecord(payload.session);
  if (!sessionId) {
    return [];
  }

  if (frame.event === "chat") {
    const state = String(payload.state ?? "delta");
    const deltaText = typeof payload.deltaText === "string" ? payload.deltaText : "";
    const messageContent = extractTextContent(message.content) ?? "";
    let content = state === "delta" ? deltaText || messageContent : messageContent || deltaText;
    const thinking = extractThinkingSignal(payload, message.content);
    if (!content.trim() && !thinking?.trim()) {
      return [];
    }
    const isDelta = state === "delta";
    const runKey = typeof payload.runId === "string" && payload.runId ? payload.runId : sessionId;
    let nextSeq = lastSeq + 1;
    const events: GatewayEvent[] = [];

    if (thinking?.trim()) {
      const previousThinking = subscription?.renderedThinkingTextByRun.get(runKey) ?? "";
      const shouldSkipThinking = isDelta && previousThinking && previousThinking.startsWith(thinking);
      if (!shouldSkipThinking) {
        const replaceThinking =
          payload.replace === true || (isDelta && Boolean(previousThinking) && thinking.startsWith(previousThinking));
        events.push({
          id: `${sessionId}:thinking:${nextSeq}`,
          sessionId,
          seq: nextSeq,
          kind: "assistant.thinking",
          payload: buildThinkingPayload(thinking, exposeRawThinking, {
            state,
            mode: isDelta ? (replaceThinking ? "replace" : "append") : "replace",
            replace: replaceThinking,
            runId: payload.runId,
            rawSeq: payload.seq
          }),
          createdAt: toIsoString(message.timestamp ?? Date.now())
        });
        nextSeq += 1;
        if (isDelta && subscription) {
          subscription.renderedThinkingTextByRun.set(
            runKey,
            replaceThinking ? thinking : `${previousThinking}${thinking}`
          );
        }
      }
    }

    if (!content.trim()) {
      return events;
    }

    const previousRendered = subscription?.renderedAssistantTextByRun.get(runKey) ?? "";
    if (!isDelta && previousRendered.trim()) {
      content = normalizeFinalAssistantContent(content, previousRendered);
    }
    if (isDelta && previousRendered && previousRendered.startsWith(content)) {
      return events;
    }
    const replace = payload.replace === true || (isDelta && Boolean(previousRendered) && content.startsWith(previousRendered));
    const mode = isDelta ? (replace ? "replace" : "append") : "replace";
    const chunks = isDelta && mode === "append" ? splitDeltaText(content) : [content];
    const createdAt = toIsoString(message.timestamp ?? Date.now());
    chunks.forEach((chunk, index) => {
      const seq = nextSeq + index;
      events.push({
        id: `${sessionId}:chat:${seq}`,
        sessionId,
        seq,
        kind: isDelta ? "assistant.delta" : "assistant.message",
        payload: {
          content: chunk,
          state,
          mode,
          replace,
          runId: payload.runId,
          rawSeq: payload.seq,
          ...(isDelta && mode === "append" && chunks.length > 1
            ? {
                syntheticChunk: {
                  index,
                  total: chunks.length
                }
              }
            : {})
        },
        createdAt
      });
    });
    if (isDelta && subscription) {
      subscription.renderedAssistantTextByRun.set(
        runKey,
        mode === "replace" ? content : `${previousRendered}${chunks.join("")}`
      );
    }
    return events;
  }

  if (frame.event === "session.message") {
    const role = typeof message.role === "string" ? message.role : "";
    if (role !== "assistant" && role !== "user") {
      return [];
    }
    const content = extractTextContent(message.content) ?? "";
    const thinking = role === "assistant" ? extractThinkingContent(message.content) : null;
    if (!content.trim() && !thinking?.trim()) {
      return [];
    }
    const shouldSkipAssistantContent =
      role === "assistant" && content.trim() && shouldSkipLiveSessionAssistantMessage(subscription, content);
    const rawSeq = Number(payload.messageSeq ?? messageMeta.seq);
    let seq = Number.isFinite(rawSeq) ? Math.max(lastSeq + 1, rawSeq) : lastSeq + 1;
    const createdAt = toIsoString(message.timestamp ?? Date.now());
    const events: GatewayEvent[] = [];
    if (thinking?.trim()) {
      events.push({
        id: `${sessionId}:thinking:${seq}`,
        sessionId,
        seq,
        kind: "assistant.thinking",
        payload: buildThinkingPayload(thinking, exposeRawThinking, {
          state: "final",
          mode: "replace",
          replace: true,
          rawSeq: payload.messageSeq ?? messageMeta.seq
        }),
        createdAt
      });
      seq += 1;
    }
    if (content.trim() && !shouldSkipAssistantContent) {
      events.push({
        id: `${sessionId}:message:${seq}`,
        sessionId,
        seq,
        kind: role === "user" ? "user.message" : "assistant.message",
        payload: {
          content
        },
        createdAt
      });
    }
    return events;
  }

  if (frame.event === "session.tool") {
    const rawSeq = Number(payload.seq);
    const seq = Number.isFinite(rawSeq) ? Math.max(lastSeq + 1, rawSeq) : lastSeq + 1;
    const normalizedPayload = normalizeSessionToolPayload(payload, subscription);
    const data = toRecord(normalizedPayload.data);
    const kind = String(normalizedPayload.phase ?? data.phase ?? "tool");
    const eventKind = ["result", "done", "output"].includes(kind) ? "tool.result" : "tool.call";
    rememberSessionToolCallData(eventKind, data, subscription);
    return [
      {
        id: `${sessionId}:tool:${seq}`,
        sessionId,
        seq,
        kind: eventKind,
        payload: {
          ...normalizedPayload,
          ...(Number.isFinite(rawSeq) ? { rawSeq } : {})
        },
        createdAt: new Date().toISOString()
      }
    ];
  }

  if (frame.event === "sessions.changed") {
    const phase = String(payload.phase ?? "");
    const seq = lastSeq + 1;
    if (phase === "start") {
      return [
        {
          id: `${sessionId}:status:${seq}`,
          sessionId,
          seq,
          kind: "run.started",
          payload,
          createdAt: toIsoString(payload.startedAt ?? payload.ts ?? Date.now())
        }
      ];
    }
    if (phase === "end") {
      const status = normalizeRunEndStatus(payload.status ?? session.status);
      const eventKind =
        status === "completed"
          ? "run.completed"
          : status === "failed"
            ? "run.failed"
            : status === "interrupted"
              ? "run.interrupted"
              : "status.update";
      return [
        {
          id: `${sessionId}:status:${seq}`,
          sessionId,
          seq,
          kind: eventKind,
          payload,
          createdAt: toIsoString(payload.endedAt ?? payload.ts ?? Date.now())
        }
      ];
    }
    return [
      {
        id: `${sessionId}:status:${seq}`,
        sessionId,
        seq,
        kind: "status.update",
        payload,
        createdAt: toIsoString(payload.updatedAt ?? payload.ts ?? Date.now())
      }
    ];
  }

  return [];
}

function shouldSkipLiveSessionAssistantMessage(subscription: SessionSubscription | undefined, content: string): boolean {
  if (!subscription) {
    return false;
  }
  const normalized = normalizeComparableAssistantText(content);
  if (!normalized) {
    return false;
  }
  if (subscription.skippedSessionAssistantText.has(normalized)) {
    return true;
  }
  for (const renderedText of subscription.renderedAssistantTextByRun.values()) {
    const rendered = normalizeComparableAssistantText(renderedText);
    if (rendered && (rendered === normalized || rendered.endsWith(normalized))) {
      subscription.skippedSessionAssistantText.add(normalized);
      return true;
    }
  }
  return false;
}

function normalizeComparableAssistantText(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function isOpenClawDuplicateTraceEnabled(): boolean {
  const value = String(process.env.OPENCLAW_TRACE_DUPLICATES ?? process.env.OPENCLAW_DIAG_LOGS ?? "").toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}

function logOpenClawDuplicateTrace(label: string, payload: Record<string, unknown>): void {
  if (!isOpenClawDuplicateTraceEnabled()) {
    return;
  }
  try {
    console.info(`[openclaw-dup-trace] ${label} ${JSON.stringify(payload)}`);
  } catch {
    console.info(`[openclaw-dup-trace] ${label}`);
  }
}

function summarizeGatewayEventForDuplicateTrace(event: GatewayEvent): Record<string, unknown> {
  const payload = toRecord(event.payload);
  const content = typeof payload.content === "string" ? payload.content : "";
  const timeline = toRecord(payload.openclaw_timeline);
  return {
    id: event.id,
    kind: event.kind,
    seq: event.seq,
    rawSeq: payload.rawSeq,
    runId: payload.runId,
    state: payload.state,
    mode: payload.mode,
    replace: payload.replace,
    segmentId: traceString(timeline.segment_id, payload.segment_id),
    segmentType: traceString(timeline.segment_type, payload.segment_type),
    deltaIndex: timeline.delta_index ?? payload.delta_index,
    contentLength: content.length,
    contentHash: content ? createHash("sha1").update(content).digest("hex").slice(0, 12) : ""
  };
}

function traceString(...values: unknown[]): string {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value;
    }
  }
  return "";
}

function normalizeRunEndStatus(value: unknown): "completed" | "failed" | "interrupted" | "unknown" {
  const status = String(value ?? "").trim().toLowerCase();
  if (["done", "success", "succeeded", "completed", "complete"].includes(status)) {
    return "completed";
  }
  if (["failed", "failure", "error", "errored", "timeout", "timed_out", "timedout"].includes(status)) {
    return "failed";
  }
  if (["aborted", "abort", "cancelled", "canceled", "interrupted"].includes(status)) {
    return "interrupted";
  }
  return "unknown";
}

function isOpenClawStopSettledEvent(event: GatewayEvent, startedAtMs: number): boolean {
  if (event.kind !== "run.completed" && event.kind !== "run.failed" && event.kind !== "run.interrupted") {
    return false;
  }

  const eventMs = Date.parse(event.createdAt);
  if (Number.isFinite(eventMs) && eventMs < startedAtMs - 100) {
    return false;
  }

  const payload = toRecord(event.payload);
  return String(payload.phase ?? "") === "end";
}

function extractSessionKey(payload: unknown): string | null {
  const record = toRecord(payload);
  const session = toRecord(record.session);
  const sessionKey = record.sessionKey ?? session.key;
  return typeof sessionKey === "string" && sessionKey.trim() ? sessionKey : null;
}

function isStaleChatFrame(frame: Extract<GatewayFrame, { type: "event" }>, subscription: SessionSubscription): boolean {
  if (frame.event !== "chat") {
    return false;
  }

  const payload = toRecord(frame.payload);
  const runId = typeof payload.runId === "string" ? payload.runId : "";
  if (runId) {
    subscription.activeRunIds.add(runId);
    return false;
  }

  const message = toRecord(payload.message);
  const timestamp = Number(message.timestamp ?? payload.ts ?? 0);
  if (!Number.isFinite(timestamp) || timestamp <= 0) {
    return false;
  }

  return timestamp < subscription.openedAtMs - 5_000;
}

function updateActiveRunTracking(frame: Extract<GatewayFrame, { type: "event" }>, subscription: SessionSubscription) {
  if (frame.event !== "sessions.changed") {
    return;
  }

  const payload = toRecord(frame.payload);
  const runId = typeof payload.runId === "string" ? payload.runId : "";
  if (!runId) {
    return;
  }

  const phase = String(payload.phase ?? "");
  if (phase === "start") {
    subscription.activeRunIds.add(runId);
  } else if (phase === "end") {
    subscription.activeRunIds.delete(runId);
  }
}

function extractTextContent(content: unknown): string | null {
  if (typeof content === "string") {
    return content;
  }
  const lines = extractTextSegments(content);

  return lines.length > 0 ? lines.join("\n") : null;
}

function extractTextSegments(content: unknown): string[] {
  if (!Array.isArray(content)) {
    return [];
  }

  return content
    .flatMap((item) => {
      if (!item || typeof item !== "object") {
        return [];
      }
      const record = item as Record<string, unknown>;
      if (record.type === "text" && typeof record.text === "string") {
        return [record.text];
      }
      return [];
    })
    .filter((value) => value.trim().length > 0);
}

function extractThinkingContent(content: unknown): string | null {
  if (!Array.isArray(content)) {
    return null;
  }

  const lines = content
    .flatMap((item) => {
      if (!item || typeof item !== "object") {
        return [];
      }
      const record = item as Record<string, unknown>;
      if (record.type === "thinking" && typeof record.thinking === "string") {
        return [record.thinking];
      }
      return [];
    })
    .filter((value) => value.trim().length > 0);

  return lines.length > 0 ? lines.join("\n") : null;
}

function extractThinkingSignal(payload: Record<string, unknown>, messageContent: unknown): string | null {
  const nestedData = toRecord(payload.data);
  const candidates = [
    payload.thinking,
    payload.thinkingText,
    payload.reasoning,
    payload.reasoningText,
    payload.reasoning_content,
    payload.deltaThinking,
    payload.deltaReasoning,
    nestedData.thinking,
    nestedData.thinkingText,
    nestedData.reasoning,
    nestedData.reasoningText,
    nestedData.reasoning_content,
    extractThinkingContent(messageContent)
  ];

  for (const candidate of candidates) {
    const text = extractStringContent(candidate);
    if (text?.trim()) {
      return text;
    }
  }
  return null;
}

function extractStringContent(value: unknown): string | null {
  if (typeof value === "string") {
    return value;
  }
  if (!Array.isArray(value)) {
    return null;
  }
  const text = value
    .flatMap((entry) => {
      if (!entry || typeof entry !== "object") {
        return [];
      }
      const record = entry as Record<string, unknown>;
      if (typeof record.text === "string") {
        return [record.text];
      }
      if (typeof record.thinking === "string") {
        return [record.thinking];
      }
      if (typeof record.content === "string") {
        return [record.content];
      }
      return [];
    })
    .join("");
  return text || null;
}

function buildThinkingPayload(
  thinking: string,
  exposeRawThinking: boolean,
  extra: Record<string, unknown>
): Record<string, unknown> {
  const content = normalizeThinkingContent(thinking);
  if (exposeRawThinking) {
    return {
      content,
      privateContentOmitted: false,
      rawThinkingVisible: true,
      textLength: content.length,
      ...extra
    };
  }

  return {
    content: "Claw emitted a private thinking update. Raw reasoning is hidden.",
    privateContentOmitted: true,
    textLength: content.length,
    ...extra
  };
}

function normalizeThinkingContent(value: string): string {
  return collapseRepeatedLeadingSegment(collapseRepeatedGrowingPrefix(value));
}

function normalizeFinalAssistantContent(content: string, previousRendered: string): string {
  const trimmedPrevious = previousRendered.trim();
  const trimmedContent = content.trim();
  if (!trimmedPrevious || !trimmedContent || trimmedContent === trimmedPrevious) {
    return trimmedContent || content;
  }

  const index = trimmedContent.lastIndexOf(trimmedPrevious);
  if (index >= 0) {
    const trailingLength = trimmedContent.length - index - trimmedPrevious.length;
    const endsWithPreviousAnswer = trimmedPrevious.length >= 10 && trailingLength <= 8;
    const previousDominatesSnapshot = trimmedPrevious.length >= Math.min(160, trimmedContent.length * 0.45);
    if (endsWithPreviousAnswer || previousDominatesSnapshot) {
      return trimmedPrevious;
    }
  }

  return content;
}

function collapseRepeatedGrowingPrefix(value: string): string {
  const leadingWhitespace = value.match(/^\s*/)?.[0] ?? "";
  const text = value.trimStart();
  if (text.length < 40) {
    return value;
  }

  for (let split = Math.floor(text.length / 2); split >= 20; split -= 1) {
    const first = text.slice(0, split).trim();
    const second = text.slice(split).trimStart();
    if (first.length >= 20 && second.startsWith(first)) {
      return `${leadingWhitespace}${second}`;
    }
  }

  return value;
}

function collapseRepeatedLeadingSegment(value: string): string {
  const leadingWhitespace = value.match(/^\s*/)?.[0] ?? "";
  const text = value.trimStart();
  const maxSegmentLength = Math.min(32, Math.floor(text.length / 2));

  for (let length = maxSegmentLength; length >= 2; length -= 1) {
    const segment = text.slice(0, length);
    if (!segment.trim() || !text.startsWith(`${segment}${segment}`)) {
      continue;
    }

    const remainder = text.slice(length * 2);
    const next = remainder[0] ?? "";
    const segmentLooksNatural =
      /\s/.test(segment) ||
      /^[A-Z][a-z]+$/.test(segment) ||
      /[\u4e00-\u9fff]/.test(segment);
    const hasBoundary = !next || /\s|[,.!?;:，。！？；：]/.test(next) || /[\u4e00-\u9fff]/.test(next);

    if (segmentLooksNatural && hasBoundary) {
      return `${leadingWhitespace}${segment}${remainder}`;
    }
  }

  return value;
}

function splitDeltaText(text: string): string[] {
  if (text.length <= MAX_DELTA_CHUNK_CHARS) {
    return [text];
  }

  const chunks: string[] = [];
  let cursor = 0;
  while (cursor < text.length) {
    const remaining = text.length - cursor;
    if (remaining <= MAX_DELTA_CHUNK_CHARS) {
      chunks.push(text.slice(cursor));
      break;
    }

    const hardLimit = cursor + MAX_DELTA_CHUNK_CHARS;
    const softLimit = cursor + MIN_DELTA_CHUNK_CHARS;
    let splitAt = findReadableSplit(text, softLimit, hardLimit);
    if (splitAt <= cursor) {
      splitAt = hardLimit;
    }

    chunks.push(text.slice(cursor, splitAt));
    cursor = splitAt;
  }

  return chunks;
}

function findReadableSplit(text: string, minIndex: number, maxIndex: number): number {
  const boundedMax = Math.min(maxIndex, text.length);
  const candidates = ["\n\n", "\n", "。", "！", "？", ". ", "! ", "? ", "；", "; ", "，", ", ", " "];
  for (const marker of candidates) {
    const index = text.lastIndexOf(marker, boundedMax);
    if (index >= minIndex) {
      return index + marker.length;
    }
  }
  return boundedMax;
}

function normalizeStatus(status: unknown): GatewaySession["status"] {
  switch (status) {
    case "running":
      return "running";
    case "done":
    case "completed":
      return "completed";
    case "failed":
      return "failed";
    case "aborted":
    case "interrupted":
      return "interrupted";
    case "archived":
      return "archived";
    default:
      return "idle";
  }
}

function toIsoString(value: unknown): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Date(value).toISOString();
  }
  if (typeof value === "string" && value.trim()) {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      return new Date(asNumber).toISOString();
    }
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString();
    }
  }
  return new Date().toISOString();
}

function optionalIsoString(value: unknown): string | undefined {
  if (typeof value === "number" && Number.isFinite(value)) {
    return new Date(value).toISOString();
  }
  if (typeof value === "string" && value.trim()) {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      return new Date(asNumber).toISOString();
    }
    const date = new Date(value);
    if (!Number.isNaN(date.getTime())) {
      return date.toISOString();
    }
  }
  return undefined;
}

function fallbackSession(sessionId: string, hostKind = "openclaw"): GatewaySession {
  const now = new Date().toISOString();
  return {
    id: sessionId,
    title: sessionId,
    status: "idle",
    hostKind,
    runnerCommand: "openclaw-gateway",
    createdAt: now,
    updatedAt: now,
    lastEventSeq: 0
  };
}

function toRecord(value: unknown): Record<string, any> {
  return value && typeof value === "object" ? (value as Record<string, any>) : {};
}
