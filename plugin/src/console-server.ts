import { randomUUID } from "node:crypto";
import { createReadStream } from "node:fs";
import { access } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { join, normalize, resolve } from "node:path";

import WebSocket, { WebSocketServer } from "ws";

import { createHub53AIBridge, type Hub53AIConfig, type Hub53AIStatusSnapshot } from "./53aihub-client";
import type { AgentEventProbe, AgentEventProbeSnapshot } from "./agent-event-probe";
import { FileSessionStore } from "./file-store";
import type {
  GatewayClient,
  GatewayConfig,
  GatewayEvent,
  GatewayHealthSnapshot,
  GatewayRuntimeInfo
} from "./gateway-client";
import { readHostRuntimeInfo, type HostRuntimeInfo } from "./host";
import type { ControlAction, SessionDetail, SessionMessage, SessionStatus, SessionSummary, TimelineEvent } from "./models";

type ConsoleConfig = {
  host: string;
  port: number;
};

type PersistenceConfig = {
  maxSessions: number;
};

type CreateConsoleServerInput = {
  stateDir: string;
  configPath: string;
  hostKind: string;
  pluginVersion: string;
  token: string;
  gatewayConfig: GatewayConfig;
  hub53aiConfig?: Hub53AIConfig;
  consoleConfig: ConsoleConfig;
  persistence: PersistenceConfig;
  hostRuntime?: HostRuntimeInfo;
  gateway: GatewayClient;
  agentEventProbe?: AgentEventProbe;
  webDir?: string;
};

type StatusSnapshot = {
  hostKind: string;
  stateDir: string;
  configPath: string;
  serviceVersion: string;
  pluginVersion: string;
  port: number;
  pid: number;
  runnerCommand: string;
  activeSessionCount: number;
  runningSessionCount: number;
  healthy: boolean;
  connectionHealthy: boolean;
  gatewayHealth?: GatewayHealthSnapshot;
  modelPrimary?: string;
  enabledSkills: string[];
  cronScheduler?: GatewayRuntimeInfo["cronScheduler"];
  cronTasks?: NonNullable<GatewayRuntimeInfo["cronTasks"]>;
  hub53ai?: Hub53AIStatusSnapshot;
  agentEvents?: AgentEventProbeSnapshot;
};

const RUNTIME_INFO_CACHE_TTL_MS = 5_000;
const HEALTH_INFO_CACHE_TTL_MS = 5_000;
const REMOTE_SYNC_INTERVAL_MS = 15_000;
const REMOTE_SYNC_QUICK_PAGE_LIMIT = 500;

export function createConsoleServer(input: CreateConsoleServerInput) {
  const hostRuntime = input.hostRuntime ?? readHostRuntimeInfo(input.configPath);
  const store = new FileSessionStore(
    join(input.stateDir, "claw-control-center-state.json"),
    input.persistence.maxSessions
  );

  const sessionStreams = new Map<string, () => void>();
  const sessionSockets = new Map<string, Set<WebSocket>>();
  const statusSockets = new Set<WebSocket>();
  let lastGatewayError: Error | null = null;
  let currentPort = input.consoleConfig.port;
  let remoteSyncTimer: NodeJS.Timeout | undefined;
  let remoteSyncInFlight = false;
  let runtimeInfoCache: {
    value: GatewayRuntimeInfo | null;
    updatedAtMs: number;
    inFlight: Promise<void> | null;
  } = {
    value: null,
    updatedAtMs: 0,
    inFlight: null
  };
  let healthInfoCache: {
    value: GatewayHealthSnapshot | null;
    updatedAtMs: number;
    inFlight: Promise<void> | null;
  } = {
    value: null,
    updatedAtMs: 0,
    inFlight: null
  };
  const hub53ai = input.hub53aiConfig
    ? createHub53AIBridge({
        stateDir: input.stateDir,
        configPath: input.configPath,
        config: input.hub53aiConfig,
        gateway: input.gateway,
        rpcContext: {
          getStatusSnapshot: () => buildStatusSnapshot(),
          getConfigSnapshot: () => buildConfigSnapshot()
        },
        callbacks: {
          onSessionUpsert: async (session) => {
            await store.upsertSession(session);
            broadcastStatus();
          },
          onUserMessage: async (message) => {
            await store.appendMessage(message);
          },
          onSessionStatus: async (sessionId, status) => {
            await store.setSessionStatus(sessionId, status);
            broadcastStatus();
          },
          onBridgeThinkingEvent: async (event) => {
            await appendEvent(event);
            broadcastSessionEvent(event.sessionId, event);
          },
          listSessionMessages: (sessionId) => store.getSession(sessionId)?.messages ?? [],
          listSessionEvents: (sessionId) => store.getSession(sessionId)?.events ?? [],
          listKnownSessions: () => store.listSessions(),
          onEnsureSessionStream: ensureSessionStream,
          getLastEventSeq: (sessionId) => store.getLastEventSeq(sessionId),
          onStatusChange: broadcastStatus
        }
      })
    : undefined;

  const httpServer = createServer(async (request, response) => {
    try {
      await routeRequest(request, response);
    } catch (error) {
      writeJson(response, 500, {
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  const wsServer = new WebSocketServer({ noServer: true });
  httpServer.on("upgrade", (request, socket, head) => {
    const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
    if (!url.pathname.startsWith("/ws/")) {
      socket.destroy();
      return;
    }
    wsServer.handleUpgrade(request, socket, head, (client: WebSocket) => {
      handleSocket(client, url.pathname);
    });
  });

  async function start() {
    await store.init();
    await syncRemoteSessions();
    await refreshRuntimeInfo(true);
    await refreshGatewayHealth(true);
    await hub53ai?.start();
    await new Promise<void>((resolvePromise) => {
      httpServer.listen(input.consoleConfig.port, input.consoleConfig.host, () => resolvePromise());
    });
    const address = httpServer.address();
    if (!address || typeof address === "string") {
      throw new Error("failed to resolve local server address");
    }
    currentPort = address.port;

    for (const session of store.listSessions()) {
      if (session.status === "running") {
        ensureSessionStream(session.id);
      }
    }
    remoteSyncTimer = setInterval(() => {
      void syncRemoteSessions().then((changed) => {
        if (changed) {
          broadcastStatus();
        }
      });
    }, REMOTE_SYNC_INTERVAL_MS);
  }

  async function stop() {
    if (remoteSyncTimer) {
      clearInterval(remoteSyncTimer);
      remoteSyncTimer = undefined;
    }
    await hub53ai?.stop();
    for (const close of sessionStreams.values()) {
      close();
    }
    sessionStreams.clear();
    for (const socket of statusSockets) {
      socket.close();
    }
    for (const sockets of sessionSockets.values()) {
      for (const socket of sockets) {
        socket.close();
      }
    }
    await new Promise<void>((resolvePromise) => httpServer.close(() => resolvePromise()));
    await input.gateway.stop();
  }

  function handleSocket(socket: WebSocket, pathname: string) {
    if (pathname === "/ws/status") {
      statusSockets.add(socket);
      socket.send(JSON.stringify(buildStatusSnapshot()));
      socket.on("close", () => statusSockets.delete(socket));
      return;
    }

    const sessionMatch = pathname.match(/^\/ws\/sessions\/([^/]+)$/);
    if (sessionMatch) {
      const sessionId = decodeURIComponent(sessionMatch[1]!);
      const sockets = sessionSockets.get(sessionId) ?? new Set<WebSocket>();
      sockets.add(socket);
      sessionSockets.set(sessionId, sockets);
      void ensureSessionStream(sessionId);
      socket.on("close", () => {
        sockets.delete(socket);
        if (sockets.size === 0) {
          sessionSockets.delete(sessionId);
        }
      });
      return;
    }

    socket.close();
  }

  async function routeRequest(request: IncomingMessage, response: ServerResponse) {
    const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
    const method = request.method ?? "GET";

    if (method === "GET" && url.pathname === "/api/bootstrap") {
      writeJson(response, 200, {
        token: input.token,
        status: buildStatusSnapshot(),
        config: buildConfigSnapshot()
      });
      return;
    }

    if (method === "GET" && url.pathname === "/api/status") {
      writeJson(response, 200, buildStatusSnapshot());
      return;
    }

    if (method === "GET" && url.pathname === "/api/agent-events") {
      const afterSeq = Number(url.searchParams.get("afterSeq") ?? "0");
      writeJson(response, 200, {
        probe: input.agentEventProbe?.getSnapshot(),
        events: input.agentEventProbe?.getEvents(afterSeq) ?? []
      });
      return;
    }

    if (method === "GET" && url.pathname === "/api/config") {
      writeJson(response, 200, buildConfigSnapshot());
      return;
    }

    if (method === "GET" && url.pathname === "/api/sessions") {
      const limit = readPositiveInteger(url.searchParams.get("limit"), input.persistence.maxSessions);
      const offset = readPositiveInteger(url.searchParams.get("offset"), 0);
      const page = await listSessionPage(limit, offset);
      writeJson(response, 200, page);
      return;
    }

    if (method === "POST" && url.pathname === "/api/sessions") {
      requireToken(request, input.token);
      const body = await readJsonBody<{ title?: string; initialPrompt?: string }>(request);
      const session = await input.gateway.createSession(
        body.title?.trim() || `Session ${store.listSessions().length + 1}`,
        body.initialPrompt ?? ""
      );
      await store.upsertSession(session);
      if (body.initialPrompt?.trim()) {
        await ensureSessionStream(session.id);
        await store.appendMessage(buildUserMessage(session.id, body.initialPrompt));
        await store.setSessionStatus(session.id, "running");
        await input.gateway.sendMessage(session.id, body.initialPrompt);
      }
      broadcastStatus();
      writeJson(response, 201, session);
      return;
    }

    const sessionMatch = url.pathname.match(/^\/api\/sessions\/([^/]+)(?:\/(messages|control|events))?$/);
    if (sessionMatch) {
      const sessionId = decodeURIComponent(sessionMatch[1]!);
      const suffix = sessionMatch[2];

      if (method === "GET" && !suffix) {
        await reconcileDerivedAssistantMessage(sessionId);
        let session = store.getSession(sessionId);
        if (!session || !store.isHydrated(sessionId)) {
          await hydrateSession(sessionId);
          session = store.getSession(sessionId);
        }
        if (!session) {
          writeJson(response, 404, { error: "session not found" });
          return;
        }
        writeJson(response, 200, {
          session: session.session,
          messages: session.messages
        } satisfies SessionDetail);
        return;
      }

      if (method === "GET" && suffix === "events") {
        await reconcileDerivedAssistantMessage(sessionId);
        const afterSeq = Number(url.searchParams.get("afterSeq") ?? "0");
        let session = store.getSession(sessionId);
        if (!session || !store.isHydrated(sessionId)) {
          await hydrateSession(sessionId);
          session = store.getSession(sessionId);
        }
        if (!session) {
          writeJson(response, 404, { error: "session not found" });
          return;
        }
        writeJson(response, 200, {
          events: session.events.filter((event) => event.seq > afterSeq)
        });
        return;
      }

      if (method === "POST" && suffix === "messages") {
        requireToken(request, input.token);
        const body = await readJsonBody<{ content?: string }>(request);
        if (!body.content?.trim()) {
          writeJson(response, 400, { error: "message content is required" });
          return;
        }
        const existing = store.getSession(sessionId);
        if (!existing) {
          writeJson(response, 404, { error: "session not found" });
          return;
        }

        await ensureSessionStream(sessionId);
        await store.appendMessage(buildUserMessage(sessionId, body.content));
        await store.setSessionStatus(sessionId, "running");

        try {
          await input.gateway.sendMessage(sessionId, body.content);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          await store.setSessionStatus(sessionId, "failed");
          await appendEvent({
            id: randomUUID(),
            sessionId,
            seq: existing.session.lastEventSeq + 1,
            kind: "run.failed",
            payload: { error: message },
            createdAt: new Date().toISOString()
          });
          throw error;
        }

        broadcastStatus();
        writeJson(response, 202, { ok: true });
        return;
      }

      if (method === "POST" && suffix === "control") {
        requireToken(request, input.token);
        const body = await readJsonBody<{ action?: ControlAction; title?: string }>(request);
        const action = body.action;
        if (!action) {
          writeJson(response, 400, { error: "action is required" });
          return;
        }
        if (action === "rename") {
          await store.renameSession(sessionId, body.title?.trim() || "Renamed session");
        } else if (action === "archive") {
          await store.archiveSession(sessionId);
        }
        await input.gateway.controlSession(sessionId, action, body.title);
        broadcastStatus();
        writeJson(response, 200, { ok: true });
        return;
      }
    }

    await serveStatic(url.pathname, response);
  }

  async function serveStatic(pathname: string, response: ServerResponse) {
    if (!input.webDir) {
      writeJson(response, 404, { error: "not found" });
      return;
    }

    const requestedPath = pathname === "/" ? "/index.html" : pathname;
    const filePath = resolve(input.webDir, `.${normalize(requestedPath)}`);
    if (!filePath.startsWith(resolve(input.webDir))) {
      writeJson(response, 403, { error: "forbidden" });
      return;
    }

    try {
      await access(filePath);
      response.writeHead(200, {
        "Content-Type": contentTypeFor(filePath)
      });
      createReadStream(filePath).pipe(response);
    } catch {
      const indexPath = resolve(input.webDir, "index.html");
      await access(indexPath);
      response.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8"
      });
      createReadStream(indexPath).pipe(response);
    }
  }

  async function ensureSessionStream(sessionId: string) {
    if (sessionStreams.has(sessionId)) {
      return;
    }
    const lastSeq = store.getLastEventSeq(sessionId);
    const close = input.gateway.subscribe(sessionId, lastSeq, {
      onEvent: (event) => {
        lastGatewayError = null;
        void handleGatewayEvent(event);
      },
      onDisconnect: (error) => {
        lastGatewayError = error ?? new Error("gateway stream disconnected");
        sessionStreams.delete(sessionId);
        broadcastStatus();
        setTimeout(() => {
          void recoverSessionStream(sessionId);
        }, input.gatewayConfig.streamReconnectMs);
      }
    });
    sessionStreams.set(sessionId, close);
  }

  async function recoverSessionStream(sessionId: string) {
    try {
      const events = await input.gateway.listEvents(sessionId, store.getLastEventSeq(sessionId));
      for (const event of events) {
        await handleGatewayEvent(event);
      }
      await ensureSessionStream(sessionId);
    } catch (error) {
      lastGatewayError = error instanceof Error ? error : new Error(String(error));
      broadcastStatus();
    }
  }

  async function handleGatewayEvent(event: GatewayEvent) {
    await appendEvent(event);
    const nextStatus = nextStatusForEvent(event.kind);
    if (nextStatus) {
      await store.setSessionStatus(event.sessionId, nextStatus);
    }
    if (event.kind === "assistant.message" || event.kind === "user.message") {
      const content = String((event.payload as { content?: unknown }).content ?? "");
      if (content) {
        const role = event.kind === "user.message" ? "user" : "assistant";
        const record = store.getSession(event.sessionId);
        if (!record || !hasNearbyDuplicateMessage(record.messages, role, content, event.createdAt)) {
          await store.appendMessage({
            id: `${role}-${event.seq}`,
            sessionId: event.sessionId,
            role,
            content,
            createdAt: event.createdAt
          });
        }
      }
    }
    if (isTerminalEvent(event.kind)) {
      await reconcileDerivedAssistantMessage(event.sessionId);
    }
    broadcastSessionEvent(event.sessionId, event);
    broadcastStatus();
  }

  async function appendEvent(event: TimelineEvent) {
    await store.appendEvent(event);
  }

  async function syncRemoteSessions() {
    if (remoteSyncInFlight) {
      return false;
    }
    remoteSyncInFlight = true;
    try {
      lastGatewayError = null;
      const page = await input.gateway.listSessionPage({
        limit: REMOTE_SYNC_QUICK_PAGE_LIMIT,
        offset: 0
      });
      const before = sessionListSignature(store.listSessions());
      await store.replaceSessions(page.sessions);
      return before !== sessionListSignature(store.listSessions());
    } catch (error) {
      lastGatewayError = error instanceof Error ? error : new Error(String(error));
      return false;
    } finally {
      remoteSyncInFlight = false;
    }
  }

  async function listSessionPage(limit: number, offset: number) {
    try {
      const before = sessionListSignature(store.listSessions());
      const page = await input.gateway.listSessionPage({ limit, offset });
      lastGatewayError = null;
      for (const session of page.sessions) {
        await store.upsertSession(session);
      }
      if (before !== sessionListSignature(store.listSessions())) {
        broadcastStatus();
      }
      return page;
    } catch (error) {
      lastGatewayError = error instanceof Error ? error : new Error(String(error));
      const sessions = store.listSessions().slice(offset, offset + limit);
      return {
        sessions,
        pagination: {
          limit,
          offset,
          total: store.listSessions().length,
          hasMore: offset + sessions.length < store.listSessions().length,
          ...(offset + sessions.length < store.listSessions().length ? { nextOffset: offset + sessions.length } : {})
        }
      };
    }
  }

  async function hydrateSession(sessionId: string) {
    const session = await input.gateway.getSession(sessionId);
    lastGatewayError = null;
    await store.upsertSession(session);

    const messages = await input.gateway.getSessionMessages(sessionId, 200);
    const lastSeq = store.getLastEventSeq(sessionId);
    let events = await input.gateway.listEvents(sessionId, lastSeq);
    if (!events.length && lastSeq > 0) {
      events = await input.gateway.listEvents(sessionId, 0);
    }
    await store.replaceSessionDetail(sessionId, { messages, events });
    await reconcileDerivedAssistantMessage(sessionId);
  }

  async function reconcileDerivedAssistantMessage(sessionId: string) {
    const record = store.getSession(sessionId);
    if (!record) {
      return;
    }

    const derived = deriveDerivedAssistantMessage(record.messages, record.events, record.session.status, sessionId);
    if (!derived) {
      return;
    }

    await store.appendMessage(derived);
  }

  async function refreshRuntimeInfo(force = false) {
    const now = Date.now();
    if (!force && runtimeInfoCache.value && now - runtimeInfoCache.updatedAtMs < RUNTIME_INFO_CACHE_TTL_MS) {
      return;
    }
    if (runtimeInfoCache.inFlight) {
      await runtimeInfoCache.inFlight;
      return;
    }

    runtimeInfoCache.inFlight = (async () => {
      try {
        runtimeInfoCache.value = await input.gateway.getRuntimeInfo();
        runtimeInfoCache.updatedAtMs = Date.now();
      } catch {
        runtimeInfoCache.updatedAtMs = Date.now();
      } finally {
        runtimeInfoCache.inFlight = null;
      }
    })();
    await runtimeInfoCache.inFlight;
  }

  function getRuntimeInfoSnapshot(): GatewayRuntimeInfo {
    if (!runtimeInfoCache.inFlight && Date.now() - runtimeInfoCache.updatedAtMs >= RUNTIME_INFO_CACHE_TTL_MS) {
      void refreshRuntimeInfo();
    }
    return runtimeInfoCache.value ?? { enabledSkills: [] };
  }

  async function refreshGatewayHealth(force = false) {
    const now = Date.now();
    if (!force && healthInfoCache.value && now - healthInfoCache.updatedAtMs < HEALTH_INFO_CACHE_TTL_MS) {
      return;
    }
    if (healthInfoCache.inFlight) {
      await healthInfoCache.inFlight;
      return;
    }

    healthInfoCache.inFlight = (async () => {
      try {
        healthInfoCache.value = await input.gateway.getHealth();
        healthInfoCache.updatedAtMs = Date.now();
      } catch (error) {
        healthInfoCache.value = {
          status: "unknown",
          lastError: error instanceof Error ? error.message : String(error)
        };
        healthInfoCache.updatedAtMs = Date.now();
      } finally {
        healthInfoCache.inFlight = null;
      }
    })();
    await healthInfoCache.inFlight;
  }

  function getGatewayHealthSnapshot(): GatewayHealthSnapshot | undefined {
    if (!healthInfoCache.inFlight && Date.now() - healthInfoCache.updatedAtMs >= HEALTH_INFO_CACHE_TTL_MS) {
      void refreshGatewayHealth();
    }
    return healthInfoCache.value ?? undefined;
  }

  function buildStatusSnapshot(): StatusSnapshot {
    const sessions = store.listSessions();
    const runtimeInfo = getRuntimeInfoSnapshot();
    const gatewayHealth = getGatewayHealthSnapshot();
    const runtimeSkills = runtimeInfo.enabledSkills.filter((skill) => skill.trim());
    const runtimeCronTasks = runtimeInfo.cronTasks;
    const cronTasks = runtimeCronTasks ?? hostRuntime.cronTasks ?? [];
    const connectionHealthy = lastGatewayError === null;
    const healthy = typeof gatewayHealth?.ok === "boolean" ? gatewayHealth.ok : connectionHealthy;
    return {
      hostKind: input.hostKind,
      stateDir: input.stateDir,
      configPath: input.configPath,
      serviceVersion: input.pluginVersion,
      pluginVersion: input.pluginVersion,
      port: currentPort,
      pid: process.pid,
      runnerCommand: "gateway",
      activeSessionCount: sessions.length,
      runningSessionCount: sessions.filter((session) => session.status === "running").length,
      healthy,
      connectionHealthy,
      gatewayHealth,
      modelPrimary: runtimeInfo.modelPrimary ?? hostRuntime.modelPrimary,
      enabledSkills: runtimeSkills.length ? runtimeSkills : hostRuntime.enabledSkills,
      cronScheduler: runtimeInfo.cronScheduler ?? hostRuntime.cronScheduler,
      cronTasks,
      hub53ai: hub53ai?.getStatus(),
      agentEvents: input.agentEventProbe?.getSnapshot()
    };
  }

  function buildConfigSnapshot() {
    return {
      gateway: {
        ...input.gatewayConfig,
        secret: "[redacted]"
      },
      hub53ai: input.hub53aiConfig
        ? {
            ...input.hub53aiConfig,
            secret: "[redacted]"
          }
        : undefined,
      config: {
        gateway: {
          ...input.gatewayConfig,
          secret: "[redacted]"
        },
        hub53ai: input.hub53aiConfig
          ? {
              ...input.hub53aiConfig,
              secret: "[redacted]"
            }
          : undefined,
        console: input.consoleConfig,
        persistence: input.persistence
      }
    };
  }

  function broadcastStatus() {
    const snapshot = JSON.stringify(buildStatusSnapshot());
    for (const socket of statusSockets) {
      socket.send(snapshot);
    }
  }

  function broadcastSessionEvent(sessionId: string, event: TimelineEvent) {
    const sockets = sessionSockets.get(sessionId);
    if (!sockets) {
      return;
    }
    const payload = JSON.stringify(event);
    for (const socket of sockets) {
      socket.send(payload);
    }
  }

  return {
    start,
    stop,
    get baseUrl() {
      return `http://${input.consoleConfig.host}:${currentPort}`;
    }
  };
}

function buildUserMessage(sessionId: string, content: string): SessionMessage {
  return {
    id: `user-${randomUUID()}`,
    sessionId,
    role: "user",
    content,
    createdAt: new Date().toISOString()
  };
}

function requireToken(request: IncomingMessage, token: string) {
  if (!token) {
    return;
  }
  if (request.headers["x-plugin-token"] !== token) {
    throw new Error("request is missing a valid plugin token");
  }
}

async function readJsonBody<T>(request: IncomingMessage): Promise<T> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.from(chunk));
  }
  const raw = Buffer.concat(chunks).toString("utf8");
  return raw ? (JSON.parse(raw) as T) : ({} as T);
}

function writeJson(response: ServerResponse, statusCode: number, payload: unknown) {
  response.writeHead(statusCode, {
    "Content-Type": "application/json"
  });
  response.end(JSON.stringify(payload));
}

function sessionListSignature(sessions: SessionSummary[]): string {
  return sessions
    .map((session) => `${session.id}:${session.updatedAt}:${session.status}:${session.lastEventSeq}`)
    .sort()
    .join("|");
}

function readPositiveInteger(value: string | null, fallback: number): number {
  if (value === null || !value.trim()) {
    return fallback;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function hasNearbyDuplicateMessage(
  messages: SessionMessage[],
  role: string,
  content: string,
  createdAt: string,
  windowMs = 5_000
): boolean {
  const createdMs = new Date(createdAt).getTime();
  return messages.some((message) => {
    if (message.role !== role || message.content.trim() !== content.trim()) {
      return false;
    }
    const messageMs = new Date(message.createdAt).getTime();
    return Number.isFinite(createdMs) && Number.isFinite(messageMs) && Math.abs(createdMs - messageMs) <= windowMs;
  });
}

function nextStatusForEvent(kind: string): SessionStatus | undefined {
  switch (kind) {
    case "run.completed":
      return "completed";
    case "run.failed":
      return "failed";
    case "run.interrupted":
      return "interrupted";
    case "run.started":
    case "assistant.thinking":
    case "assistant.delta":
    case "assistant.message":
    case "tool.call":
    case "tool.result":
    case "status.update":
      return "running";
    default:
      return undefined;
  }
}

function isTerminalEvent(kind: string): boolean {
  return kind === "run.completed" || kind === "run.failed" || kind === "run.interrupted";
}

function readEventRunId(event?: TimelineEvent): string {
  const payload = event?.payload;
  const value = payload?.runId ?? payload?.run_id;
  return typeof value === "string" && value.trim() ? value : "";
}

export function deriveDerivedAssistantMessage(
  messages: SessionMessage[],
  events: TimelineEvent[],
  status: SessionStatus,
  sessionId: string
): SessionMessage | null {
  if (!["completed", "failed", "interrupted"].includes(status)) {
    return null;
  }

  const lastTerminalEvent = [...events].reverse().find((event) => isTerminalEvent(event.kind));
  if (!lastTerminalEvent) {
    return null;
  }
  const terminalRunId = readEventRunId(lastTerminalEvent);
  const lastUserEvent = [...events]
    .reverse()
    .find((event) => event.kind === "user.message" && event.seq < lastTerminalEvent.seq);
  const runStartEvent = terminalRunId
    ? [...events]
        .reverse()
        .find(
          (event) =>
            event.kind === "run.started" &&
            event.seq < lastTerminalEvent.seq &&
            readEventRunId(event) === terminalRunId
        )
    : undefined;
  const turnBoundarySeq = Math.max(lastUserEvent?.seq || 0, runStartEvent?.seq || 0);

  const deltaEvents = events
    .filter(
      (event) =>
        event.kind === "assistant.delta" &&
        event.seq > turnBoundarySeq &&
        event.seq <= lastTerminalEvent.seq &&
        (!terminalRunId || readEventRunId(event) === terminalRunId) &&
        typeof event.payload?.content === "string" &&
        String(event.payload.content).trim().length > 0
    )
    .sort((left, right) => left.seq - right.seq);
  const lastDeltaEvent = deltaEvents.at(-1);
  if (!lastDeltaEvent) {
    return null;
  }

  const lastUserIndex = findLastIndex(messages, (message) => message.role === "user");
  const assistantMessagesSinceLastUser = messages
    .slice(lastUserIndex + 1)
    .filter((message) => message.role === "assistant");

  const cumulative = mergeAssistantDeltaEvents(deltaEvents).trim();
  if (!cumulative) {
    return null;
  }

  if (assistantMessagesSinceLastUser.some((message) => message.content.trim() === cumulative)) {
    return null;
  }

  const combinedPrefix = assistantMessagesSinceLastUser.map((message) => message.content).join("");
  const remainder = stripKnownAssistantPrefix(cumulative, combinedPrefix, assistantMessagesSinceLastUser);
  if (!remainder) {
    return null;
  }

  if (messages.some((message) => message.role === "assistant" && message.content.trim() === remainder)) {
    return null;
  }

  return {
    id: `assistant-derived-${lastDeltaEvent.seq}`,
    sessionId,
    role: "assistant",
    content: remainder,
    createdAt: lastDeltaEvent.createdAt
  };
}

function mergeAssistantDeltaEvents(events: TimelineEvent[]): string {
  let merged = "";
  for (const event of events) {
    const content = String(event.payload.content ?? "");
    if (!content) {
      continue;
    }

    const mode = event.payload.mode;
    if (mode === "replace" || event.payload.replace === true) {
      merged = content;
      continue;
    }

    if (mode === "append") {
      if (content === merged || merged.endsWith(content)) {
        continue;
      }
      merged = content.startsWith(merged) ? content : `${merged}${content}`;
      continue;
    }

    if (content === merged || merged.startsWith(content)) {
      continue;
    }
    merged = content.startsWith(merged) ? content : `${merged}${content}`;
  }
  return merged;
}

function stripKnownAssistantPrefix(
  cumulative: string,
  combinedPrefix: string,
  assistantMessages: SessionMessage[]
): string | null {
  let remainder = cumulative;

  if (combinedPrefix.trim() && remainder.startsWith(combinedPrefix)) {
    remainder = remainder.slice(combinedPrefix.length).trim();
  } else {
    for (const message of assistantMessages) {
      const prefix = message.content.trim();
      if (!prefix) {
        continue;
      }
      if (remainder.startsWith(prefix)) {
        remainder = remainder.slice(prefix.length).trimStart();
      } else {
        break;
      }
    }
    remainder = remainder.trim();
  }

  return remainder.length > 0 ? remainder : null;
}

function findLastIndex<T>(values: T[], predicate: (value: T) => boolean): number {
  for (let index = values.length - 1; index >= 0; index -= 1) {
    if (predicate(values[index]!)) {
      return index;
    }
  }
  return -1;
}

function contentTypeFor(filePath: string): string {
  if (filePath.endsWith(".js")) {
    return "text/javascript; charset=utf-8";
  }
  if (filePath.endsWith(".css")) {
    return "text/css; charset=utf-8";
  }
  if (filePath.endsWith(".json")) {
    return "application/json; charset=utf-8";
  }
  if (filePath.endsWith(".svg")) {
    return "image/svg+xml";
  }
  return "text/html; charset=utf-8";
}
