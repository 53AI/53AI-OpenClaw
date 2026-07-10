import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";

import type { SessionDetail, SessionMessage, SessionStatus, SessionSummary, TimelineEvent } from "./models";
import {
  dedupeMessages,
  mergePreservedHubUserMetadata,
  preserveExistingHubTitle
} from "./hub-message-utils";

type StoredSession = SessionDetail & {
  events: TimelineEvent[];
  hydrated: boolean;
};

type StoredState = {
  sessions: Record<string, StoredSession>;
};

export class FileSessionStore {
  private state: StoredState = { sessions: {} };
  private persistChain: Promise<void> = Promise.resolve();

  constructor(
    private readonly filePath: string,
    private readonly maxSessions: number
  ) {}

  async init(): Promise<void> {
    await mkdir(dirname(this.filePath), { recursive: true });
    try {
      const raw = await readFile(this.filePath, "utf8");
      const parsed = JSON.parse(raw) as StoredState;
      this.state = parsed?.sessions ? parsed : { sessions: {} };
    } catch {
      await this.persist();
    }
  }

  listSessions(): SessionSummary[] {
    return Object.values(this.state.sessions)
      .map((entry) => entry.session)
      .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
  }

  getSession(sessionId: string): StoredSession | undefined {
    return this.state.sessions[sessionId];
  }

  getLastEventSeq(sessionId: string): number {
    return this.state.sessions[sessionId]?.session.lastEventSeq ?? 0;
  }

  isHydrated(sessionId: string): boolean {
    return this.state.sessions[sessionId]?.hydrated ?? false;
  }

  async upsertSession(session: SessionSummary): Promise<void> {
    this.mergeSession(session);
    this.trimSessions();
    await this.persist();
  }

  async replaceSessions(sessions: SessionSummary[]): Promise<void> {
    const remoteIds = new Set(sessions.map((session) => session.id));
    for (const session of sessions) {
      this.mergeSession(session);
    }
    for (const sessionId of Object.keys(this.state.sessions)) {
      if (!remoteIds.has(sessionId)) {
        delete this.state.sessions[sessionId];
      }
    }
    this.trimSessions();
    await this.persist();
  }

  private mergeSession(session: SessionSummary): void {
    const existing = this.state.sessions[session.id];
    const title = preserveExistingHubTitle(existing?.session.title, session.title);
    const mergedSession = {
      ...session,
      title,
      lastEventSeq: Math.max(session.lastEventSeq, existing?.session.lastEventSeq ?? 0)
    };
    this.state.sessions[session.id] = {
      session: mergedSession,
      messages: existing?.messages ?? [],
      events: existing?.events ?? [],
      hydrated: existing?.hydrated ?? false
    };
  }

  async renameSession(sessionId: string, title: string): Promise<void> {
    const record = this.requireSession(sessionId);
    record.session.title = title;
    record.session.updatedAt = new Date().toISOString();
    await this.persist();
  }

  async archiveSession(sessionId: string): Promise<void> {
    await this.setSessionStatus(sessionId, "archived");
  }

  async setSessionStatus(sessionId: string, status: SessionStatus): Promise<void> {
    const record = this.requireSession(sessionId);
    record.session.status = status;
    record.session.updatedAt = new Date().toISOString();
    await this.persist();
  }

  async appendMessage(message: SessionMessage): Promise<void> {
    const record = this.requireSession(message.sessionId);
    if (!record.messages.some((entry) => entry.id === message.id)) {
      record.messages.push(message);
      record.hydrated = true;
      record.session.updatedAt = message.createdAt;
      await this.persist();
    }
  }

  async appendEvent(event: TimelineEvent): Promise<void> {
    const record = this.requireSession(event.sessionId);
    if (record.events.some((entry) => entry.seq === event.seq)) {
      return;
    }
    record.events.push(event);
    record.events.sort((left, right) => left.seq - right.seq);
    record.hydrated = true;
    record.session.lastEventSeq = Math.max(record.session.lastEventSeq, event.seq);
    record.session.updatedAt = event.createdAt;
    await this.persist();
  }

  async replaceSessionDetail(
    sessionId: string,
    detail: {
      messages: SessionMessage[];
      events: TimelineEvent[];
    }
  ): Promise<void> {
    const record = this.requireSession(sessionId);
    record.messages = dedupeMessages(mergePreservedHubUserMetadata(detail.messages, record.messages));
    record.events = dedupeEvents(detail.events);
    record.hydrated = true;
    record.session.lastEventSeq = record.events.at(-1)?.seq ?? record.session.lastEventSeq;
    record.session.updatedAt =
      record.messages.at(-1)?.createdAt ?? record.events.at(-1)?.createdAt ?? record.session.updatedAt;
    await this.persist();
  }

  private requireSession(sessionId: string): StoredSession {
    const record = this.state.sessions[sessionId];
    if (!record) {
      throw new Error(`unknown session: ${sessionId}`);
    }
    return record;
  }

  private trimSessions(): void {
    const sessions = this.listSessions();
    if (sessions.length <= this.maxSessions) {
      return;
    }
    for (const session of sessions.slice(this.maxSessions)) {
      delete this.state.sessions[session.id];
    }
  }

  private async persist(): Promise<void> {
    const snapshot = JSON.stringify(this.state, null, 2);
    this.persistChain = this.persistChain.then(() => writeFile(this.filePath, snapshot));
    await this.persistChain;
  }
}

function dedupeEvents(events: TimelineEvent[]): TimelineEvent[] {
  return Array.from(new Map(events.map((event) => [event.seq, event])).values()).sort((left, right) => left.seq - right.seq);
}
