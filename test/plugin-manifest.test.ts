import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { test } from "node:test";

import { aiHubPlugin } from "../src/channel.js";

test("openclaw plugin manifest exposes sendThinkingMessage", () => {
  const manifestPath = resolve(process.cwd(), "openclaw.plugin.json");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8")) as {
    configSchema?: {
      properties?: Record<string, unknown>;
    };
  };

  assert.ok(manifest.configSchema?.properties?.sendThinkingMessage, "sendThinkingMessage should be declared in manifest schema");
});

test("channel plugin declares block streaming support", () => {
  assert.equal(aiHubPlugin.capabilities.blockStreaming, true);
});
