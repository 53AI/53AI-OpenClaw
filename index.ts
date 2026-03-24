import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { emptyPluginConfigSchema } from "openclaw/plugin-sdk";

import { aiHubPlugin } from "./src/channel.js";
import { setRuntime } from "./src/runtime.js";
import { handleBeforeCompaction, handleAfterCompaction } from "./src/compaction-hooks.js";

const plugin = {
  id: "53aihub-openclaw-plugin",
  name: "53AIHub",
  description: "53AIHub (AgentHub) OpenClaw 插件",
  configSchema: emptyPluginConfigSchema(),
  register(api: OpenClawPluginApi) {
    setRuntime(api.runtime);
    api.registerChannel({ plugin: aiHubPlugin });
    api.on("before_compaction", handleBeforeCompaction);
    api.on("after_compaction", handleAfterCompaction);
  },
};

export default plugin;
