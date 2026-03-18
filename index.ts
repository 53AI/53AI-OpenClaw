import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { emptyPluginConfigSchema } from "openclaw/plugin-sdk";

import { aiHubPlugin } from "./src/channel.js";
import { setRuntime } from "./src/runtime.js";

const plugin = {
  id: "53ai-openclaw",
  name: "53AI OpenClaw",
  description: "53AIHub (AgentHub) OpenClaw 插件",
  configSchema: emptyPluginConfigSchema(),
  register(api: OpenClawPluginApi) {
    setRuntime(api.runtime);
    api.registerChannel({ plugin: aiHubPlugin });
  },
};

export default plugin;
