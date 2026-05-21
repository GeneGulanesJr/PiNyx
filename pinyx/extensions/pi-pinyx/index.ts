import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const PINYX_BASE_URL = "http://localhost:7331";
const PINYX_HEALTH_URL = `${PINYX_BASE_URL}/health`;
const PINYX_MODELS_URL = `${PINYX_BASE_URL}/v1/models`;

interface PiNyxModel {
  id: string;
  name: string;
  reasoning: boolean;
  input: string[];
  context_window: number;
  max_tokens: number;
  cost: {
    input: number;
    output: number;
    cache_read: number;
    cache_write: number;
  };
}

interface PiNyxModelsResponse {
  object: string;
  data: PiNyxModel[];
}

interface PiNyxHealthResponse {
  status: string;
  version: string;
  providers: Record<string, { status: string; api_key: boolean }>;
  gateway: { host: string; port: number };
}

async function checkPiNyxHealth(): Promise<PiNyxHealthResponse | null> {
  try {
    const res = await fetch(PINYX_HEALTH_URL, { signal: AbortSignal.timeout(3000) });
    if (!res.ok) return null;
    return (await res.json()) as PiNyxHealthResponse;
  } catch {
    return null;
  }
}

async function fetchPiNyxModels(): Promise<PiNyxModel[]> {
  try {
    const res = await fetch(PINYX_MODELS_URL, { signal: AbortSignal.timeout(5000) });
    if (!res.ok) return [];
    const data = (await res.json()) as PiNyxModelsResponse;
    return data.data || [];
  } catch {
    return [];
  }
}

export default async function (pi: ExtensionAPI) {
  pi.registerProvider("pinyx", {
    name: "PiNyx (local)",
    baseUrl: `${PINYX_BASE_URL}/v1`,
    api: "openai-completions",
    apiKey: "pinyx",
    models: [],
    oauth: {
      name: "PiNyx (local)",

      async login(callbacks) {
        const health = await checkPiNyxHealth();
        if (!health || health.status !== "ok") {
          throw new Error(
            "PiNyx is not running. Start it with: pinyx"
          );
        }
        callbacks.onAuth({ url: PINYX_BASE_URL });
        return {
          refresh: "pinyx-local",
          access: "pinyx",
          expires: Date.now() + 365 * 24 * 60 * 60 * 1000,
        };
      },

      async refreshToken(credentials) {
        return credentials;
      },

      getApiKey(credentials) {
        return credentials.access;
      },
    },
  });

  const health = await checkPiNyxHealth();
  if (health && health.status === "ok") {
    const models = await fetchPiNyxModels();
    if (models.length > 0) {
      pi.registerProvider("pinyx", {
        baseUrl: `${PINYX_BASE_URL}/v1`,
        api: "openai-completions",
        apiKey: "pinyx",
        models: models.map((m) => ({
          id: m.id,
          name: m.name || m.id,
          reasoning: m.reasoning || false,
          input: (m.input || ["text"]) as ("text" | "image")[],
          contextWindow: m.context_window || 128000,
          maxTokens: m.max_tokens || 16384,
          cost: {
            input: m.cost?.input || 0,
            output: m.cost?.output || 0,
            cacheRead: m.cost?.cache_read || 0,
            cacheWrite: m.cost?.cache_write || 0,
          },
        })),
      });
    }
  }

  pi.registerCommand("pinyx-status", {
    description: "Show PiNyx gateway status",
    handler: async (_args, ctx) => {
      const health = await checkPiNyxHealth();
      if (!health) {
        ctx.ui.notify("PiNyx is not running. Start with: pinyx", "error");
        return;
      }

      const providers = Object.entries(health.providers || {})
        .map(([name, info]) => `  ${name}: ${info.status} (key: ${info.api_key ? "yes" : "no"})`)
        .join("\n");

      ctx.ui.notify(
        `PiNyx v${health.version} — ${health.gateway.host}:${health.gateway.port}\nProviders:\n${providers}`,
        "info"
      );
    },
  });

  pi.on("session_start", async (_event, ctx) => {
    const health = await checkPiNyxHealth();
    if (health && health.status === "ok") {
      const modelCount = (await fetchPiNyxModels()).length;
      ctx.ui.setStatus("pinyx", `PiNyx: ${modelCount} models`);
    } else {
      ctx.ui.setStatus("pinyx", "PiNyx: offline");
    }
  });
}
