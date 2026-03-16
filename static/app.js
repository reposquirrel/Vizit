const treeRoot = document.getElementById("tree-root");
const addFolderBtn = document.getElementById("add-folder");
const addPageBtn = document.getElementById("add-page");
const pageContent = document.getElementById("page-content");
const nodeMenu = document.getElementById("node-menu");
const renameNodeBtn = document.getElementById("rename-node");
const copyNodeBtn = document.getElementById("copy-node");
const addSpaceBtn = document.getElementById("add-space");
const updateNodeBtn = document.getElementById("update-node");
const removeNodeBtn = document.getElementById("remove-node");
const hamburgerMenu = document.getElementById("hamburger-menu");
const hamburgerButton = document.getElementById("hamburger");
const openConfigurationBtn = document.getElementById("open-configuration");
const configModal = document.getElementById("config-modal");
const closeConfigBtn = document.getElementById("close-config");
const cancelConfigBtn = document.getElementById("cancel-config");
const configForm = document.getElementById("config-form");
const configStatus = document.getElementById("config-status");
const testConfigBtn = document.getElementById("test-config");
const promptModal = document.getElementById("prompt-modal");
const promptForm = document.getElementById("prompt-form");
const promptInput = document.getElementById("prompt-input");
const promptStatus = document.getElementById("prompt-status");
const promptMetadata = document.getElementById("prompt-metadata");
const closePromptBtn = document.getElementById("close-prompt");
const cancelPromptBtn = document.getElementById("cancel-prompt");
const codeModal = document.getElementById("code-modal");
const codeForm = document.getElementById("code-form");
const codeEditor = document.getElementById("code-editor");
const codeStatus = document.getElementById("code-status");
const closeCodeBtn = document.getElementById("close-code");
const cancelCodeBtn = document.getElementById("cancel-code");
const logModal = document.getElementById("log-modal");
const logTitle = document.getElementById("log-title");
const logEyebrow = document.getElementById("log-eyebrow");
const logContent = document.getElementById("log-content");
const logPane = document.querySelector(".log-pane");
const logScrollIndicator = document.getElementById("log-scroll-indicator");
const logCloseBtn = document.getElementById("close-log");
const logSecondaryBtn = document.getElementById("log-secondary");
const logPrimaryBtn = document.getElementById("log-primary");
const logActions = document.getElementById("log-actions");
const logBackgroundBtn = document.getElementById("log-background");
const resumePromptBtn = document.getElementById("resume-prompt");

const DEFAULT_MODEL = "gpt-5.1-codex";
const COPILOT_MODELS = [
  "gpt-5.1-codex",
  "gpt-5.1-codex-mini",
  "gpt-5.1",
  "gpt-5",
  "gpt-5-mini",
  "gpt-4.1",
  "claude-sonnet-4.5",
  "claude-haiku-4.5",
  "claude-opus-4.5",
  "claude-sonnet-4",
  "gemini-3-pro-preview",
];
const MAX_ITERATION_CAP = 10;
const DEFAULT_MAX_ITERATIONS = 10;
const CONNECTOR_DEFAULTS = Object.freeze({
  siteUrl: "",
  projectKey: "",
  accountEmail: "",
  apiKey: "",
  model: DEFAULT_MODEL,
  copilotMaxIterations: DEFAULT_MAX_ITERATIONS,
});

function normalizeSiteUrl(value) {
  if (!value) {
    return "";
  }
  let cleaned = value.trim();
  if (!cleaned) {
    return "";
  }
  if (!/^https?:\/\//i.test(cleaned)) {
    cleaned = `https://${cleaned}`;
  }
  return cleaned.replace(/\/+$/, "");
}

function normalizeProjectKey(value) {
  const trimmed = (value || "").trim();
  if (!trimmed) {
    return "";
  }
  if (/^-?\d+$/.test(trimmed)) {
    return String(Math.abs(parseInt(trimmed, 10)));
  }
  return trimmed.toUpperCase();
}

function normalizeIterationLimit(value) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_MAX_ITERATIONS;
  }
  return Math.min(MAX_ITERATION_CAP, Math.max(1, parsed));
}

let connectorCache = null;
let connectorFetchPromise = null;

function getConnectorSnapshot() {
  if (connectorCache) {
    return connectorCache;
  }
  return resolveConnectorDefaults({});
}

function setConnectorSnapshot(payload) {
  connectorCache = resolveConnectorDefaults(payload);
  return connectorCache;
}

async function fetchConnectorConfig({ force = false } = {}) {
  if (connectorFetchPromise && !force) {
    return connectorFetchPromise;
  }
  connectorFetchPromise = (async () => {
    try {
      const response = await fetch("/api/config", { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`Unable to load configuration (${response.status})`);
      }
      const result = await response.json();
      return setConnectorSnapshot(result.connector || {});
    } catch (error) {
      console.warn("Unable to load connector config", error);
      if (!connectorCache) {
        connectorCache = resolveConnectorDefaults({});
      }
      return connectorCache;
    } finally {
      connectorFetchPromise = null;
    }
  })();
  return connectorFetchPromise;
}

function clampSpaceHeight(value) {
  const parsed = Math.round(Number(value));
  if (!Number.isFinite(parsed)) {
    return DEFAULT_SPACE_HEIGHT;
  }
  return Math.max(MIN_SPACE_HEIGHT, parsed);
}

function clampSpaceWidth(value) {
  const parsed = Math.round(Number(value));
  if (!Number.isFinite(parsed)) {
    return DEFAULT_SPACE_WIDTH;
  }
  return Math.min(MAX_SPACE_WIDTH, Math.max(MIN_SPACE_WIDTH, parsed));
}

function clampPositionValue(value) {
  const parsed = Math.round(Number(value));
  if (!Number.isFinite(parsed)) {
    return CANVAS_PADDING;
  }
  return Math.max(0, parsed);
}

function normalizeSpacePosition(space) {
  if (!space || typeof space !== "object") {
    return { x: CANVAS_PADDING, y: CANVAS_PADDING };
  }
  return {
    x: clampPositionValue(space.x),
    y: clampPositionValue(space.y),
  };
}

function coerceAspectRatio(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

function setSpaceAspectRatio(space, element, ratio) {
  const parsed = coerceAspectRatio(ratio);
  if (!parsed) {
    return;
  }
  if (space && typeof space === "object") {
    space.aspectRatio = parsed;
  }
  if (element && element.dataset) {
    element.dataset.aspectRatio = String(parsed);
  }
}

function resolveSpaceAspectRatio(space, element) {
  const datasetRatio = element && element.dataset ? coerceAspectRatio(element.dataset.aspectRatio) : null;
  if (datasetRatio) {
    return datasetRatio;
  }
  if (space && coerceAspectRatio(space.aspectRatio)) {
    return coerceAspectRatio(space.aspectRatio);
  }
  const width = Number(space && space.width) || (element && element.clientWidth) || DEFAULT_SPACE_WIDTH;
  const height = Number(space && space.height) || (element && element.clientHeight) || DEFAULT_SPACE_HEIGHT;
  if (width > 0 && height > 0) {
    return height / width;
  }
  return DEFAULT_ASPECT_RATIO;
}

function enforceAspectRatioForCard(space, card, element, { spaceId = null, preferWidth = true, persist = false } = {}) {
  if (!space || !card) {
    return;
  }
  const ratio = resolveSpaceAspectRatio(space, element);
  if (!ratio) {
    return;
  }
  const rect = card.getBoundingClientRect();
  let nextWidth = clampSpaceWidth(Number(space.width) || rect.width || DEFAULT_SPACE_WIDTH);
  let nextHeight = clampSpaceHeight(Number(space.height) || rect.height || DEFAULT_SPACE_HEIGHT);
  if (preferWidth) {
    nextHeight = clampSpaceHeight(nextWidth * ratio);
  } else {
    nextWidth = clampSpaceWidth(nextHeight / ratio);
    nextHeight = clampSpaceHeight(nextWidth * ratio);
  }
  card.style.width = `${nextWidth}px`;
  card.style.height = `${nextHeight}px`;
  if (element) {
    element.style.width = `${nextWidth}px`;
    element.style.height = `${nextHeight}px`;
  }
  space.width = nextWidth;
  space.height = nextHeight;
  if (persist && spaceId) {
    scheduleLayoutPersist(spaceId, { width: nextWidth, height: nextHeight });
  }
}

function resolveConnectorDefaults(raw) {
  const source = raw && typeof raw === "object" ? raw : {};
  const resolved = {
    siteUrl: normalizeSiteUrl(source.siteUrl),
    projectKey: normalizeProjectKey(source.projectKey),
    accountEmail: (source.accountEmail || "").trim() || CONNECTOR_DEFAULTS.accountEmail,
    apiKey: (source.apiKey || "").trim(),
    model: source.model && COPILOT_MODELS.includes(source.model) ? source.model : DEFAULT_MODEL,
    copilotMaxIterations: normalizeIterationLimit(source.copilotMaxIterations),
  };
  if (source.updatedAt) {
    resolved.updatedAt = source.updatedAt;
  }
  return resolved;
}

let menuTargetId = null;
let menuTargetType = null;
let activeSpaceId = null;
let activeSpaceTabId = null;
let logPrimaryAction = null;
const THINKING_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
let logHistory = [];
let thinkingTimeoutId = null;
let thinkingIntervalId = null;
let thinkingFrameIndex = 0;
let thinkingLine = "";
let pendingAutoUpdateTimer = null;
const layoutPersistTimers = new Map();
const layoutPersistPayloads = new Map();
const pendingSpaceUpdates = new Set();
const spaceUpdateMessages = new Map();
const tabUpdatesInFlight = new Set();
const promptSessions = new Map();
const promptInProgressSpaces = new Set();
const backgroundPromptSpaces = new Set();
const spaceElevation = new Map();
let highestSpaceElevation = 0;
let currentPromptSpaceId = null;
const MIN_SPACE_HEIGHT = 200;
const DEFAULT_SPACE_HEIGHT = 360;
const MIN_SPACE_WIDTH = 260;
const MAX_SPACE_WIDTH = 1400;
const DEFAULT_SPACE_WIDTH = 420;
const CANVAS_PADDING = 24;
const MIN_CANVAS_HEIGHT = 480;
const GUIDE_MATCH_TOLERANCE = 1;
const RESIZE_CORNER_THRESHOLD = 28;
const DEFAULT_ASPECT_RATIO = DEFAULT_SPACE_HEIGHT / DEFAULT_SPACE_WIDTH;
const RATIO_TOLERANCE = 0.015;
const LOG_AUTO_CLOSE_DELAY_MS = 1200;
let activeDragSession = null;
let activeResizeSession = null;

const state = {
  tree: [],
  selectedNodeId: null,
  selectedNodeType: "root",
  activePageId: null,
  expandedFolders: new Set(),
  spaceMoveContext: null,
};

setLogBackgroundAvailability(false);
updateResumePromptButton();

function mergeSpaceUpdate(spaceUpdate) {
  if (!spaceUpdate || !spaceUpdate.id || !Array.isArray(state.tree)) {
    return false;
  }
  const apply = (nodes) => {
    if (!Array.isArray(nodes)) {
      return false;
    }
    for (const node of nodes) {
      if (node.type === "tab" && Array.isArray(node.spaces)) {
        const target = node.spaces.find((space) => space.id === spaceUpdate.id);
        if (target) {
          if (typeof spaceUpdate.height === "number") {
            target.height = clampSpaceHeight(spaceUpdate.height);
          }
          if (typeof spaceUpdate.width === "number") {
            target.width = clampSpaceWidth(spaceUpdate.width);
          }
          if (typeof spaceUpdate.x === "number") {
            target.x = clampPositionValue(spaceUpdate.x);
          }
          if (typeof spaceUpdate.y === "number") {
            target.y = clampPositionValue(spaceUpdate.y);
          }
          if (spaceUpdate.updated_at || spaceUpdate.updatedAt) {
            target.updated_at = spaceUpdate.updated_at || spaceUpdate.updatedAt;
          }
          return true;
        }
      }
      if (node.type === "folder" && Array.isArray(node.children) && apply(node.children)) {
        return true;
      }
    }
    return false;
  };
  return apply(state.tree);
}

function toggleHamburgerMenu() {
  const isVisible = hamburgerMenu.classList.toggle("visible");
  hamburgerButton.setAttribute("aria-expanded", String(isVisible));
}

function closeHamburgerMenu() {
  hamburgerMenu.classList.remove("visible");
  hamburgerButton.setAttribute("aria-expanded", "false");
}

async function showConfigModal() {
  if (!configModal || !configForm) {
    return;
  }
  await fetchConnectorConfig();
  populateConnectorForm();
  configModal.classList.add("visible");
  configModal.setAttribute("aria-hidden", "false");
  if (configStatus) {
    configStatus.textContent = "";
  }
}

function hideConfigModal() {
  if (!configModal) {
    return;
  }
  configModal.classList.remove("visible");
  configModal.setAttribute("aria-hidden", "true");
}


function populateConnectorForm() {
  if (!configForm) {
    return;
  }
  const saved = getConnectorSnapshot();
  configForm.siteUrl.value = saved.siteUrl || "";
  configForm.projectKey.value = saved.projectKey || "";
  configForm.accountEmail.value = saved.accountEmail || "";
  configForm.apiKey.value = saved.apiKey || "";
  if (configForm.model) {
    configForm.model.value = saved.model && COPILOT_MODELS.includes(saved.model) ? saved.model : DEFAULT_MODEL;
  }
  if (configForm.copilotMaxIterations) {
    configForm.copilotMaxIterations.value = saved.copilotMaxIterations ?? DEFAULT_MAX_ITERATIONS;
  }
}

function buildConnectorPayload(formData) {
  return {
    siteUrl: (formData.get("siteUrl") || "").trim(),
    projectKey: (formData.get("projectKey") || "").trim(),
    accountEmail: (formData.get("accountEmail") || "").trim(),
    apiKey: (formData.get("apiKey") || "").trim(),
    model: formData.get("model") || DEFAULT_MODEL,
    copilotMaxIterations: (formData.get("copilotMaxIterations") || "").toString().trim(),
  };
}

async function handleTestConnector() {
  if (!configForm || !configStatus) {
    return;
  }
  const payload = resolveConnectorDefaults(buildConnectorPayload(new FormData(configForm)));
  if (!payload.siteUrl || !payload.accountEmail || !payload.apiKey) {
    configStatus.textContent = "Site URL, account email, and API token are required to test.";
    return;
  }
  configStatus.textContent = "Testing connection...";
  if (testConfigBtn) {
    testConfigBtn.disabled = true;
  }
  try {
    const response = await fetch("/api/connectors/test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to verify configuration");
    }
    configStatus.textContent = result.message || "Connection verified.";
  } catch (error) {
    configStatus.textContent = error.message || "Unable to verify configuration";
  } finally {
    if (testConfigBtn) {
      testConfigBtn.disabled = false;
    }
  }
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  }[char]));
}

function getSpaceImageUrl(space) {
  const version = space.updated_at || space.updatedAt || Date.now();
  const path = space.image_url || `/static/${space.image_path}`;
  return `${path}?v=${encodeURIComponent(version)}`;
}

async function copySpaceImageToClipboard(space) {
  if (!space) {
    window.alert("Space data missing");
    return;
  }
  if (!navigator.clipboard || typeof window.ClipboardItem !== "function") {
    window.alert("Clipboard image copying is not supported in this browser.");
    return;
  }
  const rawPath = space.image_url || (space.image_path ? `/static/${space.image_path}` : null);
  if (!rawPath) {
    window.alert("This space does not have an image yet.");
    return;
  }
  const imageUrl = getSpaceImageUrl(space);
  try {
    const response = await fetch(imageUrl, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Unable to download image (${response.status})`);
    }
    const blob = await response.blob();
    const mimeType = blob.type || "image/png";
    const clipboardItem = new window.ClipboardItem({ [mimeType]: blob });
    await navigator.clipboard.write([clipboardItem]);
  } catch (error) {
    console.error("Failed to copy image", error);
    window.alert(error.message || "Unable to copy image to clipboard.");
  }
}

function closeAllSpaceMenus() {
  document.querySelectorAll(".space-menu.open").forEach((menu) => menu.classList.remove("open"));
  document.querySelectorAll(".space-versions-list.open").forEach((list) => list.classList.remove("open"));
}

function collectAllTabs(nodes = state.tree, bucket = []) {
  if (!Array.isArray(nodes)) {
    return bucket;
  }
  nodes.forEach((node) => {
    if (!node) {
      return;
    }
    if (node.type === "tab") {
      bucket.push({ id: node.id, name: node.name || "Untitled page" });
    }
    if (node.type === "folder" && Array.isArray(node.children)) {
      collectAllTabs(node.children, bucket);
    }
  });
  return bucket;
}

function buildPromptMetadataHtml(space, connector) {
  const snapshot = connector || getConnectorSnapshot();
  const scriptPath = space.python_path || "auto-assigned";
  const tokenLabel = snapshot.apiKey ? "Configured" : "Not set";
  const lines = [
    `<strong>Jira Site:</strong> ${escapeHtml(snapshot.siteUrl || "Not configured")}`,
    `<strong>Project Key:</strong> ${escapeHtml(snapshot.projectKey || "Not set")}`,
    `<strong>Account:</strong> ${escapeHtml(snapshot.accountEmail || "Not set")}`,
    `<strong>API Token:</strong> ${escapeHtml(tokenLabel)}`,
    `<strong>Model:</strong> ${escapeHtml(snapshot.model || DEFAULT_MODEL)}`,
    `<strong>Copilot Attempts:</strong> ${escapeHtml(String(snapshot.copilotMaxIterations || DEFAULT_MAX_ITERATIONS))}`,
    `<strong>PNG Output:</strong> ${escapeHtml(space.image_path || "spaces/<id>.png")}`,
    `<strong>Script Path:</strong> ${escapeHtml(scriptPath)}`,
  ];
  if (Array.isArray(space.versions) && space.versions.length > 0) {
    lines.push("<strong>Existing versions detected. The new prompt will iterate on the selected version.</strong>");
  }
  return lines.join("<br />");
}

function showPromptModalForSpace(space, tabId) {
  activeSpaceId = space.id;
  activeSpaceTabId = tabId;
  promptInput.value = space.last_prompt || "";
  promptStatus.textContent = "";
  promptMetadata.innerHTML = buildPromptMetadataHtml(space, getConnectorSnapshot());
  promptModal.classList.add("visible");
  promptModal.setAttribute("aria-hidden", "false");
  closeAllSpaceMenus();
  setTimeout(() => promptInput.focus(), 10);
  fetchConnectorConfig().then((fresh) => {
    if (activeSpaceId === space.id) {
      promptMetadata.innerHTML = buildPromptMetadataHtml(space, fresh);
    }
  });
}

function hidePromptModal() {
  promptModal.classList.remove("visible");
  promptModal.setAttribute("aria-hidden", "true");
  promptStatus.textContent = "";
  promptMetadata.textContent = "";
  promptInput.value = "";
  activeSpaceId = null;
  activeSpaceTabId = null;
}

async function showCodeModalForSpace(spaceId, tabId) {
  try {
    activeSpaceId = spaceId;
    activeSpaceTabId = tabId;
    codeStatus.textContent = "Loading script...";
    closeAllSpaceMenus();
    const response = await fetch(`/api/spaces/${spaceId}`);
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error || "Unable to load script");
    }
    codeEditor.value = data.script || "";
    codeStatus.textContent = "";
    codeModal.classList.add("visible");
    codeModal.setAttribute("aria-hidden", "false");
    setTimeout(() => codeEditor.focus(), 10);
  } catch (error) {
    window.alert(error.message);
    activeSpaceId = null;
  }
}

function hideCodeModal() {
  codeModal.classList.remove("visible");
  codeModal.setAttribute("aria-hidden", "true");
  codeStatus.textContent = "";
  activeSpaceId = null;
  activeSpaceTabId = null;
}

function hideLogModal(options = {}) {
  const { resetState = true } = options;
  logModal.classList.remove("visible");
  logModal.setAttribute("aria-hidden", "true");
  stopThinkingWatchdog();
  if (resetState) {
    resetLogState("");
    setLogBackgroundAvailability(false);
  }
  hideLogScrollIndicator();
  logPrimaryAction = null;
}

function autoCloseLogModal({ delay = LOG_AUTO_CLOSE_DELAY_MS } = {}) {
  if (!logModal) {
    return;
  }
  const timeout = Math.max(0, Number(delay) || 0);
  window.setTimeout(() => {
    if (logModal.classList.contains("visible")) {
      hideLogModal();
    }
  }, timeout);
}

function setLogPrimaryButton(label, action) {
  if (label) {
    logPrimaryBtn.textContent = label;
    logPrimaryBtn.style.display = "inline-flex";
    logPrimaryAction = action || null;
  } else {
    logPrimaryBtn.style.display = "none";
    logPrimaryAction = null;
  }
}

function positionLogScrollIndicator() {
  if (!logScrollIndicator || !logPane || !logContent) {
    return;
  }
  const paneHeight = logPane.clientHeight;
  if (!paneHeight) {
    return;
  }
  const contentHeight = Math.max(logContent.scrollHeight, 1);
  const visibleBottom = Math.min(contentHeight, logContent.scrollTop + logContent.clientHeight);
  const ratio = visibleBottom / contentHeight;
  const padding = 12;
  const usableHeight = Math.max(paneHeight - padding * 2, 1);
  const offset = padding + usableHeight * ratio;
  logScrollIndicator.style.top = `${offset}px`;
}

function showLogScrollIndicator() {
  if (!logScrollIndicator) {
    return;
  }
  logScrollIndicator.classList.add("visible");
}

function hideLogScrollIndicator() {
  if (!logScrollIndicator) {
    return;
  }
  logScrollIndicator.classList.remove("visible");
}

function renderLogContent() {
  const lines = [...logHistory];
  if (thinkingLine) {
    lines.push(thinkingLine);
  }
  const shouldStickToBottom =
    logPane && logPane.scrollTop + logPane.clientHeight >= logPane.scrollHeight - 8;
  logContent.textContent = lines.join("\n");
  logContent.scrollTop = logContent.scrollHeight;
  if (logPane && shouldStickToBottom) {
    logPane.scrollTop = logPane.scrollHeight;
  }
  positionLogScrollIndicator();
  if (lines.length > 0) {
    showLogScrollIndicator();
  } else {
    hideLogScrollIndicator();
  }
}

function resetLogState(initialLog = "") {
  logHistory = initialLog && initialLog.trim() ? [initialLog.trim()] : [];
  thinkingLine = "";
  renderLogContent();
}

function openLogModal({ title, eyebrow, initialLog = "", primaryLabel, secondaryLabel = "Close", primaryAction, preserveLog = false }) {
  logTitle.textContent = title;
  logEyebrow.textContent = eyebrow || "Execution";
  if (preserveLog) {
    renderLogContent();
  } else {
    resetLogState(initialLog);
  }
  setLogPrimaryButton(primaryLabel || null, primaryAction || null);
  logSecondaryBtn.textContent = secondaryLabel || "Close";
  logModal.classList.add("visible");
  logModal.setAttribute("aria-hidden", "false");
}

function appendLogMessage(message) {
  if (typeof message !== "string" || !message.trim()) {
    return;
  }
  const normalized = message.replace(/\r?\n$/, "");
  logHistory.push(normalized);
  renderLogContent();
}

function setLogBackgroundAvailability(isVisible) {
  if (!logBackgroundBtn) {
    return;
  }
  if (isVisible) {
    logBackgroundBtn.style.display = "inline-flex";
    logBackgroundBtn.disabled = false;
  } else {
    logBackgroundBtn.style.display = "none";
    logBackgroundBtn.disabled = true;
  }
}

function updateResumePromptButton() {
  if (!resumePromptBtn) {
    return;
  }
  const count = backgroundPromptSpaces.size;
  if (count > 0) {
    resumePromptBtn.style.display = "block";
    resumePromptBtn.disabled = false;
    resumePromptBtn.textContent = count > 1 ? `Prompt to foreground (${count})` : "Prompt to foreground";
  } else {
    resumePromptBtn.style.display = "none";
    resumePromptBtn.disabled = true;
  }
}

function getOrAssignSpaceElevation(spaceId) {
  if (!spaceId) {
    return 1;
  }
  if (spaceElevation.has(spaceId)) {
    const stored = Number(spaceElevation.get(spaceId)) || 1;
    if (stored > highestSpaceElevation) {
      highestSpaceElevation = stored;
    }
    return stored;
  }
  const nextValue = ++highestSpaceElevation;
  spaceElevation.set(spaceId, nextValue);
  return nextValue;
}

function elevateSpaceCard(spaceId, card) {
  if (!spaceId || !card) {
    return;
  }
  const nextValue = ++highestSpaceElevation;
  spaceElevation.set(spaceId, nextValue);
  card.style.zIndex = String(nextValue);
}

function ensurePromptSession(spaceId, allowBackground = false) {
  if (!spaceId) {
    return null;
  }
  let session = promptSessions.get(spaceId);
  const backgroundable = Boolean(allowBackground);
  if (!session) {
    session = {
      allowBackground: backgroundable,
      logBuffer: [],
      backgrounded: false,
    };
    promptSessions.set(spaceId, session);
    promptInProgressSpaces.add(spaceId);
    renderTabs();
  } else if (backgroundable && !session.allowBackground) {
    session.allowBackground = true;
  }
  return session;
}

function resetPromptSessionLog(spaceId) {
  const session = promptSessions.get(spaceId);
  if (session) {
    session.logBuffer = [];
  }
}

function writePromptSessionLog(spaceId, message) {
  if (typeof message !== "string" || !message.trim()) {
    return;
  }
  const session = promptSessions.get(spaceId);
  const normalized = message.replace(/\r?\n$/, "");
  if (!session) {
    appendLogMessage(normalized);
    return;
  }
  session.logBuffer.push(normalized);
  if (!session.allowBackground) {
    appendLogMessage(normalized);
    return;
  }
  if (!session.backgrounded && currentPromptSpaceId === spaceId && logModal.classList.contains("visible")) {
    appendLogMessage(normalized);
  }
}

function focusPromptSession(spaceId) {
  const session = promptSessions.get(spaceId);
  if (!session) {
    currentPromptSpaceId = null;
    setLogBackgroundAvailability(false);
    return;
  }
  currentPromptSpaceId = spaceId;
  session.backgrounded = false;
  backgroundPromptSpaces.delete(spaceId);
  updateResumePromptButton();
  if (session.allowBackground) {
    setLogBackgroundAvailability(true);
  } else {
    setLogBackgroundAvailability(false);
  }
  logHistory = session.logBuffer.slice();
  thinkingLine = "";
  renderLogContent();
  renderTabs();
}

function finalizePromptSession(spaceId) {
  if (!spaceId) {
    return;
  }
  const session = promptSessions.get(spaceId);
  promptSessions.delete(spaceId);
  promptInProgressSpaces.delete(spaceId);
  backgroundPromptSpaces.delete(spaceId);
  if (currentPromptSpaceId === spaceId) {
    currentPromptSpaceId = null;
    setLogBackgroundAvailability(false);
  }
  updateResumePromptButton();
  renderTabs();
}

function handleRunPromptInBackground() {
  if (!currentPromptSpaceId) {
    hideLogModal();
    return;
  }
  const session = promptSessions.get(currentPromptSpaceId);
  if (!session || !session.allowBackground) {
    hideLogModal();
    return;
  }
  session.backgrounded = true;
  backgroundPromptSpaces.add(currentPromptSpaceId);
  updateResumePromptButton();
  renderTabs();
  setLogBackgroundAvailability(false);
  hideLogModal({ resetState: false });
  currentPromptSpaceId = null;
}

function resumePromptSession(spaceId) {
  const session = promptSessions.get(spaceId);
  if (!session) {
    backgroundPromptSpaces.delete(spaceId);
    updateResumePromptButton();
    window.alert("Prompt is no longer running for this space.");
    return;
  }
  closeHamburgerMenu();
  openLogModal({
    title: "Copilot output",
    eyebrow: "Copilot",
    initialLog: "",
    primaryLabel: null,
    secondaryLabel: "Close",
    preserveLog: true,
  });
  focusPromptSession(spaceId);
  startThinkingWatchdog();
}

function handleResumePromptClick() {
  if (!backgroundPromptSpaces.size) {
    return;
  }
  const candidates = [...backgroundPromptSpaces]
    .map((spaceId) => {
      const record = findSpaceRecord(spaceId);
      if (!record || !record.space) {
        backgroundPromptSpaces.delete(spaceId);
        return null;
      }
      return {
        spaceId,
        label: record.space.title || record.space.name || "Space",
      };
    })
    .filter(Boolean);
  if (!candidates.length) {
    updateResumePromptButton();
    window.alert("No background prompts are available.");
    return;
  }
  let target = candidates[0];
  if (candidates.length > 1) {
    const options = candidates.map((entry, index) => `${index + 1}. ${entry.label}`).join("\n");
    const selection = window.prompt(`Which prompt should return to the foreground?\n${options}\nEnter number:`, "");
    if (!selection) {
      return;
    }
    const choice = Number.parseInt(selection, 10);
    if (!Number.isFinite(choice) || choice < 1 || choice > candidates.length) {
      window.alert("Invalid selection.");
      return;
    }
    target = candidates[choice - 1];
  }
  resumePromptSession(target.spaceId);
}

function stopThinkingWatchdog() {
  if (thinkingTimeoutId) {
    clearTimeout(thinkingTimeoutId);
    thinkingTimeoutId = null;
  }
  if (thinkingIntervalId) {
    clearInterval(thinkingIntervalId);
    thinkingIntervalId = null;
  }
  if (thinkingLine) {
    thinkingLine = "";
    renderLogContent();
  }
}

function cancelAutoUpdateSchedule() {
  if (pendingAutoUpdateTimer) {
    clearTimeout(pendingAutoUpdateTimer);
    pendingAutoUpdateTimer = null;
  }
}

function scheduleAutoUpdate(spaceId, delay = 0) {
  cancelAutoUpdateSchedule();
  if (!spaceId) {
    return;
  }
  const execute = () => {
    pendingAutoUpdateTimer = null;
    if (logModal.classList.contains("visible")) {
      hideLogModal();
    }
    runUpdateAndShow(spaceId).catch((error) => {
      console.error("Auto update failed", error);
    });
  };
  if (delay <= 0) {
    execute();
    return;
  }
  pendingAutoUpdateTimer = setTimeout(execute, delay);
}

function startThinkingWatchdog() {
  if (logModal.getAttribute("aria-hidden") === "true") {
    return;
  }
  if (thinkingTimeoutId || thinkingIntervalId) {
    stopThinkingWatchdog();
  }
  thinkingTimeoutId = setTimeout(() => {
    thinkingFrameIndex = 0;
    thinkingLine = `[thinking] ${THINKING_FRAMES[thinkingFrameIndex]} waiting for Copilot...`;
    renderLogContent();
    thinkingIntervalId = setInterval(() => {
      thinkingFrameIndex = (thinkingFrameIndex + 1) % THINKING_FRAMES.length;
      thinkingLine = `[thinking] ${THINKING_FRAMES[thinkingFrameIndex]} waiting for Copilot...`;
      renderLogContent();
    }, 400);
  }, 1000);
}

function noteStreamActivity(eventType) {
  if (eventType === "complete" || eventType === "error") {
    stopThinkingWatchdog();
  } else {
    startThinkingWatchdog();
  }
}

function collectFolderIds(nodes, bag) {
  (nodes || []).forEach((node) => {
    if (node.type === "folder") {
      bag.add(node.id);
      collectFolderIds(node.children || [], bag);
    }
  });
}

function pruneExpandedFolders() {
  const validIds = new Set();
  collectFolderIds(state.tree, validIds);
  const next = new Set();
  state.expandedFolders.forEach((id) => {
    if (validIds.has(id)) {
      next.add(id);
    }
  });
  state.expandedFolders = next;
}

function toggleFolder(nodeId) {
  if (!nodeId) {
    return;
  }
  if (state.expandedFolders.has(nodeId)) {
    state.expandedFolders.delete(nodeId);
  } else {
    state.expandedFolders.add(nodeId);
  }
  renderTree();
}

async function loadTree() {
  const response = await fetch("/api/tree");
  const data = await response.json();
  state.tree = data.nodes || [];

  if (state.selectedNodeId) {
    const stillExists = findNodeInTree(state.tree, state.selectedNodeId);
    if (!stillExists) {
      resetSelection();
    }
  }

  if (state.activePageId) {
    const activeNode = findNodeInTree(state.tree, state.activePageId);
    if (!activeNode || activeNode.type !== "tab") {
      state.activePageId = null;
    }
  }

  if (state.spaceMoveContext) {
    const tabNode = findNodeInTree(state.tree, state.spaceMoveContext.tabId);
    const hasSpace = Boolean(tabNode && (tabNode.spaces || []).some((space) => space.id === state.spaceMoveContext.spaceId));
    if (!hasSpace) {
      state.spaceMoveContext = null;
      abortActiveDrag();
      document.body.classList.remove("space-move-active");
    }
  }

  pruneExpandedFolders();
  hideContextMenu();
  renderTree();
  renderTabs();
}

function renderTree() {
  treeRoot.innerHTML = "";
  const list = document.createElement("ul");
  list.className = "tree-list";

  state.tree.forEach((node) => {
    list.appendChild(renderNode(node));
  });

  treeRoot.appendChild(list);
}

function renderNode(node) {
  const item = document.createElement("li");
  item.className = "tree-branch";

  const row = document.createElement("div");
  row.className = `tree-node ${node.type}`;
  row.dataset.nodeId = node.id;
  row.dataset.nodeType = node.type;
  const isFolder = node.type === "folder";
  const hasChildren = Boolean(node.children && node.children.length);
  const isExpanded = isFolder && state.expandedFolders.has(node.id);
  if (state.selectedNodeId === node.id) {
    row.classList.add("selected");
  }
  if (isFolder) {
    row.classList.add(isExpanded ? "expanded" : "collapsed");
    row.title = "Double-click to expand/collapse";
  }
  row.addEventListener("click", (event) => {
    event.stopPropagation();
    setSelection(node);
    if (node.type === "tab") {
      openPage(node);
    }
  });
  row.addEventListener("contextmenu", (event) => {
    event.stopPropagation();
    setSelection(node);
    showContextMenu(event, node);
  });
  if (isFolder) {
    row.addEventListener("dblclick", (event) => {
      event.stopPropagation();
      toggleFolder(node.id);
    });
  }

  const label = document.createElement("span");
  label.className = "label";
  label.textContent = node.name;

  row.appendChild(label);
  item.appendChild(row);

  if (isFolder && isExpanded && hasChildren) {
    const childList = document.createElement("ul");
    childList.className = "child-list";
    node.children.forEach((child) => childList.appendChild(renderNode(child)));
    item.appendChild(childList);
  }

  return item;
}

function setSelection(node) {
  if (!node) {
    resetSelection();
    return;
  }

  state.selectedNodeId = node.id;
  state.selectedNodeType = node.type;
  renderTree();
}

function resetSelection() {
  state.selectedNodeId = null;
  state.selectedNodeType = "root";
  renderTree();
}

function showContextMenu(event, node) {
  event.preventDefault();
  menuTargetId = node.id;
  menuTargetType = node.type;
  if (renameNodeBtn) {
    renameNodeBtn.style.display = node.type === "tab" ? "block" : "none";
  }
  if (copyNodeBtn) {
    copyNodeBtn.style.display = node.type === "tab" ? "block" : "none";
  }
  if (addSpaceBtn) {
    addSpaceBtn.style.display = node.type === "tab" ? "block" : "none";
  }
  if (updateNodeBtn) {
    if (node.type === "tab") {
      updateNodeBtn.style.display = "block";
      const busy = tabUpdatesInFlight.has(node.id);
      updateNodeBtn.disabled = busy;
      updateNodeBtn.textContent = busy ? "Updating..." : "Update";
    } else {
      updateNodeBtn.style.display = "none";
      updateNodeBtn.disabled = false;
      updateNodeBtn.textContent = "Update";
    }
  }
  nodeMenu.style.left = "-9999px";
  nodeMenu.style.top = "-9999px";
  nodeMenu.classList.add("visible");
  const menuWidth = nodeMenu.offsetWidth || 180;
  const menuHeight = nodeMenu.offsetHeight || 60;
  const clickX = event.pageX;
  const clickY = event.pageY;
  const adjustedX = Math.min(clickX, window.innerWidth - menuWidth - 8);
  const adjustedY = Math.min(clickY, window.innerHeight - menuHeight - 8);
  nodeMenu.style.left = `${adjustedX}px`;
  nodeMenu.style.top = `${adjustedY}px`;
}

function hideContextMenu() {
  nodeMenu.classList.remove("visible");
  menuTargetId = null;
  menuTargetType = null;
}

async function handleRemoveNode() {
  if (!menuTargetId) {
    return;
  }
  const confirmMessage = menuTargetType === "folder"
    ? "Remove this folder and all nested items?"
    : "Remove this page?";
  const confirmed = window.confirm(confirmMessage);
  if (!confirmed) {
    hideContextMenu();
    return;
  }

  try {
    const response = await fetch(`/api/nodes/${menuTargetId}`, {
      method: "DELETE",
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to remove item");
    }

    if (state.selectedNodeId === menuTargetId) {
      resetSelection();
    }

    hideContextMenu();
    await loadTree();
  } catch (error) {
    window.alert(error.message || "Unable to remove item.");
  }
}

async function handleRenameNode() {
  if (!menuTargetId || menuTargetType !== "tab") {
    hideContextMenu();
    return;
  }
  const liveNode = findNodeInTree(state.tree, menuTargetId);
  const currentName = liveNode ? liveNode.name : "";
  const rawName = window.prompt("Rename page", currentName);
  if (rawName === null) {
    hideContextMenu();
    return;
  }
  const nextName = rawName.trim();
  if (!nextName) {
    window.alert("Name cannot be empty.");
    return;
  }
  try {
    const response = await fetch(`/api/nodes/${menuTargetId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: nextName }),
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to rename page");
    }
    hideContextMenu();
    await loadTree();
  } catch (error) {
    window.alert(error.message || "Unable to rename page.");
  }
}

async function copyTabById(tabId) {
  if (!tabId) {
    throw new Error("Page not found");
  }
  const response = await fetch(`/api/nodes/${tabId}/copy`, { method: "POST" });
  const result = await response.json();
  if (!response.ok) {
    throw new Error(result.error || "Unable to copy page");
  }
  const newTabId = result.node && result.node.id;
  await loadTree();
  if (newTabId) {
    const newTabNode = findNodeInTree(state.tree, newTabId);
    if (newTabNode) {
      openPage(newTabNode);
    }
  }
}

async function handleCopyNode() {
  if (!menuTargetId || menuTargetType !== "tab") {
    hideContextMenu();
    return;
  }
  const targetId = menuTargetId;
  hideContextMenu();
  try {
    await copyTabById(targetId);
  } catch (error) {
    window.alert(error.message || "Unable to copy page.");
  }
}

async function handleAddSpaceRequest() {
  if (!menuTargetId || menuTargetType !== "tab") {
    hideContextMenu();
    return;
  }
  const targetTabId = menuTargetId;
  hideContextMenu();
  await createSpace(targetTabId);
}

async function handleUpdateNode() {
  if (!menuTargetId || menuTargetType !== "tab") {
    hideContextMenu();
    return;
  }
  const targetTabId = menuTargetId;
  hideContextMenu();
  try {
    await runTabSequentialUpdate(targetTabId);
  } catch (error) {
    console.error("Page-level update failed", error);
    window.alert(error.message || "Unable to run updates for this page.");
  }
}

async function handleCreate(type) {
  closeHamburgerMenu();
  hideContextMenu();
  const canCreate = state.selectedNodeType === "folder" || state.selectedNodeType === "root";
  if (!canCreate) {
    window.alert("Select a folder to add new items.");
    return;
  }

  const promptLabel = type === "folder" ? "Enter folder name" : "Enter page name";
  const rawName = window.prompt(promptLabel, "");
  if (rawName === null) {
    return;
  }

  const name = rawName.trim();
  if (!name) {
    window.alert("Name cannot be empty.");
    return;
  }

  try {
    const payload = {
      name,
      type,
      parentId: state.selectedNodeType === "root" ? null : state.selectedNodeId,
    };
    const response = await fetch("/api/nodes", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to create item");
    }

    if (payload.parentId) {
      state.expandedFolders.add(payload.parentId);
    }
    if (result.node && result.node.type === "folder") {
      state.expandedFolders.add(result.node.id);
    }

    await loadTree();
    if (result.node && result.node.type === "tab") {
      const newPage = findNodeInTree(state.tree, result.node.id);
      if (newPage) {
        openPage(newPage);
      }
    }
  } catch (error) {
    window.alert(error.message || "Unable to create item.");
  }
}

async function createSpace(tabId) {
  try {
    const response = await fetch("/api/spaces", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tabId }),
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to create space");
    }
    await loadTree();
  } catch (error) {
    window.alert(error.message);
  }
}

async function startPromptStream(spaceId, promptText, options = {}) {
  const { allowBackground = false } = options;
  if (!spaceId) {
    writePromptSessionLog(spaceId, "Missing space context");
    return;
  }
  const session = ensurePromptSession(spaceId, allowBackground);
  if (session && allowBackground) {
    session.allowBackground = true;
  }
  await fetchConnectorConfig();
  const connector = getConnectorSnapshot();
  const model = connector.model && COPILOT_MODELS.includes(connector.model) ? connector.model : DEFAULT_MODEL;
  const payload = { prompt: promptText, connector, model };
  let response;
  try {
    response = await fetch(`/api/spaces/${spaceId}/prompt-stream`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
  } catch (error) {
    writePromptSessionLog(spaceId, `Network error: ${error.message}`);
    setLogPrimaryButton(null, null);
    stopThinkingWatchdog();
    finalizePromptSession(spaceId);
    return;
  }
  if (!response.ok || !response.body) {
    writePromptSessionLog(spaceId, "Failed to start Copilot stream");
    setLogPrimaryButton(null, null);
    stopThinkingWatchdog();
    finalizePromptSession(spaceId);
    return;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }
      buffer += decoder.decode(value, { stream: true });
      let newlineIndex;
      while ((newlineIndex = buffer.indexOf("\n")) >= 0) {
        const chunk = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if (!chunk) {
          continue;
        }
        try {
          const event = JSON.parse(chunk);
          handlePromptStreamEvent(event, spaceId);
          noteStreamActivity(event.type);
        } catch (error) {
          console.warn("Unable to parse stream chunk", chunk, error);
        }
      }
    }
    if (buffer.trim()) {
      try {
        const event = JSON.parse(buffer.trim());
        handlePromptStreamEvent(event, spaceId);
        noteStreamActivity(event.type);
      } catch (error) {
        console.warn("Unable to parse trailing chunk", buffer, error);
      }
    }
    stopThinkingWatchdog();
  } catch (error) {
    writePromptSessionLog(spaceId, `Stream interrupted: ${error.message}`);
    setLogPrimaryButton(null, null);
    stopThinkingWatchdog();
    finalizePromptSession(spaceId);
  } finally {
    reader.releaseLock();
    if (promptSessions.has(spaceId)) {
      finalizePromptSession(spaceId);
    }
  }
}

function handlePromptStreamEvent(event, spaceId) {
  if (!event || !event.type) {
    return;
  }
  switch (event.type) {
    case "status":
      writePromptSessionLog(spaceId, `[status] ${event.message}`);
      break;
    case "log":
      writePromptSessionLog(spaceId, event.message);
      break;
    case "error":
      writePromptSessionLog(spaceId, `ERROR: ${event.message}`);
      setLogPrimaryButton(null, null);
      finalizePromptSession(spaceId);
      break;
    case "complete":
      writePromptSessionLog(spaceId, "-- Copilot finished --");
      if (event.space) {
        loadTree();
      }
      setLogPrimaryButton(null, null);
      scheduleAutoUpdate(spaceId);
      finalizePromptSession(spaceId);
      break;
    default:
      break;
  }
}

async function updateSpace(spaceId, { suppressMenuClose = false } = {}) {
  if (!suppressMenuClose) {
    closeAllSpaceMenus();
  }
  const response = await fetch(`/api/spaces/${spaceId}/update`, {
    method: "POST",
  });
  const result = await response.json();
  if (!response.ok) {
    const error = new Error(result.error || "Unable to refresh space");
    error.details = {
      space: result.space,
      log: result.log,
      status: response.status,
    };
    throw error;
  }
  await loadTree();
  return result;
}

async function runTabSequentialUpdate(tabId) {
  if (!tabId) {
    return;
  }
  if (tabUpdatesInFlight.has(tabId)) {
    window.alert("An update is already running for this page.");
    return;
  }
  const tabNode = findNodeInTree(state.tree, tabId);
  if (!tabNode) {
    window.alert("Unable to find this page.");
    return;
  }
  const spaces = Array.isArray(tabNode.spaces) ? tabNode.spaces.filter((space) => space && space.id) : [];
  if (!spaces.length) {
    window.alert("This page has no spaces to update.");
    return;
  }
  const queue = spaces
    .filter((space) => !pendingSpaceUpdates.has(space.id))
    .map((space, index) => ({
      id: space.id,
      label: space.title || space.name || `Space ${index + 1}`,
    }));
  if (!queue.length) {
    window.alert("All spaces on this page are already updating.");
    return;
  }
  tabUpdatesInFlight.add(tabId);
  queue.forEach((item) => {
    pendingSpaceUpdates.add(item.id);
    spaceUpdateMessages.set(item.id, "Queued...");
  });
  renderTabs();
  const failures = [];
  let unexpectedError = null;
  try {
    for (const item of queue) {
      if (!pendingSpaceUpdates.has(item.id)) {
        continue;
      }
      spaceUpdateMessages.set(item.id, "Updating...");
      renderTabs();
      try {
        await updateSpace(item.id, { suppressMenuClose: true });
      } catch (error) {
        console.error("Space update failed", error);
        failures.push(`${item.label}: ${error.message || "Update failed"}`);
      } finally {
        pendingSpaceUpdates.delete(item.id);
        spaceUpdateMessages.delete(item.id);
        renderTabs();
      }
    }
  } catch (error) {
    unexpectedError = error;
    console.error("Tab update aborted", error);
  } finally {
    queue.forEach((item) => {
      pendingSpaceUpdates.delete(item.id);
      spaceUpdateMessages.delete(item.id);
    });
    tabUpdatesInFlight.delete(tabId);
    renderTabs();
  }
  if (unexpectedError && !failures.length) {
    failures.push(unexpectedError.message || "Unexpected error during page update");
  }
  if (failures.length) {
    window.alert(`Some spaces failed to update:\n${failures.join("\n")}`);
  }
}

async function runUpdateAndShow(spaceId) {
  if (!spaceId) {
    return;
  }
  closeAllSpaceMenus();
  openLogModal({
    title: "Script output",
    eyebrow: "Runtime",
    initialLog: "Generating chart...",
    primaryLabel: null,
    secondaryLabel: "Close",
  });
  try {
    const result = await updateSpace(spaceId, { suppressMenuClose: true });
    const successMessage = result?.log?.trim() || "Graph generation completed successfully";
    appendLogMessage(successMessage);
    if (!result) {
      setLogPrimaryButton(null, null);
      autoCloseLogModal();
      return;
    }

    const pngGenerated = typeof result.pngGenerated === "boolean"
      ? result.pngGenerated
      : Boolean(result.pngReady);

    if (pngGenerated) {
      setLogPrimaryButton(null, null);
      autoCloseLogModal();
      return;
    }

    const retryPrompt = result.space && result.space.last_prompt;
    if (!retryPrompt) {
      appendLogMessage("Script did not output a PNG and no saved prompt is available for retry.");
      setLogPrimaryButton(null, null);
      return;
    }

    appendLogMessage("Script did not output a PNG. Re-running Copilot with the last prompt...");
    appendLogMessage("Awaiting Copilot response...");
    startThinkingWatchdog();
    await startPromptStream(spaceId, retryPrompt);
  } catch (error) {
    const details = (error && error.details) || {};
    if (details.log) {
      appendLogMessage(details.log);
    }
    appendLogMessage(error && error.message ? error.message : "Unable to refresh space");
    const retryPrompt = details.space && details.space.last_prompt;
    if (retryPrompt) {
      appendLogMessage("Attempting Copilot repair with the saved prompt...");
      appendLogMessage("Awaiting Copilot response...");
      startThinkingWatchdog();
      await startPromptStream(spaceId, retryPrompt);
      return;
    }
    setLogPrimaryButton(null, null);
  }
}

async function removeSpace(spaceId) {
  const confirmed = window.confirm("Remove this space?");
  if (!confirmed) {
    return;
  }
  closeAllSpaceMenus();
  try {
    const response = await fetch(`/api/spaces/${spaceId}`, { method: "DELETE" });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to remove space");
    }
    spaceElevation.delete(spaceId);
    await loadTree();
  } catch (error) {
    window.alert(error.message);
  }
}

async function copySpace(spaceId) {
  closeAllSpaceMenus();
  try {
    const response = await fetch(`/api/spaces/${spaceId}/copy`, { method: "POST" });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to copy space");
    }
    await loadTree();
    const targetTabId = result.tabId;
    if (targetTabId) {
      const tabNode = findNodeInTree(state.tree, targetTabId);
      if (tabNode) {
        openPage(tabNode);
      }
    }
  } catch (error) {
    window.alert(error.message || "Unable to copy space");
  }
}

async function copySpaceToTab(spaceId) {
  closeAllSpaceMenus();
  const tabs = collectAllTabs(state.tree, []);
  if (!tabs.length) {
    window.alert("No pages available to copy into.");
    return;
  }
  const options = tabs.map((tab, index) => `${index + 1}. ${tab.name || "Untitled page"}`).join("\n");
  const selection = window.prompt(`Copy space to which page?\n${options}\nEnter number:`, "");
  if (!selection) {
    return;
  }
  const choice = Number.parseInt(selection, 10);
  if (!Number.isFinite(choice) || choice < 1 || choice > tabs.length) {
    window.alert("Invalid page selection.");
    return;
  }
  const targetTab = tabs[choice - 1];
  try {
    const response = await fetch(`/api/spaces/${spaceId}/copy-to/${targetTab.id}`, { method: "POST" });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to copy space to page");
    }
    await loadTree();
    const targetTabId = result.tabId;
    if (targetTabId) {
      const tabNode = findNodeInTree(state.tree, targetTabId);
      if (tabNode) {
        openPage(tabNode);
      }
    }
  } catch (error) {
    window.alert(error.message || "Unable to copy space to page");
  }
}

async function selectSpaceVersion(spaceId, versionId) {
  if (!spaceId || !versionId) {
    return;
  }
  try {
    const response = await fetch(`/api/spaces/${spaceId}/versions/${versionId}/activate`, {
      method: "POST",
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to switch version");
    }
    await loadTree();
  } catch (error) {
    window.alert(error.message || "Unable to switch version");
  }
}

async function saveSpaceCode(spaceId, code) {
  const response = await fetch(`/api/spaces/${spaceId}/code`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code }),
  });
  const result = await response.json();
  if (!response.ok) {
    throw new Error(result.error || "Unable to save code");
  }
  await loadTree();
  return result.log;
}

async function saveSpaceLayout(spaceId, layout, options = {}) {
  const { immediate = false } = options;
  if (!layout || (layout.height == null && layout.width == null && layout.x == null && layout.y == null)) {
    return;
  }
  const payload = JSON.stringify(layout);
  const requestUrl = `/api/spaces/${spaceId}`;
  if (immediate && typeof navigator !== "undefined" && typeof navigator.sendBeacon === "function") {
    try {
      const blob = new Blob([payload], { type: "application/json" });
      const sent = navigator.sendBeacon(requestUrl, blob);
      if (sent) {
        return;
      }
    } catch (error) {
      // Fallback to fetch below if sendBeacon fails
    }
  }
  try {
    const response = await fetch(requestUrl, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: payload,
      keepalive: immediate,
    });
    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.error || "Unable to persist layout");
    }
    if (result.space) {
      mergeSpaceUpdate(result.space);
    }
  } catch (error) {
    console.error("Failed to persist layout", error);
  }
}

function scheduleLayoutPersist(spaceId, patch = {}) {
  if (!spaceId || !patch) {
    return;
  }
  const payload = layoutPersistPayloads.get(spaceId) || {};
  let hasChanges = false;
  if (typeof patch.height === "number") {
    payload.height = clampSpaceHeight(patch.height);
    hasChanges = true;
  }
  if (typeof patch.width === "number") {
    payload.width = clampSpaceWidth(patch.width);
    hasChanges = true;
  }
  if (typeof patch.x === "number") {
    payload.x = clampPositionValue(patch.x);
    hasChanges = true;
  }
  if (typeof patch.y === "number") {
    payload.y = clampPositionValue(patch.y);
    hasChanges = true;
  }
  if (!hasChanges) {
    return;
  }
  layoutPersistPayloads.set(spaceId, payload);
  if (layoutPersistTimers.has(spaceId)) {
    clearTimeout(layoutPersistTimers.get(spaceId));
  }
  const timer = setTimeout(() => {
    layoutPersistTimers.delete(spaceId);
    const pending = layoutPersistPayloads.get(spaceId) || {};
    layoutPersistPayloads.delete(spaceId);
    saveSpaceLayout(spaceId, pending);
  }, 350);
  layoutPersistTimers.set(spaceId, timer);
}

function flushPendingLayoutPersists({ immediate = false } = {}) {
  if (!layoutPersistPayloads.size && !layoutPersistTimers.size) {
    return;
  }
  layoutPersistTimers.forEach((timer) => clearTimeout(timer));
  layoutPersistTimers.clear();
  const entries = Array.from(layoutPersistPayloads.entries());
  layoutPersistPayloads.clear();
  entries.forEach(([spaceId, payload]) => {
    const maybePromise = saveSpaceLayout(spaceId, payload, { immediate });
    if (maybePromise && typeof maybePromise.catch === "function") {
      maybePromise.catch((error) => {
        if (!immediate) {
          console.error("Failed to persist layout", error);
        }
      });
    }
  });
}

function updateCanvasAutoHeight(canvas) {
  if (!canvas) {
    return;
  }
  const parentHeight = Math.max(
    MIN_CANVAS_HEIGHT,
    canvas.parentElement ? canvas.parentElement.clientHeight : MIN_CANVAS_HEIGHT,
  );
  canvas.style.minHeight = `${parentHeight}px`;
  const cards = Array.from(canvas.querySelectorAll(".space-card"));
  if (!cards.length) {
    canvas.style.removeProperty("height");
    return;
  }
  let maxBottom = 0;
  cards.forEach((card) => {
    const top = Number.parseFloat(card.style.top) || 0;
    const height = card.offsetHeight || MIN_SPACE_HEIGHT;
    maxBottom = Math.max(maxBottom, top + height);
  });
  const required = Math.max(parentHeight, maxBottom + 80);
  if (required > parentHeight) {
    canvas.style.height = `${required}px`;
  } else {
    canvas.style.removeProperty("height");
  }
}

function measureCardBounds(card, widthOverride = null, heightOverride = null) {
  if (!card) {
    return { left: 0, top: 0, right: 0, bottom: 0 };
  }
  const left = Math.round(Number.parseFloat(card.style.left) || card.offsetLeft || 0);
  const top = Math.round(Number.parseFloat(card.style.top) || card.offsetTop || 0);
  const width = Math.round(widthOverride ?? card.offsetWidth ?? card.getBoundingClientRect().width ?? 0);
  const height = Math.round(heightOverride ?? card.offsetHeight ?? card.getBoundingClientRect().height ?? 0);
  return { left, top, right: left + width, bottom: top + height };
}

function ensureGuideLayer(canvas) {
  if (!canvas) {
    return null;
  }
  let layer = canvas.querySelector(".canvas-guides");
  if (!layer) {
    layer = document.createElement("div");
    layer.className = "canvas-guides";
    canvas.appendChild(layer);
  }
  return layer;
}

function renderCanvasGuides(canvas, movingCard, highlightEdges = null) {
  const layer = ensureGuideLayer(canvas);
  if (!layer) {
    return;
  }
  layer.innerHTML = "";
  const cards = Array.from(canvas.querySelectorAll(".space-card"));
  const verticalPositions = new Set();
  const horizontalPositions = new Set();
  cards.forEach((card) => {
    if (card === movingCard) {
      return;
    }
    const left = Math.round(Number.parseFloat(card.style.left) || card.offsetLeft || 0);
    const top = Math.round(Number.parseFloat(card.style.top) || card.offsetTop || 0);
    const width = Math.round(card.offsetWidth || card.getBoundingClientRect().width || 0);
    const height = Math.round(card.offsetHeight || card.getBoundingClientRect().height || 0);
    verticalPositions.add(left);
    verticalPositions.add(left + width);
    horizontalPositions.add(top);
    horizontalPositions.add(top + height);
  });
  const highlight = highlightEdges || {};
  const highlightLeft = typeof highlight.left === "number" ? highlight.left : null;
  const highlightRight = typeof highlight.right === "number" ? highlight.right : null;
  const highlightTop = typeof highlight.top === "number" ? highlight.top : null;
  const highlightBottom = typeof highlight.bottom === "number" ? highlight.bottom : null;
  const tolerance = GUIDE_MATCH_TOLERANCE;

  verticalPositions.forEach((position) => {
    const line = document.createElement("div");
    line.className = "guide-line vertical";
    line.style.left = `${position}px`;
    if (
      highlightEdges &&
      ((highlightLeft != null && Math.abs(position - highlightLeft) <= tolerance) ||
        (highlightRight != null && Math.abs(position - highlightRight) <= tolerance))
    ) {
      line.classList.add("matching");
    }
    layer.appendChild(line);
  });
  horizontalPositions.forEach((position) => {
    const line = document.createElement("div");
    line.className = "guide-line horizontal";
    line.style.top = `${position}px`;
    if (
      highlightEdges &&
      ((highlightTop != null && Math.abs(position - highlightTop) <= tolerance) ||
        (highlightBottom != null && Math.abs(position - highlightBottom) <= tolerance))
    ) {
      line.classList.add("matching");
    }
    layer.appendChild(line);
  });
  if (!verticalPositions.size && !horizontalPositions.size) {
    layer.classList.remove("visible");
  } else {
    layer.classList.add("visible");
  }
}

function clearCanvasGuides(canvas) {
  if (!canvas) {
    return;
  }
  const layer = canvas.querySelector(".canvas-guides");
  if (layer) {
    layer.innerHTML = "";
    layer.classList.remove("visible");
  }
}

function beginSpaceResize(event, space, tabId, card, view) {
  if (!space || !card || !view || event.button !== 0) {
    return;
  }
  const canvas = card.closest(".spaces-canvas");
  if (!canvas) {
    return;
  }
  const target = event.currentTarget;
  const rect = target.getBoundingClientRect();
  const localX = typeof event.offsetX === "number" ? event.offsetX : event.clientX - rect.left;
  const localY = typeof event.offsetY === "number" ? event.offsetY : event.clientY - rect.top;
  const nearRightEdge = localX >= target.clientWidth - RESIZE_CORNER_THRESHOLD;
  const nearBottomEdge = localY >= target.clientHeight - RESIZE_CORNER_THRESHOLD;
  if (!nearRightEdge || !nearBottomEdge) {
    return;
  }
  event.preventDefault();
  event.stopPropagation();
  if (activeResizeSession && typeof activeResizeSession.cleanup === "function") {
    activeResizeSession.cleanup();
  }
  const handlePointerEnd = (pointerEvent) => {
    finishSpaceResize(pointerEvent);
  };
  const pointerMoveListener = (pointerEvent) => {
    handleSpaceResizePointerMove(pointerEvent);
  };
  window.addEventListener("pointermove", pointerMoveListener, true);
  window.addEventListener("pointerup", handlePointerEnd, true);
  window.addEventListener("pointercancel", handlePointerEnd, true);
  const cardRect = card.getBoundingClientRect();
  const startWidth = clampSpaceWidth(space.width ?? cardRect.width ?? DEFAULT_SPACE_WIDTH);
  const startHeight = clampSpaceHeight(space.height ?? cardRect.height ?? DEFAULT_SPACE_HEIGHT);
  const aspectRatio = resolveSpaceAspectRatio(space, view) || DEFAULT_ASPECT_RATIO;
  const previousResizeStyle = target.style.resize || "";
  target.style.resize = "none";
  if (typeof target.setPointerCapture === "function" && typeof event.pointerId === "number") {
    try {
      target.setPointerCapture(event.pointerId);
    } catch (error) {
      // ignore capture issues
    }
  }
  activeResizeSession = {
    pointerId: event.pointerId,
    spaceId: space.id,
    tabId,
    card,
    view,
    canvas,
    spaceRef: space,
    startPointerX: event.clientX,
    startPointerY: event.clientY,
    startWidth,
    startHeight,
    lastWidth: startWidth,
    lastHeight: startHeight,
    aspectRatio,
    captureTarget: target,
    previousResizeStyle,
    cleanup: () => {
      window.removeEventListener("pointermove", pointerMoveListener, true);
      window.removeEventListener("pointerup", handlePointerEnd, true);
      window.removeEventListener("pointercancel", handlePointerEnd, true);
    },
  };
  document.body.classList.add("space-resize-active");
  const bounds = measureCardBounds(card, startWidth, startHeight);
  renderCanvasGuides(canvas, card, bounds);
}

function handleSpaceResizePointerMove(event) {
  const session = activeResizeSession;
  if (!session || event.pointerId !== session.pointerId) {
    return;
  }
  event.preventDefault();
  event.stopPropagation();
  const deltaX = event.clientX - session.startPointerX;
  const deltaY = event.clientY - session.startPointerY;
  let nextWidth = clampSpaceWidth(session.startWidth + deltaX);
  let nextHeight = clampSpaceHeight(session.startHeight + deltaY);
  const ratio = session.aspectRatio || resolveSpaceAspectRatio(session.spaceRef, session.view) || DEFAULT_ASPECT_RATIO;
  if (ratio) {
    if (Math.abs(deltaX) >= Math.abs(deltaY)) {
      nextHeight = clampSpaceHeight(nextWidth * ratio);
    } else {
      nextWidth = clampSpaceWidth(nextHeight / ratio);
      nextHeight = clampSpaceHeight(nextWidth * ratio);
    }
  }
  session.lastWidth = nextWidth;
  session.lastHeight = nextHeight;
  if (session.card) {
    session.card.style.width = `${nextWidth}px`;
    session.card.style.height = `${nextHeight}px`;
  }
  if (session.view) {
    session.view.style.width = `${nextWidth}px`;
    session.view.style.height = `${nextHeight}px`;
  }
  if (session.spaceRef) {
    session.spaceRef.width = nextWidth;
    session.spaceRef.height = nextHeight;
  }
  if (session.canvas) {
    const bounds = measureCardBounds(session.card, nextWidth, nextHeight);
    renderCanvasGuides(session.canvas, session.card, bounds);
    updateCanvasAutoHeight(session.canvas);
  }
}

function finishSpaceResize(event = null) {
  const session = activeResizeSession;
  if (!session) {
    return;
  }
  if (
    event &&
    typeof event.pointerId === "number" &&
    typeof session.pointerId === "number" &&
    event.pointerId !== session.pointerId &&
    event.type !== "pointercancel"
  ) {
    return;
  }
  const shouldPersist = !event || event.type !== "pointercancel";
  if (session.cleanup) {
    session.cleanup();
  }
  if (session.captureTarget && typeof session.captureTarget.releasePointerCapture === "function") {
    try {
      session.captureTarget.releasePointerCapture(session.pointerId);
    } catch (error) {
      // ignore release issues
    }
  }
  if (session.view && typeof session.previousResizeStyle !== "undefined") {
    session.view.style.resize = session.previousResizeStyle || "";
  }
  const fallbackWidth = clampSpaceWidth(session.startWidth ?? session.card?.offsetWidth ?? DEFAULT_SPACE_WIDTH);
  const fallbackHeight = clampSpaceHeight(session.startHeight ?? session.card?.offsetHeight ?? DEFAULT_SPACE_HEIGHT);
  const finalWidth = shouldPersist ? clampSpaceWidth(session.lastWidth ?? fallbackWidth) : fallbackWidth;
  const finalHeight = shouldPersist ? clampSpaceHeight(session.lastHeight ?? fallbackHeight) : fallbackHeight;
  if (session.card) {
    session.card.style.width = `${finalWidth}px`;
    session.card.style.height = `${finalHeight}px`;
  }
  if (session.view) {
    session.view.style.width = `${finalWidth}px`;
    session.view.style.height = `${finalHeight}px`;
  }
  if (session.spaceRef) {
    session.spaceRef.width = finalWidth;
    session.spaceRef.height = finalHeight;
  }
  if (shouldPersist && session.spaceId) {
    scheduleLayoutPersist(session.spaceId, { width: finalWidth, height: finalHeight });
  }
  updateCanvasAutoHeight(session.canvas);
  clearCanvasGuides(session.canvas);
  document.body.classList.remove("space-resize-active");
  activeResizeSession = null;
}

function beginSpaceDrag(event, space, tabId, card) {
  if (
    !space ||
    !card ||
    !state.spaceMoveContext ||
    state.spaceMoveContext.spaceId !== space.id ||
    state.spaceMoveContext.tabId !== tabId ||
    event.button !== 0 ||
    event.target.closest(".space-menu-root")
  ) {
    return;
  }
  const canvas = card.closest(".spaces-canvas");
  if (!canvas) {
    return;
  }
  event.preventDefault();
  event.stopPropagation();
  abortActiveDrag();
  const coords = normalizeSpacePosition(space);
  card.style.left = `${coords.x}px`;
  card.style.top = `${coords.y}px`;
  const cardRect = card.getBoundingClientRect();
  const session = {
    pointerId: event.pointerId,
    card,
    canvas,
    spaceId: space.id,
    tabId,
    spaceRef: space,
    offsetX: event.clientX - cardRect.left,
    offsetY: event.clientY - cardRect.top,
    startLeft: coords.x,
    startTop: coords.y,
    lastLeft: coords.x,
    lastTop: coords.y,
    cardWidth: cardRect.width,
    cardHeight: cardRect.height,
    originalZ: card.style.zIndex || "",
  };
  activeDragSession = session;
  card.classList.add("dragging");
  renderCanvasGuides(canvas, card, {
    left: coords.x,
    top: coords.y,
    right: coords.x + cardRect.width,
    bottom: coords.y + cardRect.height,
  });
  card.style.zIndex = 999;
  card.setPointerCapture(event.pointerId);
  card.addEventListener("pointermove", handleSpacePointerMove);
  card.addEventListener("pointerup", finishSpaceDrag);
  card.addEventListener("pointercancel", finishSpaceDrag);
}

function handleSpacePointerMove(event) {
  const session = activeDragSession;
  if (!session || event.pointerId !== session.pointerId) {
    return;
  }
  const canvasRect = session.canvas.getBoundingClientRect();
  const nextLeft = event.clientX - canvasRect.left + session.canvas.scrollLeft - session.offsetX;
  const nextTop = event.clientY - canvasRect.top + session.canvas.scrollTop - session.offsetY;
  const clampedLeft = clampPositionValue(nextLeft);
  const clampedTop = clampPositionValue(nextTop);
  session.card.style.left = `${clampedLeft}px`;
  session.card.style.top = `${clampedTop}px`;
  session.lastLeft = clampedLeft;
  session.lastTop = clampedTop;
  const cardRect = session.card.getBoundingClientRect();
  session.cardWidth = cardRect.width;
  session.cardHeight = cardRect.height;
  renderCanvasGuides(session.canvas, session.card, {
    left: clampedLeft,
    top: clampedTop,
    right: clampedLeft + cardRect.width,
    bottom: clampedTop + cardRect.height,
  });
  const requiredHeight = clampedTop + session.cardHeight + 80;
  const currentHeight = Math.max(session.canvas.clientHeight, MIN_CANVAS_HEIGHT);
  if (requiredHeight > currentHeight) {
    session.canvas.style.height = `${requiredHeight}px`;
  }
}

function finishSpaceDrag(event) {
  const session = activeDragSession;
  if (!session || event.pointerId !== session.pointerId) {
    return;
  }
  const shouldPersist = event.type !== "pointercancel";
  try {
    session.card.releasePointerCapture(event.pointerId);
  } catch (error) {
    // ignore release issues
  }
  session.card.classList.remove("dragging");
  session.card.style.zIndex = session.originalZ;
  session.card.removeEventListener("pointermove", handleSpacePointerMove);
  session.card.removeEventListener("pointerup", finishSpaceDrag);
  session.card.removeEventListener("pointercancel", finishSpaceDrag);
  const finalLeft = shouldPersist ? (session.lastLeft ?? session.startLeft) : session.startLeft;
  const finalTop = shouldPersist ? (session.lastTop ?? session.startTop) : session.startTop;
  session.card.style.left = `${finalLeft}px`;
  session.card.style.top = `${finalTop}px`;
  if (shouldPersist && session.spaceRef && typeof session.spaceRef === "object") {
    session.spaceRef.x = finalLeft;
    session.spaceRef.y = finalTop;
    scheduleLayoutPersist(session.spaceId, { x: finalLeft, y: finalTop });
    updateCanvasAutoHeight(session.canvas);
  }
  clearCanvasGuides(session.canvas);
  activeDragSession = null;
  cancelSpaceMove();
}

function abortActiveDrag({ persist = false } = {}) {
  const session = activeDragSession;
  if (!session) {
    return;
  }
  try {
    session.card.releasePointerCapture(session.pointerId);
  } catch (error) {
    // ignore
  }
  session.card.classList.remove("dragging");
  session.card.style.zIndex = session.originalZ;
  session.card.removeEventListener("pointermove", handleSpacePointerMove);
  session.card.removeEventListener("pointerup", finishSpaceDrag);
  session.card.removeEventListener("pointercancel", finishSpaceDrag);
  const finalLeft = persist ? (session.lastLeft ?? session.startLeft) : session.startLeft;
  const finalTop = persist ? (session.lastTop ?? session.startTop) : session.startTop;
  session.card.style.left = `${finalLeft}px`;
  session.card.style.top = `${finalTop}px`;
  if (persist && session.spaceRef && typeof session.spaceRef === "object") {
    session.spaceRef.x = finalLeft;
    session.spaceRef.y = finalTop;
    scheduleLayoutPersist(session.spaceId, { x: finalLeft, y: finalTop });
    updateCanvasAutoHeight(session.canvas);
  }
  clearCanvasGuides(session.canvas);
  activeDragSession = null;
}

function observeSpaceResize(spaceId, element, initialSize, spaceRef, card) {
  if (!spaceId || !element || typeof ResizeObserver === "undefined") {
    return;
  }
  const initialBounds = element.getBoundingClientRect();
  const parentCanvas = card?.closest(".spaces-canvas") || null;
  let lastHeight = clampSpaceHeight(initialSize?.height ?? initialBounds.height);
  let lastWidth = clampSpaceWidth(initialSize?.width ?? initialBounds.width);
  let suppressNextNotification = false;
  let pendingAnimationFrame = false;
  let queuedRect = initialBounds;

  const processResize = () => {
    pendingAnimationFrame = false;
    if (!element.isConnected) {
      return;
    }
    const rect = queuedRect;
    if (!rect || rect.width < 1 || rect.height < 1) {
      return;
    }
    let nextHeight = clampSpaceHeight(rect.height ?? lastHeight);
    let nextWidth = clampSpaceWidth(rect.width ?? lastWidth);
    const prevHeight = lastHeight;
    const prevWidth = lastWidth;
    const ratio = resolveSpaceAspectRatio(spaceRef, element);
    const widthDrift = Math.abs(nextWidth - prevWidth);
    const heightDrift = Math.abs(nextHeight - prevHeight);
    const measuredRatio = nextHeight / Math.max(nextWidth, 1);
    if (ratio && Math.abs(measuredRatio - ratio) > RATIO_TOLERANCE) {
      if (widthDrift >= heightDrift) {
        nextHeight = clampSpaceHeight(nextWidth * ratio);
      } else {
        nextWidth = clampSpaceWidth(nextHeight / ratio);
        nextHeight = clampSpaceHeight(nextWidth * ratio);
      }
      suppressNextNotification = true;
      element.style.width = `${nextWidth}px`;
      element.style.height = `${nextHeight}px`;
    }
    if (card) {
      card.style.width = `${nextWidth}px`;
      card.style.height = `${nextHeight}px`;
    }
    if (spaceRef && typeof spaceRef === "object") {
      spaceRef.height = nextHeight;
      spaceRef.width = nextWidth;
    }
    const shouldPersist = Math.abs(nextHeight - prevHeight) >= 2 || Math.abs(nextWidth - prevWidth) >= 2;
    lastHeight = nextHeight;
    lastWidth = nextWidth;

    if (activeResizeSession && activeResizeSession.spaceId === spaceId && (activeResizeSession.canvas || parentCanvas)) {
      const bounds = measureCardBounds(card, nextWidth, nextHeight);
      renderCanvasGuides(activeResizeSession.canvas || parentCanvas, card, bounds);
    }

    if (shouldPersist) {
      scheduleLayoutPersist(spaceId, { height: nextHeight, width: nextWidth });
      updateCanvasAutoHeight(parentCanvas || card?.closest(".spaces-canvas"));
    }
  };

  const queueProcess = () => {
    if (pendingAnimationFrame) {
      return;
    }
    pendingAnimationFrame = true;
    const runner = () => {
      processResize();
    };
    if (typeof window !== "undefined" && typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(runner);
    } else {
      setTimeout(runner, 16);
    }
  };

  const observer = new ResizeObserver((entries) => {
    if (suppressNextNotification) {
      suppressNextNotification = false;
      return;
    }
    if (!element.isConnected) {
      observer.disconnect();
      return;
    }
    const entry = entries[entries.length - 1];
    if (!entry?.contentRect || entry.contentRect.width < 1 || entry.contentRect.height < 1) {
      return;
    }
    queuedRect = entry.contentRect;
    queueProcess();
  });

  observer.observe(element);
}

function activateSpaceMove(spaceId, tabId) {
  closeAllSpaceMenus();
  if (state.spaceMoveContext && state.spaceMoveContext.spaceId === spaceId && state.spaceMoveContext.tabId === tabId) {
    cancelSpaceMove();
    return;
  }
  state.spaceMoveContext = { spaceId, tabId };
  document.body.classList.add("space-move-active");
  renderTabs();
}

function cancelSpaceMove() {
  const wasActive = Boolean(state.spaceMoveContext);
  abortActiveDrag();
  if (wasActive) {
    state.spaceMoveContext = null;
  }
  document.body.classList.remove("space-move-active");
  if (wasActive) {
    renderTabs();
  }
}


async function handlePromptSubmit(event) {
  event.preventDefault();
  if (!activeSpaceId) {
    promptStatus.textContent = "Select a space first";
    return;
  }
  const text = promptInput.value.trim();
  if (!text) {
    promptStatus.textContent = "Prompt cannot be empty";
    return;
  }
  const targetSpaceId = activeSpaceId;
  hidePromptModal();
  const session = ensurePromptSession(targetSpaceId, true);
  if (session) {
    session.backgrounded = false;
    resetPromptSessionLog(targetSpaceId);
  }
  backgroundPromptSpaces.delete(targetSpaceId);
  updateResumePromptButton();
  openLogModal({
    title: "Copilot output",
    eyebrow: "Copilot",
    initialLog: "",
    primaryLabel: null,
    secondaryLabel: "Close",
  });
  focusPromptSession(targetSpaceId);
  writePromptSessionLog(targetSpaceId, "Generating with Copilot...");
  writePromptSessionLog(targetSpaceId, "Awaiting Copilot response...");
  startThinkingWatchdog();
  await startPromptStream(targetSpaceId, text, { allowBackground: true });
}

async function handleCodeSubmit(event) {
  event.preventDefault();
  if (!activeSpaceId) {
    codeStatus.textContent = "Select a space first";
    return;
  }
  codeStatus.textContent = "Saving...";
  try {
    const log = await saveSpaceCode(activeSpaceId, codeEditor.value);
    codeStatus.textContent = log || "Saved";
    setTimeout(() => {
      hideCodeModal();
    }, 700);
  } catch (error) {
    codeStatus.textContent = error.message;
  }
}

function openPage(node) {
  if (!node) {
    return;
  }
  state.activePageId = node.id;
  renderTabs();
}

function captureLiveSpaceMetrics() {
  const cards = document.querySelectorAll(".space-card");
  if (!cards.length) {
    return;
  }
  cards.forEach((card) => {
    const panel = card.closest(".page-panel");
    if (!panel) {
      return;
    }
    if (card.offsetParent === null || panel.offsetParent === null) {
      return;
    }
    const spaceId = card.dataset.spaceId;
    if (!spaceId) {
      return;
    }
    const view = card.querySelector(".space-view");
    const rect = view ? view.getBoundingClientRect() : card.getBoundingClientRect();
    const measuredWidth = clampSpaceWidth(rect.width || view?.offsetWidth || card.offsetWidth);
    const measuredHeight = clampSpaceHeight(rect.height || view?.offsetHeight || card.offsetHeight);
    const record = findSpaceRecord(spaceId);
    if (!record || !record.space) {
      return;
    }
    const { space } = record;
    const patch = {};
    if (!Number.isFinite(Number(space.width)) || Math.abs(Number(space.width) - measuredWidth) >= 2) {
      space.width = measuredWidth;
      patch.width = measuredWidth;
    }
    if (!Number.isFinite(Number(space.height)) || Math.abs(Number(space.height) - measuredHeight) >= 2) {
      space.height = measuredHeight;
      patch.height = measuredHeight;
    }
    if (Object.keys(patch).length) {
      scheduleLayoutPersist(spaceId, patch);
    }
  });
}

function capturePageScrollState() {
  if (!pageContent) {
    return null;
  }
  const snapshot = {
    containerTop: pageContent.scrollTop,
    containerLeft: pageContent.scrollLeft,
    panels: [],
  };
  const panels = pageContent.querySelectorAll(".page-panel[data-page-id]");
  panels.forEach((panel) => {
    const pageId = panel.dataset.pageId;
    if (!pageId) {
      return;
    }
    const canvas = panel.querySelector(".spaces-canvas");
    snapshot.panels.push({
      pageId,
      panelTop: panel.scrollTop,
      panelLeft: panel.scrollLeft,
      canvasTop: canvas ? canvas.scrollTop : 0,
      canvasLeft: canvas ? canvas.scrollLeft : 0,
    });
  });
  return snapshot;
}

function restorePageScrollState(snapshot) {
  if (!snapshot || !pageContent) {
    return;
  }
  if (Number.isFinite(snapshot.containerTop)) {
    pageContent.scrollTop = snapshot.containerTop;
  }
  if (Number.isFinite(snapshot.containerLeft)) {
    pageContent.scrollLeft = snapshot.containerLeft;
  }
  if (!Array.isArray(snapshot.panels) || !snapshot.panels.length) {
    return;
  }
  snapshot.panels.forEach((entry) => {
    if (!entry || !entry.pageId) {
      return;
    }
    const panel = pageContent.querySelector(`.page-panel[data-page-id="${entry.pageId}"]`);
    if (!panel) {
      return;
    }
    const canvas = panel.querySelector(".spaces-canvas");
    if (Number.isFinite(entry.panelTop)) {
      panel.scrollTop = entry.panelTop;
    }
    if (Number.isFinite(entry.panelLeft)) {
      panel.scrollLeft = entry.panelLeft;
    }
    if (canvas) {
      if (Number.isFinite(entry.canvasTop)) {
        canvas.scrollTop = entry.canvasTop;
      }
      if (Number.isFinite(entry.canvasLeft)) {
        canvas.scrollLeft = entry.canvasLeft;
      }
    }
  });
}

function renderTabs() {
  captureLiveSpaceMetrics();
  const scrollState = capturePageScrollState();
  if (activeResizeSession && (!activeResizeSession.card || !document.body.contains(activeResizeSession.card))) {
    finishSpaceResize();
  }
  if (!pageContent) {
    return;
  }
  pageContent.innerHTML = "";

  if (!state.activePageId) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.innerHTML = "<h4>No page selected</h4><p>Select any page from the left pane.</p>";
    pageContent.appendChild(empty);
    return;
  }

  const livePage = findNodeInTree(state.tree, state.activePageId);
  if (!livePage || livePage.type !== "tab") {
    state.activePageId = null;
    renderTabs();
    return;
  }

  const panel = document.createElement("article");
  panel.className = "page-panel";
  panel.dataset.pageId = String(livePage.id);

  const reorderActive = Boolean(state.spaceMoveContext && state.spaceMoveContext.tabId === livePage.id);
  if (reorderActive) {
    const moveHint = document.createElement("p");
    moveHint.className = "move-hint";
    moveHint.textContent = "Drag the highlighted space anywhere on the canvas. Press Esc or click outside to cancel.";
    panel.appendChild(moveHint);
  }

  const canvas = document.createElement("div");
  canvas.className = "spaces-canvas";
  const spaces = livePage.spaces || [];
  if (!spaces.length) {
    canvas.classList.add("empty");
    const placeholder = document.createElement("div");
    placeholder.className = "empty-state";
    placeholder.innerHTML = "<h4>No spaces yet</h4><p>Create a space to start visualizing insights.</p>";
    canvas.appendChild(placeholder);
  } else {
    spaces.forEach((space) => {
      const card = renderSpaceCard(space, livePage.id, canvas);
      canvas.appendChild(card);
    });
  }
  updateCanvasAutoHeight(canvas);

  panel.appendChild(canvas);
  pageContent.appendChild(panel);
  restorePageScrollState(scrollState);
}

function renderSpaceCard(space, tabId, canvas) {
  const card = document.createElement("div");
  card.className = "space-card";
  card.dataset.spaceId = space.id;
  let initialHeight = clampSpaceHeight(space.height);
  let initialWidth = clampSpaceWidth(space.width);
  const coords = normalizeSpacePosition(space);
  space.x = coords.x;
  space.y = coords.y;
  card.style.left = `${coords.x}px`;
  card.style.top = `${coords.y}px`;
  card.style.width = `${initialWidth}px`;
  card.style.height = `${initialHeight}px`;
  const initialElevation = getOrAssignSpaceElevation(space.id);
  card.style.zIndex = String(initialElevation);
  card.addEventListener(
    "pointerdown",
    () => {
      elevateSpaceCard(space.id, card);
    },
    { capture: true },
  );
  const isPendingUpdate = pendingSpaceUpdates.has(space.id);
  const pendingMessage = spaceUpdateMessages.get(space.id) || "Queued...";
  const isPromptRunning = promptInProgressSpaces.has(space.id);
  const isBackgroundPrompt = backgroundPromptSpaces.has(space.id);
  if (isPendingUpdate) {
    card.classList.add("pending-update");
  }
  if (isPromptRunning) {
    card.classList.add("prompt-running");
  }
  const moveContext = state.spaceMoveContext;
  const isMoveActive = Boolean(moveContext && moveContext.tabId === tabId);
  const isDragSource = isMoveActive && moveContext.spaceId === space.id;

  if (isMoveActive) {
    card.classList.add("move-enabled");
  }
  if (isDragSource) {
    card.classList.add("move-source");
    card.addEventListener("pointerdown", (event) => beginSpaceDrag(event, space, tabId, card));
  }

  const menuRoot = document.createElement("div");
  menuRoot.className = "space-menu-root";
  const menuButton = document.createElement("button");
  menuButton.type = "button";
  menuButton.className = "space-menu-button";
  menuButton.textContent = "≡";
  const menu = document.createElement("div");
  menu.className = "space-menu";

  const actions = [
    { label: "Move", handler: () => activateSpaceMove(space.id, tabId) },
    { label: "Prompt", handler: () => showPromptModalForSpace(space, tabId) },
    { label: "Update", handler: () => runUpdateAndShow(space.id) },
    { label: "Copy", handler: () => copySpace(space.id) },
    { label: "Copy to page", handler: () => copySpaceToTab(space.id) },
    { label: "Code", handler: () => showCodeModalForSpace(space.id, tabId) },
    { label: "Copy to clipboard", handler: () => copySpaceImageToClipboard(space) },
    { label: "Remove", handler: () => removeSpace(space.id) },
  ];

  if (backgroundPromptSpaces.has(space.id)) {
    actions.splice(2, 0, {
      label: "Prompt to foreground",
      handler: () => resumePromptSession(space.id),
    });
  }

  actions.forEach((action) => {
    const button = document.createElement("button");
    button.type = "button";
    button.textContent = action.label;
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      menu.classList.remove("open");
      action.handler();
    });
    menu.appendChild(button);
  });

  const versionRecords = Array.isArray(space.versions) ? [...space.versions].reverse() : [];
  if (versionRecords.length > 0) {
    const divider = document.createElement("div");
    divider.className = "space-menu-divider";
    menu.appendChild(divider);

    const versionsToggle = document.createElement("button");
    versionsToggle.type = "button";
    versionsToggle.className = "versions-toggle";
    versionsToggle.textContent = "Versions ▸";

    const versionsList = document.createElement("div");
    versionsList.className = "space-versions-list";
    versionRecords.forEach((version) => {
      if (!version || !version.id) {
        return;
      }
      const versionButton = document.createElement("button");
      versionButton.type = "button";
      versionButton.className = "space-version-entry";
      if (version.isActive) {
        versionButton.classList.add("active");
      }
      versionButton.textContent = version.label || version.createdAt || "Saved version";
      const promptSummary = version.prompt ? `Prompt: ${version.prompt}` : null;
      versionButton.title = promptSummary || `Saved ${version.label || version.createdAt || "version"}`;
      versionButton.addEventListener("click", (event) => {
        event.stopPropagation();
        versionsList.classList.remove("open");
        menu.classList.remove("open");
        selectSpaceVersion(space.id, version.id);
      });
      versionsList.appendChild(versionButton);
    });

    versionsToggle.addEventListener("click", (event) => {
      event.stopPropagation();
      const isOpen = versionsList.classList.contains("open");
      document.querySelectorAll(".space-versions-list.open").forEach((list) => list.classList.remove("open"));
      versionsList.classList.toggle("open", !isOpen);
    });

    menu.appendChild(versionsToggle);
    menu.appendChild(versionsList);
  }

  menuRoot.appendChild(menuButton);
  menuRoot.appendChild(menu);

  menuButton.addEventListener("click", (event) => {
    event.stopPropagation();
    const isOpen = menu.classList.contains("open");
    closeAllSpaceMenus();
    if (!isOpen) {
      elevateSpaceCard(space.id, card);
      menu.classList.add("open");
    }
  });

  const stage = document.createElement("div");
  stage.className = "space-stage";
  const view = document.createElement("div");
  view.className = "space-view";
  if (space.aspectRatio) {
    view.dataset.aspectRatio = String(space.aspectRatio);
  }
  view.addEventListener("pointerdown", (event) => beginSpaceResize(event, space, tabId, card, view));
  if (space.image_path) {
    const img = document.createElement("img");
    img.alt = `${space.title || "Space"} visualization`;
    const handleImageMetrics = () => {
      if (img.naturalWidth > 0 && img.naturalHeight > 0) {
        const ratio = img.naturalHeight / img.naturalWidth;
        setSpaceAspectRatio(space, view, ratio);
        enforceAspectRatioForCard(space, card, view, { spaceId: space.id, preferWidth: true, persist: true });
      }
    };
    img.addEventListener("load", handleImageMetrics);
    img.src = getSpaceImageUrl(space);
    if (img.complete) {
      handleImageMetrics();
    }
    view.appendChild(img);
  } else {
    const placeholder = document.createElement("div");
    placeholder.className = "space-placeholder";
    placeholder.innerHTML = "<p>No render yet</p>";
    view.appendChild(placeholder);
  }
  stage.appendChild(view);
  if (isPromptRunning) {
    const overlay = document.createElement("div");
    overlay.className = "space-overlay";
    overlay.textContent = isBackgroundPrompt ? "Prompt running (background)" : "Prompt running...";
    stage.appendChild(overlay);
  } else if (isPendingUpdate) {
    const overlay = document.createElement("div");
    overlay.className = "space-overlay";
    overlay.textContent = pendingMessage;
    stage.appendChild(overlay);
  }
  stage.appendChild(menuRoot);
  card.appendChild(stage);

  enforceAspectRatioForCard(space, card, view, { spaceId: space.id, preferWidth: true, persist: false });
  initialWidth = space.width;
  initialHeight = space.height;

  observeSpaceResize(space.id, view, { height: initialHeight, width: initialWidth }, space, card);

  return card;
}


function findSpaceRecord(spaceId, nodes = state.tree) {
  if (!spaceId || !Array.isArray(nodes)) {
    return null;
  }
  for (const node of nodes) {
    if (node.type === "tab" && Array.isArray(node.spaces)) {
      for (const space of node.spaces) {
        if (space.id === spaceId) {
          return { space, tab: node };
        }
      }
    }
    if (node.type === "folder" && Array.isArray(node.children)) {
      const nested = findSpaceRecord(spaceId, node.children);
      if (nested) {
        return nested;
      }
    }
  }
  return null;
}

function findNodeInTree(nodes, id) {
  if (!id) {
    return null;
  }
  for (const node of nodes) {
    if (node.id === id) {
      return node;
    }
    if (node.type === "folder" && node.children) {
      const match = findNodeInTree(node.children, id);
      if (match) {
        return match;
      }
    }
  }
  return null;
}

function setupEventListeners() {
  hamburgerButton.addEventListener("click", (event) => {
    event.stopPropagation();
    toggleHamburgerMenu();
  });

  if (openConfigurationBtn) {
    openConfigurationBtn.addEventListener("click", async () => {
      closeHamburgerMenu();
      await showConfigModal();
    });
  }

  if (resumePromptBtn) {
    resumePromptBtn.addEventListener("click", handleResumePromptClick);
  }

  if (closeConfigBtn) {
    closeConfigBtn.addEventListener("click", hideConfigModal);
  }
  if (cancelConfigBtn) {
    cancelConfigBtn.addEventListener("click", hideConfigModal);
  }
  if (testConfigBtn) {
    testConfigBtn.addEventListener("click", handleTestConnector);
  }
  if (configForm) {
    configForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (configStatus) {
        configStatus.textContent = "Saving configuration...";
      }
      const formData = new FormData(configForm);
      const payload = buildConnectorPayload(formData);
      try {
        const response = await fetch("/api/config", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        const result = await response.json();
        if (!response.ok) {
          throw new Error(result.error || "Unable to save configuration");
        }
        const saved = setConnectorSnapshot(result.connector || payload);
        if (configStatus) {
          const projectLabel = saved.projectKey || "project";
          configStatus.textContent = `Saved connector for ${projectLabel}`;
        }
        setTimeout(() => hideConfigModal(), 1200);
      } catch (error) {
        if (configStatus) {
          configStatus.textContent = error.message || "Unable to save configuration";
        }
      }
    });
  }

  promptForm.addEventListener("submit", handlePromptSubmit);
  closePromptBtn.addEventListener("click", hidePromptModal);
  cancelPromptBtn.addEventListener("click", hidePromptModal);

  codeForm.addEventListener("submit", handleCodeSubmit);
  closeCodeBtn.addEventListener("click", hideCodeModal);
  cancelCodeBtn.addEventListener("click", hideCodeModal);

  logCloseBtn.addEventListener("click", hideLogModal);
  logSecondaryBtn.addEventListener("click", hideLogModal);
  if (logBackgroundBtn) {
    logBackgroundBtn.addEventListener("click", handleRunPromptInBackground);
  }
  logPrimaryBtn.addEventListener("click", async () => {
    const action = logPrimaryAction;
    hideLogModal();
    if (typeof action === "function") {
      await action();
    }
  });
  if (logContent) {
    logContent.addEventListener("scroll", () => {
      positionLogScrollIndicator();
    });
  }

  addFolderBtn.addEventListener("click", () => handleCreate("folder"));
  addPageBtn.addEventListener("click", () => handleCreate("tab"));
  if (renameNodeBtn) {
    renameNodeBtn.addEventListener("click", handleRenameNode);
  }
  if (copyNodeBtn) {
    copyNodeBtn.addEventListener("click", handleCopyNode);
  }
  if (addSpaceBtn) {
    addSpaceBtn.addEventListener("click", handleAddSpaceRequest);
  }
  if (updateNodeBtn) {
    updateNodeBtn.addEventListener("click", () => {
      handleUpdateNode();
    });
  }
  removeNodeBtn.addEventListener("click", handleRemoveNode);

  document.addEventListener("click", (event) => {
    if (!event.target.closest(".context-menu")) {
      hideContextMenu();
    }
    if (!event.target.closest(".menu-root")) {
      closeHamburgerMenu();
    }
    if (!event.target.closest(".space-menu-root")) {
      closeAllSpaceMenus();
    }
    if (state.spaceMoveContext && !event.target.closest(".space-card")) {
      cancelSpaceMove();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      hideContextMenu();
      hideConfigModal();
      hidePromptModal();
      hideCodeModal();
      closeHamburgerMenu();
      closeAllSpaceMenus();
      cancelSpaceMove();
    }
  });

  document.addEventListener("contextmenu", (event) => {
    if (!event.target.closest(".tree-node")) {
      hideContextMenu();
    }
    if (!event.target.closest(".space-menu-root")) {
      closeAllSpaceMenus();
    }
  });

  const persistLayoutsNow = () => {
    flushPendingLayoutPersists({ immediate: true });
  };

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
      persistLayoutsNow();
    }
  });

  window.addEventListener("beforeunload", persistLayoutsNow);
  window.addEventListener("pagehide", persistLayoutsNow);

  treeRoot.addEventListener("click", (event) => {
    if (!event.target.closest(".tree-node")) {
      resetSelection();
    }
  });
}

setupEventListeners();
fetchConnectorConfig().catch(() => {});
loadTree();
