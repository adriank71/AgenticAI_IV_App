const categoryClassMap = {
  assistant: "assistant-event",
  transport: "transport-event",
  other: "other-event",
};

const categoryLabelMap = {
  assistant: "Assistant",
  transport: "Transport",
  other: "Other",
};

const categorySubtitleMap = {
  assistant: "Assistant support block",
  transport: "Transport reimbursement entry",
  other: "General appointment",
};

const assistantHourFieldLabels = {
  koerperpflege: "Koerperpflege",
  mahlzeiten_eingeben: "Mahlzeiten eingeben",
  mahlzeiten_zubereiten: "Mahlzeiten zubereiten",
  begleitung_therapie: "Begleitung Therapie",
};

const reportTypeLabelMap = {
  assistenzbeitrag: "Assistenzbeitraege report",
  transportkostenabrechnung: "Transportkostenabrechnung report",
};

const appViewTitleMap = {
  dashboard: "Dashboard",
  calendar: "Calendar",
  adviser: "IV Desk",
  community: "Community",
  reports: "Storage",
  automations: "Automations",
  settings: "Settings",
};

const communityChannelTitleMap = {
  daily: "Daily Support",
  reimbursement: "Reimbursements",
  therapy: "School & Therapy",
};

const communityStorageKey = "iv_helper_parent_community";
const chatSessionsStorageKey = "iv_agent_chat_sessions";
const activeChatStorageKey = "iv_agent_active_chat_id";
const chatAttachmentMaxFiles = 5;
const chatAttachmentMaxBytes = 10 * 1024 * 1024;
const chatAttachmentMimeTypes = new Set([
  "application/pdf",
  "text/plain",
  "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
]);

const communitySeedMessages = [
  {
    id: "seed-1",
    channel: "daily",
    author: "Mara",
    text: "We finally found a calm morning routine before therapy appointments. Preparing the bag the night before made the biggest difference.",
    timestamp: "2026-04-28T07:35:00.000Z",
  },
  {
    id: "seed-2",
    channel: "daily",
    author: "Jonas",
    text: "Today was heavy, but our assistant stayed fifteen minutes longer so I could finish the IV call without rushing.",
    timestamp: "2026-04-28T09:10:00.000Z",
  },
  {
    id: "seed-3",
    channel: "reimbursement",
    author: "Lea",
    text: "For transport receipts, I add the clinic address to the calendar note right away. It saves time when I prepare the monthly report.",
    timestamp: "2026-04-27T16:45:00.000Z",
  },
  {
    id: "seed-4",
    channel: "therapy",
    author: "Noah",
    text: "Our school meeting went better after we brought a one-page therapy summary. The teacher could scan it quickly before discussing support hours.",
    timestamp: "2026-04-27T13:20:00.000Z",
  },
];

const state = {
  currentMonth: formatMonth(new Date()),
  currentView: "timeGridWeek",
  activeAppView: "adviser",
  activeCommunityChannel: "daily",
  calendar: null,
  sidebarCollapsed: localStorage.getItem("iv_helper_sidebar_collapsed") === "true",
  inspectorCollapsed: true,
  lastPanelView: "calendar",
  threadId: localStorage.getItem("iv_agent_thread_id") || "",
  activeChatId: localStorage.getItem(activeChatStorageKey) || "",
  chats: [],
  openThreadMenuId: "",
  loadingCount: 0,
  errorTimer: null,
  activeModalId: null,
  lastFocusedElement: null,
  pendingDeleteId: null,
  pendingEventData: null,
  editingEventId: null,
  generatedReports: [],
  activeGeneratedReport: null,
  selectedReportTypes: ["assistenzbeitrag"],
  chatHistory: [],
  chatPending: false,
  chatAbortController: null,
  chatAttachments: [],
  chatAttachmentStatus: "",
  chatAttachmentStatusVariant: "",
  chatDragDepth: 0,
  chatQrLoading: false,
  chatVoiceRecorder: null,
  chatVoiceStream: null,
  chatVoiceChunks: [],
  chatVoiceProcessing: false,
  chatVoiceLiveRecognition: null,
  chatVoiceInterimTranscript: "",
  chatVoiceFinalTranscript: "",
  voiceRecorder: null,
  voiceStream: null,
  voiceChunks: [],
  voiceProcessing: false,
  voiceLiveRecognition: null,
  voiceInterimTranscript: "",
  voiceFinalTranscript: "",
  voiceContextTarget: "event",
  automations: [],
  automationsLoaded: false,
  automationDraftMode: "month_end_report",
  automationVoiceRecorder: null,
  automationVoiceStream: null,
  automationVoiceChunks: [],
  automationVoiceProcessing: false,
  automationLiveRecognition: null,
  automationInterimTranscript: "",
  automationFinalTranscript: "",
  communityMessages: [],
  aiStatus: null,
  profile: null,
  profileLoaded: false,
  calendarDataCache: {},
  calendarDataRequests: {},
  chatStarted: false,
  storageBrowser: null,
  activeStorageBucket: "",
};

const elements = {
  layoutShell: document.querySelector(".layout-shell"),
  sidebarToggle: document.getElementById("toggle-sidebar"),
  inspectorToggle: document.getElementById("toggle-inspector"),
  storageToggle: document.getElementById("toggle-storage"),
  automationsToggle: document.getElementById("toggle-automations"),
  monthPicker: document.getElementById("month-picker"),
  heading: document.getElementById("calendar-heading"),
  hoursValue: document.getElementById("hours-value"),
  capacityProgress: document.getElementById("capacity-progress"),
  loadingOverlay: document.getElementById("loading-overlay"),
  errorBanner: document.getElementById("error-banner"),
  addModal: document.getElementById("add-modal"),
  addModalTitle: document.getElementById("add-modal-title"),
  exportModal: document.getElementById("export-modal"),
  reportModal: document.getElementById("report-modal"),
  exportSummary: document.getElementById("export-summary"),
  reportForm: document.getElementById("report-form"),
  reportConfigPanel: document.getElementById("report-config-panel"),
  reportMonthInput: document.getElementById("report-month"),
  reportStatus: document.getElementById("report-status"),
  submitReport: document.getElementById("submit-report"),
  reportTypeButtons: document.querySelectorAll("[data-report-type]"),
  reportPreviewWrap: document.getElementById("report-preview-wrap"),
  reportPreviewFrame: document.getElementById("report-preview-frame"),
  reportResultsWrap: document.getElementById("report-results-wrap"),
  reportResultsList: document.getElementById("report-results-list"),
  reportResultsActions: document.getElementById("report-results-actions"),
  generateNewReport: document.getElementById("generate-new-report"),
  sendReport: document.getElementById("send-report"),
  addForm: document.getElementById("add-event-form"),
  submitAddEventButton: document.getElementById("submit-add-event"),
  categoryField: document.getElementById("event-category"),
  titleField: document.getElementById("event-title-field"),
  dateField: document.getElementById("event-date-field"),
  allDayField: document.getElementById("event-all-day-field"),
  notesField: document.getElementById("event-notes-field"),
  assistantFields: document.getElementById("assistant-hours-fields"),
  assistantHourInputs: document.querySelectorAll("[data-assistant-hours]"),
  transportFields: document.getElementById("transport-fields"),
  transportModeInput: document.getElementById("transport-mode"),
  transportKilometersInput: document.getElementById("transport-kilometers"),
  transportAddressInput: document.getElementById("transport-address"),
  transportModeButtons: document.querySelectorAll("[data-transport-mode]"),
  recurrenceField: document.getElementById("event-recurrence"),
  repeatCountField: document.getElementById("event-repeat-count"),
  dateInput: document.getElementById("event-date"),
  allDayInput: document.getElementById("event-all-day"),
  timeFields: document.getElementById("event-time-fields"),
  timeInput: document.getElementById("event-time"),
  endTimeInput: document.getElementById("event-end-time"),
  titleInput: document.getElementById("event-title"),
  notesInput: document.getElementById("event-notes"),
  deletePopover: document.getElementById("delete-popover"),
  deleteTitle: document.getElementById("delete-popover-title"),
  deleteMeta: document.getElementById("delete-popover-meta"),
  confirmDelete: document.getElementById("confirm-delete"),
  cancelDelete: document.getElementById("cancel-delete"),
  editEvent: document.getElementById("edit-event"),
  viewButtons: document.querySelectorAll(".view-button"),
  focusList: document.getElementById("focus-list"),
  focusCountPill: document.getElementById("focus-count-pill"),
  generateReport: document.getElementById("generate-report"),
  navLinks: document.querySelectorAll(".nav-link"),
  viewSections: document.querySelectorAll(".app-view"),
  activeViewTitle: document.getElementById("active-view-title"),
  calendarToolbar: document.getElementById("calendar-toolbar"),
  defaultToolbar: document.getElementById("default-toolbar"),
  dashboardHoursValue: document.getElementById("dashboard-hours-value"),
  dashboardEventsValue: document.getElementById("dashboard-events-value"),
  dashboardAssistantValue: document.getElementById("dashboard-assistant-value"),
  dashboardReportLabel: document.getElementById("dashboard-report-label"),
  dashboardOpenCalendar: document.getElementById("dashboard-open-calendar"),
  dashboardOpenChat: document.getElementById("dashboard-open-chat"),
  voiceComposer: document.getElementById("voice-composer"),
  voiceComposerButton: document.getElementById("voice-composer-button"),
  voiceComposerStatus: document.getElementById("voice-composer-status"),
  voiceComposerTranscript: document.getElementById("voice-composer-transcript"),
  voiceDraftPreview: document.getElementById("voice-draft-preview"),
  voiceDraftTranscript: document.getElementById("voice-draft-transcript"),
  voiceDraftStatus: document.getElementById("voice-draft-status"),
  automationsModal: document.getElementById("automations-modal"),
  openAutomationsCardButton: document.getElementById("open-automations-card"),
  quickAddAutomationButton: document.getElementById("quick-add-automation"),
  automationVoiceCard: document.querySelector(".automation-voice-card"),
  automationVoiceButton: document.getElementById("automation-voice-button"),
  automationVoiceStatus: document.getElementById("automation-voice-status"),
  automationVoiceTranscript: document.getElementById("automation-voice-transcript"),
  automationForm: document.getElementById("automation-form"),
  automationTitleInput: document.getElementById("automation-title"),
  automationActionInput: document.getElementById("automation-action"),
  automationScheduleInput: document.getElementById("automation-schedule"),
  automationDateRow: document.getElementById("automation-date-row"),
  automationDateInput: document.getElementById("automation-date"),
  automationTimeInput: document.getElementById("automation-time"),
  automationNoteInput: document.getElementById("automation-note"),
  automationPresetButtons: document.querySelectorAll("[data-preset]"),
  automationList: document.getElementById("automation-list"),
  automationCountPill: document.getElementById("automation-count"),
  automationSummary: document.getElementById("automation-summary"),
  automationPanelList: document.getElementById("automation-panel-list"),
  automationPanelCount: document.getElementById("automation-panel-count"),
  automationPanelSummary: document.getElementById("automation-panel-summary"),
  panelNewAutomationButton: document.getElementById("panel-new-automation"),
  adviserForm: document.getElementById("adviser-form"),
  adviserShell: document.getElementById("adviser-shell"),
  chatWelcome: document.getElementById("chat-welcome"),
  adviserInput: document.getElementById("adviser-input"),
  adviserSendButton: document.getElementById("adviser-send"),
  adviserCancelButton: document.getElementById("adviser-cancel"),
  chatAttachButton: document.getElementById("chat-attach-button"),
  chatQrButton: document.getElementById("chat-qr-button"),
  chatVoiceButton: document.getElementById("chat-voice-button"),
  chatVoiceStatusRow: document.getElementById("chat-voice-status-row"),
  chatVoiceStatus: document.getElementById("chat-voice-status"),
  chatVoiceTranscript: document.getElementById("chat-voice-transcript"),
  chatFileInput: document.getElementById("chat-file-input"),
  chatAttachmentTray: document.getElementById("chat-attachment-tray"),
  chatAttachmentList: document.getElementById("chat-attachment-list"),
  chatAttachmentStatus: document.getElementById("chat-attachment-status"),
  chatQrModal: document.getElementById("chat-qr-modal"),
  chatQrImage: document.getElementById("chat-invoices-qr"),
  chatCameraLink: document.getElementById("chat-invoices-camera-link"),
  chatQrStatus: document.getElementById("chat-qr-status"),
  chatThread: document.getElementById("chat-thread"),
  chatChips: document.querySelectorAll(".chat-chip"),
  newThreadButton: document.getElementById("new-thread"),
  chatList: document.getElementById("chat-list"),
  communityThread: document.getElementById("community-thread"),
  communityForm: document.getElementById("community-form"),
  communityInput: document.getElementById("community-input"),
  communityNameInput: document.getElementById("community-name"),
  communitySendButton: document.getElementById("community-send"),
  communityChannelButtons: document.querySelectorAll("[data-community-channel]"),
  communityTopicTitle: document.getElementById("community-topic-title"),
  communityCount: document.getElementById("community-count"),
  settingsButton: document.getElementById("open-settings-view"),
  profileForm: document.getElementById("profile-form"),
  profileInputs: document.querySelectorAll("[data-profile-path]"),
  profileSaveStatus: document.getElementById("profile-save-status"),
  reloadProfileButton: document.getElementById("reload-profile"),
  refreshStorageBrowserButton: document.getElementById("refresh-storage-browser"),
  storageBrowserStatus: document.getElementById("storage-browser-status"),
  storageBucketList: document.getElementById("storage-bucket-list"),
  storageActiveBucket: document.getElementById("storage-active-bucket"),
  storageFileCount: document.getElementById("storage-file-count"),
  storageFileList: document.getElementById("storage-file-list"),
};

function formatMonth(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  return `${year}-${month}`;
}

function formatIsoDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatMonthHeading(month) {
  const [year, monthIndex] = month.split("-").map(Number);
  return new Date(year, monthIndex - 1, 1).toLocaleDateString(undefined, {
    month: "long",
    year: "numeric",
  });
}

function formatHeadingForView(viewType, startDate, endDate) {
  if (viewType === "timeGridDay") {
    return startDate.toLocaleDateString(undefined, {
      weekday: "long",
      month: "long",
      day: "numeric",
      year: "numeric",
    });
  }

  if (viewType === "timeGridWeek" || viewType === "listWeek") {
    const inclusiveEnd = new Date(endDate.getTime() - 1000);
    const startLabel = startDate.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
    });
    const endLabel = inclusiveEnd.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
      year: "numeric",
    });
    return `${startLabel} - ${endLabel}`;
  }

  return startDate.toLocaleDateString(undefined, {
    month: "long",
    year: "numeric",
  });
}

function formatClock(timeString) {
  const [hourPart, minutePart] = (timeString || "00:00").split(":");
  const date = new Date();
  date.setHours(Number(hourPart), Number(minutePart), 0, 0);
  return date.toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatTimeRange(startTime, endTime) {
  if (!startTime) {
    return "";
  }
  if (!endTime) {
    return formatClock(startTime);
  }
  return `${formatClock(startTime)} - ${formatClock(endTime)}`;
}

function addMinutesToTime(timeString, minutesToAdd) {
  const [hourPart, minutePart] = (timeString || "00:00").split(":");
  const date = new Date();
  date.setHours(Number(hourPart), Number(minutePart), 0, 0);
  date.setMinutes(date.getMinutes() + minutesToAdd);
  return `${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function addDaysToIsoDate(dateString, daysToAdd) {
  const [year, month, day] = dateString.split("-").map(Number);
  const date = new Date(year, month - 1, day);
  date.setDate(date.getDate() + daysToAdd);
  return formatIsoDate(date);
}

function syncEndTimeWithStart(forceUpdate = false) {
  if (!elements.timeInput || !elements.endTimeInput || !elements.timeInput.value) {
    return;
  }

  if (forceUpdate || !elements.endTimeInput.value || elements.endTimeInput.value <= elements.timeInput.value) {
    elements.endTimeInput.value = addMinutesToTime(elements.timeInput.value, 30);
  }
}

function toggleAllDayFields(allowEmptyTime = false) {
  const isAllDay = Boolean(elements.allDayInput && elements.allDayInput.checked);
  if (elements.timeFields) {
    elements.timeFields.classList.toggle("hidden-field", isAllDay);
  }
  if (elements.timeInput) {
    elements.timeInput.disabled = isAllDay;
    if (isAllDay) {
      elements.timeInput.value = "";
    } else if (!elements.timeInput.value && !allowEmptyTime) {
      elements.timeInput.value = "09:00";
    }
  }
  if (elements.endTimeInput) {
    elements.endTimeInput.disabled = isAllDay;
    if (isAllDay) {
      elements.endTimeInput.value = "";
    } else if (!allowEmptyTime || elements.timeInput.value) {
      syncEndTimeWithStart(true);
    }
  }
}

function formatMonthOptionLabel(monthValue) {
  const [year, month] = monthValue.split("-").map(Number);
  return new Date(year, month - 1, 1).toLocaleDateString(undefined, {
    month: "long",
    year: "numeric",
  });
}

function formatHours(value) {
  return Number(value || 0).toFixed(2);
}

function getAssistantHoursPayload() {
  const payload = {};
  elements.assistantHourInputs.forEach((input) => {
    payload[input.dataset.assistantHours] = parseFloat(input.value || "0");
  });
  return payload;
}

function sumAssistantHours(assistantHours) {
  return Object.values(assistantHours || {}).reduce((total, value) => total + Number(value || 0), 0);
}

function formatAssistantSubtitle(event) {
  const assistantHours = event.assistant_hours || {};
  const parts = Object.entries(assistantHourFieldLabels)
    .map(([field, label]) => {
      const value = Number(assistantHours[field] || 0);
      return value > 0 ? `${label}: ${formatHours(value)} h` : "";
    })
    .filter(Boolean);

  if (parts.length) {
    return parts.join(" | ");
  }

  const total = Number(event.hours || 0);
  return total > 0 ? `Assistant total: ${formatHours(total)} h` : categorySubtitleMap.assistant;
}

function formatEventSubtitle(event) {
  if (event.notes) {
    return event.notes;
  }
  if (event.category === "assistant") {
    return formatAssistantSubtitle(event);
  }
  if (event.category === "transport") {
    const parts = [];
    if (event.transport_mode) {
      parts.push(
        {
          bus_bahn: "Bus / Bahn",
          privatauto: "Privatauto",
          taxi: "Taxi",
          fahrdienst: "Fahrdienst",
        }[event.transport_mode] || event.transport_mode
      );
    }
    if (Number(event.transport_kilometers || 0) > 0) {
      parts.push(`${formatHours(event.transport_kilometers)} km`);
    }
    if (event.transport_address) {
      parts.push(event.transport_address);
    }
    if (parts.length) {
      return parts.join(" | ");
    }
  }
  return categorySubtitleMap[event.category] || categorySubtitleMap.other;
}

function getSelectedReportTypes() {
  return [...state.selectedReportTypes];
}

function setSelectedReportTypes(reportTypes) {
  state.selectedReportTypes = [...new Set(reportTypes)];
  elements.reportTypeButtons.forEach((button) => {
    const isSelected = state.selectedReportTypes.includes(button.dataset.reportType);
    button.classList.toggle("is-active", isSelected);
    button.setAttribute("aria-pressed", String(isSelected));
  });
}

function addMonths(monthValue, offset) {
  const [year, month] = monthValue.split("-").map(Number);
  const date = new Date(year, month - 1 + offset, 1);
  return formatMonth(date);
}

function populateReportMonthOptions(anchorMonth) {
  const selected = anchorMonth || state.currentMonth;
  elements.reportMonthInput.innerHTML = "";

  for (let offset = -12; offset <= 12; offset += 1) {
    const value = addMonths(selected, offset);
    const option = document.createElement("option");
    option.value = value;
    option.textContent = formatMonthOptionLabel(value);
    elements.reportMonthInput.appendChild(option);
  }

  elements.reportMonthInput.value = selected;
}

function showLoading() {
  state.loadingCount += 1;
  elements.loadingOverlay.classList.remove("hidden");
}

function hideLoading() {
  state.loadingCount = Math.max(0, state.loadingCount - 1);
  if (state.loadingCount === 0) {
    elements.loadingOverlay.classList.add("hidden");
  }
}

async function apiFetch(url, options = {}) {
  const {
    showLoading: shouldShowLoading = true,
    suppressErrorBanner = false,
    ...fetchOptions
  } = options;

  if (shouldShowLoading) {
    showLoading();
  }

  try {
    const response = await fetch(url, {
      headers: {
        "Content-Type": "application/json",
        ...(fetchOptions.headers || {}),
      },
      ...fetchOptions,
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.error || "Request failed");
    }
    return data;
  } catch (error) {
    if (!suppressErrorBanner && error.name !== "AbortError") {
      showError(error.message || "Request failed");
    }
    throw error;
  } finally {
    if (shouldShowLoading) {
      hideLoading();
    }
  }
}

async function readResponseError(response, fallbackMessage) {
  let rawText = "";
  try {
    rawText = await response.text();
  } catch (error) {
    rawText = "";
  }

  if (rawText) {
    try {
      const payload = JSON.parse(rawText);
      if (payload && payload.error) {
        return String(payload.error);
      }
    } catch (error) {
      const normalized = rawText.replace(/\s+/g, " ").trim();
      if (normalized) {
        return `${fallbackMessage} (${response.status}): ${normalized.slice(0, 180)}`;
      }
    }
  }

  if (response.status === 404) {
    return "Voice transcription route not available. Restart the server.";
  }
  return `${fallbackMessage} (${response.status || "network"})`;
}

function showError(message) {
  clearTimeout(state.errorTimer);
  elements.errorBanner.textContent = message;
  elements.errorBanner.classList.remove("hidden");
  state.errorTimer = window.setTimeout(() => {
    elements.errorBanner.classList.add("hidden");
  }, 4000);
}

function getNestedValue(source, path) {
  return path.split(".").reduce((value, key) => {
    if (!value || typeof value !== "object") {
      return undefined;
    }
    return value[key];
  }, source);
}

function setNestedValue(target, path, value) {
  const parts = path.split(".");
  let cursor = target;
  parts.slice(0, -1).forEach((key) => {
    if (!cursor[key] || typeof cursor[key] !== "object" || Array.isArray(cursor[key])) {
      cursor[key] = {};
    }
    cursor = cursor[key];
  });
  cursor[parts[parts.length - 1]] = value;
}

function syncProfileStatus(message, variant = "") {
  if (!elements.profileSaveStatus) {
    return;
  }
  elements.profileSaveStatus.textContent = message;
  elements.profileSaveStatus.dataset.variant = variant;
}

function populateProfileForm(profile) {
  elements.profileInputs.forEach((input) => {
    const value = getNestedValue(profile, input.dataset.profilePath || "");
    if (input.type === "checkbox") {
      input.checked = Boolean(value);
    } else {
      input.value = value == null ? "" : String(value);
    }
  });
}

function collectProfileForm() {
  const profile = JSON.parse(JSON.stringify(state.profile || {}));
  elements.profileInputs.forEach((input) => {
    const path = input.dataset.profilePath || "";
    if (!path) {
      return;
    }
    setNestedValue(profile, path, input.type === "checkbox" ? input.checked : input.value.trim());
  });
  return profile;
}

async function loadProfileForm({ force = false } = {}) {
  if (state.profileLoaded && !force) {
    return;
  }
  syncProfileStatus("Loading");
  const payload = await apiFetch("/api/profile?profile_id=default");
  state.profile = payload.profile || {};
  state.profileLoaded = true;
  populateProfileForm(state.profile);
  syncProfileStatus("Loaded", "success");
}

async function submitProfileForm(event) {
  event.preventDefault();
  const nextProfile = collectProfileForm();
  syncProfileStatus("Saving");
  const payload = await apiFetch("/api/profile?profile_id=default", {
    method: "PUT",
    body: JSON.stringify(nextProfile),
  });
  state.profile = payload.profile || nextProfile;
  state.profileLoaded = true;
  populateProfileForm(state.profile);
  syncProfileStatus("Saved", "success");
}

function getOpenAiStatus() {
  return state.aiStatus && state.aiStatus.openai ? state.aiStatus.openai : null;
}

function isOpenAiConfigured() {
  const status = getOpenAiStatus();
  return !status || status.configured !== false;
}

function openAiUnavailableMessage() {
  return "AI is not configured on this server. Add OPENAI_API_KEY in Vercel and redeploy.";
}

function applyAiStatus() {
  const configured = isOpenAiConfigured();
  const message = openAiUnavailableMessage();

  [elements.voiceComposerButton, elements.automationVoiceButton, elements.chatVoiceButton].forEach((button) => {
    if (!button) {
      return;
    }
    button.disabled = !configured || (button === elements.chatVoiceButton && (state.chatPending || state.chatVoiceProcessing));
    button.title = configured ? button.getAttribute("aria-label") || "" : message;
  });

  if (!configured) {
    if (elements.voiceComposerStatus) {
      elements.voiceComposerStatus.textContent = message;
    }
    if (elements.automationVoiceStatus) {
      elements.automationVoiceStatus.textContent = message;
    }
    if (elements.chatVoiceStatus) {
      elements.chatVoiceStatus.textContent = message;
    }
    syncChatVoiceStatusRow();
  }
}

async function refreshAiStatus() {
  try {
    state.aiStatus = await apiFetch("/api/ai/status", {
      showLoading: false,
      suppressErrorBanner: true,
    });
  } catch (error) {
    state.aiStatus = null;
  }
  applyAiStatus();
}

function updateViewButtons() {
  elements.viewButtons.forEach((button) => {
    const isActive = button.dataset.view === state.currentView;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  });
}

function syncMonthUi() {
  elements.monthPicker.value = state.currentMonth;

  if (!state.calendar) {
    elements.heading.textContent = formatMonthHeading(state.currentMonth);
    return;
  }

  const currentView = state.calendar.view;
  elements.heading.textContent = formatHeadingForView(
    currentView.type,
    currentView.currentStart,
    currentView.currentEnd
  );
  updateViewButtons();
}

async function refreshHours() {
  if (!elements.hoursValue) {
    return;
  }
  const data = await getCalendarMonthData(state.currentMonth);
  const totalHours = Number(data.total_hours || 0);
  elements.hoursValue.textContent = totalHours.toFixed(2);
  if (elements.capacityProgress) {
    elements.capacityProgress.style.width = `${Math.max(0, Math.min(100, (totalHours / 60) * 100))}%`;
  }
}

function renderFocusSummary(events) {
  const today = formatIsoDate(new Date());
  const todayEvents = events
    .filter((event) => event.date === today)
    .sort((left, right) => left.time.localeCompare(right.time));

  const taskText = `${todayEvents.length} ${todayEvents.length === 1 ? "task" : "tasks"}`;
  if (elements.focusCountPill) {
    elements.focusCountPill.textContent = todayEvents.length >= 3 ? `${taskText} | High attention` : taskText;
  }

  elements.focusList.innerHTML = "";
  if (!todayEvents.length) {
    const empty = document.createElement("p");
    empty.className = "muted-copy";
    empty.textContent = "No appointments for today.";
    elements.focusList.appendChild(empty);
    return;
  }

  todayEvents.forEach((event, index) => {
    const item = document.createElement("article");
    item.className = `focus-item ${event.category || "other"}`;

    const topRow = document.createElement("div");
    topRow.className = "focus-item-head";

    const timeLabel = document.createElement("span");
    timeLabel.className = "focus-time";
    timeLabel.textContent = formatClock(event.time);

    const badge = document.createElement("span");
    badge.className = "status-pill ghost";
    badge.textContent = categoryLabelMap[event.category] || categoryLabelMap.other;

    topRow.appendChild(timeLabel);
    topRow.appendChild(badge);

    const title = document.createElement("h3");
    title.className = "focus-title";
    title.textContent = event.title;

    const subtitle = document.createElement("p");
    subtitle.className = "focus-subtitle";
    subtitle.textContent = formatEventSubtitle(event);

    item.appendChild(topRow);
    item.appendChild(title);
    item.appendChild(subtitle);

    if (index === 0 && todayEvents.length >= 3) {
      const attention = document.createElement("span");
      attention.className = "status-pill high";
      attention.textContent = "Priority";
      item.appendChild(attention);
    }

    elements.focusList.appendChild(item);
  });
}

async function refreshFocusSummary() {
  const todayMonth = formatMonth(new Date());
  const data = await getCalendarMonthData(todayMonth);
  renderFocusSummary(data.events || []);
}

async function refreshSidebarData() {
  syncMonthUi();
  await Promise.all([refreshHours(), refreshFocusSummary()]);
}

async function refreshDashboardData() {
  const data = await getCalendarMonthData(state.currentMonth);
  const monthEvents = data.events || [];
  const assistantEvents = monthEvents.filter((event) => event.category === "assistant");

  if (elements.dashboardHoursValue) {
    elements.dashboardHoursValue.textContent = Number(data.total_hours || 0).toFixed(2);
  }
  if (elements.dashboardEventsValue) {
    elements.dashboardEventsValue.textContent = String(monthEvents.length);
  }
  if (elements.dashboardAssistantValue) {
    elements.dashboardAssistantValue.textContent = String(assistantEvents.length);
  }
  if (elements.dashboardReportLabel) {
    elements.dashboardReportLabel.textContent = `Generate monthly report for ${formatMonthHeading(state.currentMonth)}`;
  }
}

async function getCalendarMonthData(month, options = {}) {
  const force = Boolean(options.force);
  const profileId = getActiveProfileId();
  const cacheKey = `${profileId}:${month}`;
  if (!force && state.calendarDataCache[cacheKey]) {
    return state.calendarDataCache[cacheKey];
  }
  if (!force && state.calendarDataRequests[cacheKey]) {
    return state.calendarDataRequests[cacheKey];
  }

  const request = apiFetch(`/api/calendar-data?month=${encodeURIComponent(month)}&profile_id=${encodeURIComponent(profileId)}`, {
    showLoading: false,
  })
    .then((data) => {
      state.calendarDataCache[cacheKey] = data;

      if (Array.isArray(data.reminders)) {
        state.automations = data.reminders;
        state.automationsLoaded = true;
        renderAutomations();
      }

      return data;
    })
    .finally(() => {
      delete state.calendarDataRequests[cacheKey];
    });

  state.calendarDataRequests[cacheKey] = request;
  return request;
}

function getActiveProfileId() {
  return "default";
}

function getBrowserTimezone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "Europe/Berlin";
  } catch (error) {
    return "Europe/Berlin";
  }
}

function clearCalendarDataCache() {
  state.calendarDataCache = {};
  state.calendarDataRequests = {};
}

function getMonthsInRange(startDate, endDateExclusive) {
  const monthKeys = [];
  const cursor = new Date(startDate.getFullYear(), startDate.getMonth(), 1);
  const inclusiveEnd = new Date(endDateExclusive.getTime() - 1000);
  const endCursor = new Date(inclusiveEnd.getFullYear(), inclusiveEnd.getMonth(), 1);

  while (cursor <= endCursor) {
    monthKeys.push(formatMonth(cursor));
    cursor.setMonth(cursor.getMonth() + 1);
  }

  return monthKeys;
}

function buildCalendarEvent(event) {
  if (event.all_day) {
    return {
      id: event.id,
      title: event.title,
      start: event.date,
      end: addDaysToIsoDate(event.date, 1),
      allDay: true,
      classNames: [categoryClassMap[event.category] || categoryClassMap.other],
      extendedProps: {
        rawEvent: event,
      },
    };
  }

  return {
    id: event.id,
    title: event.title,
    start: `${event.date}T${event.time}:00`,
    end: `${event.date}T${(event.end_time || addMinutesToTime(event.time, 30))}:00`,
    classNames: [categoryClassMap[event.category] || categoryClassMap.other],
    extendedProps: {
      rawEvent: event,
    },
  };
}

function renderEventContent(arg) {
  const rawEvent = arg.event.extendedProps.rawEvent || {};

  if (rawEvent.all_day) {
    const pill = document.createElement("div");
    pill.className = "all-day-event-pill";
    pill.textContent = rawEvent.title || arg.event.title;
    return { domNodes: [pill] };
  }

  const wrapper = document.createElement("article");
  wrapper.className = `event-card ${rawEvent.category || "other"}`;

  const top = document.createElement("div");
  top.className = "event-card-top";

  const time = document.createElement("span");
  time.className = "event-time";
  time.textContent = rawEvent.all_day
    ? "All day"
    : (rawEvent.time ? formatTimeRange(rawEvent.time, rawEvent.end_time) : arg.timeText);

  const category = document.createElement("span");
  category.className = "event-badge";
  category.textContent = categoryLabelMap[rawEvent.category] || categoryLabelMap.other;

  top.appendChild(time);
  top.appendChild(category);

  const title = document.createElement("h4");
  title.className = "event-title";
  title.textContent = rawEvent.title || arg.event.title;

  const subtitle = document.createElement("p");
  subtitle.className = "event-subtitle";
  subtitle.textContent = formatEventSubtitle(rawEvent);

  wrapper.appendChild(top);
  wrapper.appendChild(title);
  wrapper.appendChild(subtitle);

  return { domNodes: [wrapper] };
}

async function fetchEvents(info, successCallback, failureCallback) {
  try {
    const months = getMonthsInRange(info.start, info.end);
    const responses = await Promise.all(
      months.map((month) => getCalendarMonthData(month))
    );

    const rawEvents = [];
    const seenIds = new Set();

    responses.forEach((response) => {
      (response.events || []).forEach((event) => {
        if (!seenIds.has(event.id)) {
          seenIds.add(event.id);
          rawEvents.push(event);
        }
      });
    });

    successCallback(rawEvents.map(buildCalendarEvent));
  } catch (error) {
    failureCallback(error);
  }
}

async function refreshCalendarData(options = {}) {
  if (options.force) {
    clearCalendarDataCache();
  }
  syncMonthUi();
  closeDeletePopover();
  if (state.calendar) {
    state.calendar.refetchEvents();
  }
  await Promise.all([refreshSidebarData(), refreshDashboardData()]);
}

function ensureCalendarInitialized() {
  if (state.calendar) {
    return;
  }
  initCalendar();
}

function syncWorkspaceLayout() {
  const panelOpen = state.activeAppView !== "adviser" && !state.inspectorCollapsed;
  document.body.classList.toggle("sidebar-collapsed", state.sidebarCollapsed);
  document.body.classList.toggle("workspace-panel-open", panelOpen);
  document.body.classList.toggle("workspace-panel-closed", !panelOpen);

  if (elements.sidebarToggle) {
    const label = state.sidebarCollapsed ? "Open sidebar" : "Collapse sidebar";
    elements.sidebarToggle.setAttribute("aria-expanded", String(!state.sidebarCollapsed));
    elements.sidebarToggle.setAttribute("aria-label", label);
    elements.sidebarToggle.setAttribute("title", label);
    const icon = elements.sidebarToggle.querySelector(".material-symbols-outlined");
    if (icon) {
      icon.textContent = state.sidebarCollapsed ? "left_panel_open" : "left_panel_close";
    }
  }

  if (elements.inspectorToggle) {
    const calendarOpen = panelOpen && state.activeAppView === "calendar";
    elements.inspectorToggle.setAttribute("aria-expanded", String(calendarOpen));
    elements.inspectorToggle.classList.toggle("is-active", calendarOpen);
  }

  if (elements.storageToggle) {
    const storageOpen = panelOpen && state.activeAppView === "reports";
    elements.storageToggle.setAttribute("aria-expanded", String(storageOpen));
    elements.storageToggle.classList.toggle("is-active", storageOpen);
  }

  if (elements.automationsToggle) {
    const automationsOpen = panelOpen && state.activeAppView === "automations";
    elements.automationsToggle.setAttribute("aria-expanded", String(automationsOpen));
    elements.automationsToggle.classList.toggle("is-active", automationsOpen);
  }

  if (elements.calendarToolbar) {
    elements.calendarToolbar.classList.toggle("hidden", state.activeAppView !== "calendar" || !panelOpen);
  }
  if (elements.defaultToolbar) {
    elements.defaultToolbar.classList.remove("hidden");
  }
}

function toggleSidebar() {
  state.sidebarCollapsed = !state.sidebarCollapsed;
  localStorage.setItem("iv_helper_sidebar_collapsed", String(state.sidebarCollapsed));
  syncWorkspaceLayout();
}

function toggleInspector() {
  toggleWorkspacePanel("calendar");
}

function toggleWorkspacePanel(viewName) {
  const isOpen = state.activeAppView === viewName && !state.inspectorCollapsed;
  if (isOpen) {
    state.inspectorCollapsed = true;
    syncWorkspaceLayout();
    return;
  }
  switchAppView(viewName).catch(() => {});
}

function navigatePeriod(offset) {
  if (!state.calendar) {
    return;
  }
  closeDeletePopover();
  if (offset < 0) {
    state.calendar.prev();
  } else {
    state.calendar.next();
  }
  state.currentMonth = formatMonth(state.calendar.getDate());
  refreshCalendarData().catch(() => {});
}

function changeCalendarView(viewName) {
  if (!state.calendar) {
    return;
  }
  state.currentView = viewName;
  state.calendar.changeView(viewName);
  refreshSidebarData().catch(() => {});
}

async function switchAppView(viewName) {
  const panelViews = new Set(["calendar", "reports", "automations", "settings"]);
  const isPanelView = panelViews.has(viewName);
  state.activeAppView = viewName;
  if (isPanelView) {
    state.lastPanelView = viewName;
    state.inspectorCollapsed = false;
  } else {
    state.inspectorCollapsed = true;
  }

  elements.navLinks.forEach((button) => {
    const isActive = button.dataset.viewTarget === viewName;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-current", isActive ? "page" : "false");
  });
  if (elements.settingsButton) {
    const isSettings = viewName === "settings";
    elements.settingsButton.classList.toggle("is-active", isSettings);
    elements.settingsButton.setAttribute("aria-current", isSettings ? "page" : "false");
  }

  elements.viewSections.forEach((section) => {
    const isChatBase = section.dataset.view === "adviser" && (viewName === "adviser" || isPanelView);
    const isTargetView = section.dataset.view === viewName;
    const isTargetPanel = isPanelView && isTargetView;
    section.classList.toggle("is-active", isChatBase || isTargetView);
    section.classList.toggle("is-panel-active", isTargetPanel);
  });
  syncWorkspaceLayout();

  if (elements.activeViewTitle) {
    elements.activeViewTitle.textContent = appViewTitleMap[viewName] || "IV Desk";
  }
  if (elements.calendarToolbar) {
    elements.calendarToolbar.classList.toggle("hidden", viewName !== "calendar" || state.inspectorCollapsed);
  }
  if (elements.defaultToolbar) {
    elements.defaultToolbar.classList.remove("hidden");
  }

  if (viewName === "calendar") {
    ensureCalendarInitialized();
    state.calendar.updateSize();
    await refreshCalendarData();
    return;
  }

  if (viewName === "dashboard") {
    await refreshDashboardData();
    return;
  }

  if (viewName === "automations") {
    await refreshAutomations();
    return;
  }

  if (viewName === "adviser" && elements.adviserInput) {
    elements.adviserInput.focus();
  }

  if (viewName === "community") {
    renderCommunityMessages();
    if (elements.communityInput) {
      elements.communityInput.focus();
    }
  }

  if (viewName === "settings") {
    await loadProfileForm();
  }
}

function openModal(modalId, triggerElement) {
  state.lastFocusedElement = triggerElement || document.activeElement;
  state.activeModalId = modalId;
  const modal = document.getElementById(modalId);
  if (!modal) {
    return;
  }
  modal.classList.remove("hidden");
  const firstFocusable = modal.querySelector("input, select, textarea, button");
  if (firstFocusable) {
    firstFocusable.focus();
  }
}

function closeModal(modalId) {
  const modal = document.getElementById(modalId);
  if (!modal) {
    return;
  }
  modal.classList.add("hidden");
  if (modalId === "add-modal") {
    resetEventFormMode();
  }
  if (state.activeModalId === modalId) {
    state.activeModalId = null;
  }
  if (state.lastFocusedElement) {
    state.lastFocusedElement.focus();
  }
}

function toggleAssistantFields() {
  const isAssistant = elements.categoryField.value === "assistant";
  const isTransport = elements.categoryField.value === "transport";

  if (elements.addModal) {
    elements.addModal.classList.toggle("transport-modal-active", isTransport);
  }
  elements.assistantFields.classList.toggle("hidden-field", !isAssistant);
  elements.assistantHourInputs.forEach((input) => {
    input.disabled = !isAssistant;
  });
  if (!isAssistant) {
    elements.assistantHourInputs.forEach((input) => {
      input.value = "0.0";
    });
  }

  if (elements.transportFields) {
    elements.transportFields.classList.toggle("hidden-field", !isTransport);
  }
  if (elements.transportModeInput) {
    elements.transportModeInput.disabled = !isTransport;
  }
  if (elements.transportKilometersInput) {
    elements.transportKilometersInput.disabled = !isTransport;
    if (!isTransport) {
      elements.transportKilometersInput.value = "0.0";
    }
  }
  if (elements.transportAddressInput) {
    elements.transportAddressInput.disabled = !isTransport;
    if (!isTransport) {
      elements.transportAddressInput.value = "";
    }
  }
  if (!isTransport && elements.transportModeInput) {
    elements.transportModeInput.value = "bus_bahn";
  }
  updateTransportModeButtons();
}

function updateTransportModeButtons() {
  if (!elements.transportModeButtons) {
    return;
  }

  elements.transportModeButtons.forEach((button) => {
    const isSelected = button.dataset.transportMode === elements.transportModeInput.value;
    button.classList.toggle("is-active", isSelected);
    button.setAttribute("aria-pressed", String(isSelected));
  });
}

function toggleRepeatCountField() {
  const hasRecurrence = elements.recurrenceField.value !== "none";
  elements.repeatCountField.disabled = !hasRecurrence;
  if (!hasRecurrence) {
    elements.repeatCountField.value = "0";
  }
}

function validateForm(formData) {
  if (!formData.date) {
    return "Date is required";
  }
  if (!formData.all_day && !formData.time) {
    return "Start time is required";
  }
  if (!formData.all_day && !formData.end_time) {
    return "End time is required";
  }
  if (!formData.all_day && formData.end_time <= formData.time) {
    return "End time must be later than start time";
  }
  if (!formData.title) {
    return "Title is required";
  }
  if (formData.repeat_count < 0 || Number.isNaN(formData.repeat_count)) {
    return "Repetitions must be greater than or equal to 0";
  }

  for (const [fieldName, value] of Object.entries(formData.assistant_hours || {})) {
    if (Number.isNaN(value) || value < 0) {
      return `${assistantHourFieldLabels[fieldName] || fieldName} must be greater than or equal to 0`;
    }
  }
  if (Number.isNaN(formData.transport_kilometers) || formData.transport_kilometers < 0) {
    return "Kilometers must be greater than or equal to 0";
  }
  return "";
}

function resetEventFormMode() {
  state.editingEventId = null;
  if (elements.addModalTitle) {
    elements.addModalTitle.textContent = "Add Event";
  }
  if (elements.submitAddEventButton) {
    elements.submitAddEventButton.textContent = "Add";
  }
  elements.recurrenceField.disabled = false;
  elements.repeatCountField.disabled = false;
  if (elements.allDayInput) {
    elements.allDayInput.checked = false;
  }
  if (elements.transportModeInput) {
    elements.transportModeInput.value = "bus_bahn";
  }
  if (elements.transportKilometersInput) {
    elements.transportKilometersInput.value = "0.0";
  }
  if (elements.transportAddressInput) {
    elements.transportAddressInput.value = "";
  }
  if (elements.voiceDraftPreview) {
    elements.voiceDraftPreview.classList.add("hidden");
  }
  if (elements.voiceDraftTranscript) {
    elements.voiceDraftTranscript.textContent = "";
  }
  if (elements.voiceDraftStatus) {
    elements.voiceDraftStatus.textContent = "";
  }
  resetVoiceTranscriptBuffers("event");
  setVoiceComposerState("event", "idle");
  toggleAllDayFields();
  toggleAssistantFields();
}

function populateEventForm(eventData, options = {}) {
  const preserveEmptyFields = Boolean(options.preserveEmptyFields);
  elements.dateInput.value = eventData.date || (preserveEmptyFields ? "" : `${state.currentMonth}-01`);
  if (elements.allDayInput) {
    elements.allDayInput.checked = Boolean(eventData.all_day);
  }
  elements.timeInput.value = eventData.time || (preserveEmptyFields ? "" : "09:00");
  elements.endTimeInput.value = eventData.end_time || (elements.timeInput.value ? addMinutesToTime(elements.timeInput.value, 30) : "");
  elements.categoryField.value = eventData.category || "assistant";
  elements.titleInput.value = eventData.title || "";
  elements.notesInput.value = eventData.notes || "";
  if (elements.transportModeInput) {
    elements.transportModeInput.value = eventData.transport_mode || "bus_bahn";
  }
  if (elements.transportKilometersInput) {
    elements.transportKilometersInput.value = String(eventData.transport_kilometers ?? 0);
  }
  if (elements.transportAddressInput) {
    elements.transportAddressInput.value = eventData.transport_address || "";
  }

  const assistantHours = eventData.assistant_hours || {};
  elements.assistantHourInputs.forEach((input) => {
    input.value = String(assistantHours[input.dataset.assistantHours] ?? 0);
  });

  elements.recurrenceField.value = "none";
  elements.repeatCountField.value = "0";
  toggleAllDayFields(preserveEmptyFields);
  toggleAssistantFields();
  toggleRepeatCountField();
}

function openAddEventModal(triggerElement = document.getElementById("open-add-modal")) {
  seedFormDefaults();
  resetEventFormMode();
  openModal("add-modal", triggerElement);
}

function openEditEventModal() {
  if (!state.pendingEventData) {
    return;
  }

  state.editingEventId = state.pendingEventData.id;
  if (elements.addModalTitle) {
    elements.addModalTitle.textContent = "Edit Event";
  }
  if (elements.submitAddEventButton) {
    elements.submitAddEventButton.textContent = "Save Changes";
  }
  elements.recurrenceField.disabled = true;
  elements.repeatCountField.disabled = true;
  populateEventForm(state.pendingEventData);
  closeDeletePopover();
  openModal("add-modal", state.lastFocusedElement || document.activeElement);
}

async function submitAddEvent(event) {
  event.preventDefault();
  const assistantHours = getAssistantHoursPayload();
  const formData = {
    date: elements.dateInput.value,
    time: elements.allDayInput && elements.allDayInput.checked ? "" : elements.timeInput.value,
    end_time: elements.allDayInput && elements.allDayInput.checked ? "" : elements.endTimeInput.value,
    all_day: Boolean(elements.allDayInput && elements.allDayInput.checked),
    category: elements.categoryField.value,
    title: elements.titleInput.value.trim(),
    notes: elements.notesInput.value.trim(),
    hours: sumAssistantHours(assistantHours),
    assistant_hours: assistantHours,
    transport_mode: elements.transportModeInput ? elements.transportModeInput.value : "",
    transport_kilometers: parseFloat(elements.transportKilometersInput?.value || "0"),
    transport_address: elements.transportAddressInput ? elements.transportAddressInput.value.trim() : "",
    recurrence: elements.recurrenceField.value,
    repeat_count: parseInt(elements.repeatCountField.value || "0", 10),
  };

  if (state.editingEventId || formData.recurrence === "none") {
    formData.repeat_count = 0;
    formData.recurrence = "none";
  }

  if (formData.category !== "assistant") {
    formData.hours = 0;
    Object.keys(formData.assistant_hours).forEach((field) => {
      formData.assistant_hours[field] = 0;
    });
  }
  if (formData.category !== "transport") {
    formData.transport_mode = "";
    formData.transport_kilometers = 0;
    formData.transport_address = "";
  }

  const validationError = validateForm(formData);
  if (validationError) {
    showError(validationError);
    return;
  }

  const requestUrl = state.editingEventId
    ? `/api/events/${encodeURIComponent(state.editingEventId)}`
    : "/api/events";
  const requestMethod = state.editingEventId ? "PUT" : "POST";

  await apiFetch(requestUrl, {
    method: requestMethod,
    body: JSON.stringify(formData),
  });

  elements.addForm.reset();
  seedFormDefaults();
  resetEventFormMode();
  closeModal("add-modal");
  await refreshCalendarData({ force: true });
}

function getVoiceContext(target) {
  if (target === "automation") {
    return {
      target: "automation",
      card: elements.automationVoiceCard,
      button: elements.automationVoiceButton,
      statusEl: elements.automationVoiceStatus,
      transcriptEl: elements.automationVoiceTranscript,
      idleStatus: 'Try: "Remind me on the last day of every month and prepare the Assistenzbeitrag"',
      recordingStatus: "Listening… tap mic again to stop",
      processingStatus: "Sending to OpenAI...",
    };
  }
  if (target === "chat") {
    return {
      target: "chat",
      card: elements.adviserForm,
      button: elements.chatVoiceButton,
      statusEl: elements.chatVoiceStatus,
      transcriptEl: null,
      idleStatus: "",
      recordingStatus: "Listening... tap mic again to stop",
      processingStatus: "Transcribing...",
    };
  }
  return {
    target: "event",
    card: elements.voiceComposer,
    button: elements.voiceComposerButton,
    statusEl: elements.voiceComposerStatus,
    transcriptEl: elements.voiceComposerTranscript,
    idleStatus: 'Tap to dictate — e.g. "Tomorrow 9 to 12 Körperpflege"',
    recordingStatus: "Listening… tap mic again to stop",
    processingStatus: "Transcribing & extracting fields…",
  };
}

function getVoiceStateKeys(target) {
  if (target === "automation") {
    return {
      recorder: "automationVoiceRecorder",
      stream: "automationVoiceStream",
      chunks: "automationVoiceChunks",
      recognition: "automationLiveRecognition",
      finalTranscript: "automationFinalTranscript",
      interimTranscript: "automationInterimTranscript",
      processing: "automationVoiceProcessing",
    };
  }
  if (target === "chat") {
    return {
      recorder: "chatVoiceRecorder",
      stream: "chatVoiceStream",
      chunks: "chatVoiceChunks",
      recognition: "chatVoiceLiveRecognition",
      finalTranscript: "chatVoiceFinalTranscript",
      interimTranscript: "chatVoiceInterimTranscript",
      processing: "chatVoiceProcessing",
    };
  }
  return {
    recorder: "voiceRecorder",
    stream: "voiceStream",
    chunks: "voiceChunks",
    recognition: "voiceLiveRecognition",
    finalTranscript: "voiceFinalTranscript",
    interimTranscript: "voiceInterimTranscript",
    processing: "voiceProcessing",
  };
}

function syncChatVoiceStatusRow() {
  if (!elements.chatVoiceStatusRow) {
    return;
  }
  const hasStatus = Boolean(elements.chatVoiceStatus && elements.chatVoiceStatus.textContent.trim());
  const hasTranscript = Boolean(elements.chatVoiceTranscript && elements.chatVoiceTranscript.textContent.trim());
  elements.chatVoiceStatusRow.classList.toggle("hidden", !hasStatus && !hasTranscript);
}

function setVoiceComposerState(target, mode) {
  const ctx = getVoiceContext(target);
  if (!ctx.button) {
    return;
  }

  const icon = ctx.button.querySelector(".voice-composer-icon");
  ctx.button.classList.toggle("is-recording", mode === "recording");
  ctx.button.classList.toggle("is-processing", mode === "processing");
  ctx.button.disabled = mode === "processing" || !isOpenAiConfigured();
  if (ctx.card) {
    ctx.card.classList.toggle("is-recording", mode === "recording");
    ctx.card.classList.toggle("is-processing", mode === "processing");
  }

  if (icon) {
    if (mode === "recording") {
      icon.textContent = "stop_circle";
    } else if (mode === "processing") {
      icon.textContent = "auto_awesome";
    } else {
      icon.textContent = ctx.target === "automation" ? "graphic_eq" : "mic";
    }
  }

  if (ctx.statusEl) {
    if (mode === "recording") {
      ctx.statusEl.textContent = ctx.recordingStatus;
    } else if (mode === "processing") {
      ctx.statusEl.textContent = ctx.processingStatus;
    } else if (!isOpenAiConfigured()) {
      ctx.statusEl.textContent = openAiUnavailableMessage();
    } else {
      ctx.statusEl.textContent = ctx.idleStatus;
    }
  }
  if (target === "chat") {
    syncChatVoiceStatusRow();
  }
}

function setVoiceTranscript(target, text, isInterim) {
  const ctx = getVoiceContext(target);
  if (!ctx.transcriptEl) {
    return;
  }
  ctx.transcriptEl.textContent = text || "";
  ctx.transcriptEl.classList.toggle("is-interim", Boolean(isInterim));
  if (target === "chat") {
    syncChatVoiceStatusRow();
  }
}

function startLiveTranscription(target) {
  const SpeechRecognitionImpl = window.SpeechRecognition || window.webkitSpeechRecognition;
  if (!SpeechRecognitionImpl) {
    return null;
  }
  try {
    const recognition = new SpeechRecognitionImpl();
    recognition.lang = navigator.language || "en-US";
    recognition.continuous = true;
    recognition.interimResults = true;

    recognition.addEventListener("result", (event) => {
      const keys = getVoiceStateKeys(target);
      let finalText = state[keys.finalTranscript];
      let interimText = "";
      for (let i = event.resultIndex; i < event.results.length; i += 1) {
        const result = event.results[i];
        const fragment = result[0]?.transcript || "";
        if (result.isFinal) {
          finalText = `${finalText} ${fragment}`.trim();
        } else {
          interimText = `${interimText} ${fragment}`.trim();
        }
      }
      state[keys.finalTranscript] = finalText;
      state[keys.interimTranscript] = interimText;
      const combined = `${finalText} ${interimText}`.trim();
      setVoiceTranscript(target, combined, !finalText && Boolean(interimText));
    });

    recognition.addEventListener("error", () => {
      // Silent fail — backend still returns the authoritative transcript.
    });

    recognition.start();
    return recognition;
  } catch (error) {
    return null;
  }
}

function stopLiveTranscription(target) {
  const keys = getVoiceStateKeys(target);
  if (state[keys.recognition]) {
    try { state[keys.recognition].stop(); } catch (e) { /* ignore */ }
    state[keys.recognition] = null;
  }
}

function resetVoiceTranscriptBuffers(target) {
  const keys = getVoiceStateKeys(target);
  state[keys.finalTranscript] = "";
  state[keys.interimTranscript] = "";
  setVoiceTranscript(target, "", false);
}

function stopVoiceStream(target) {
  const streamKey = getVoiceStateKeys(target).stream;
  if (!state[streamKey]) {
    return;
  }
  state[streamKey].getTracks().forEach((track) => track.stop());
  state[streamKey] = null;
}

function getPreferredAudioMimeType() {
  if (window.MediaRecorder && MediaRecorder.isTypeSupported("audio/webm")) {
    return "audio/webm";
  }
  if (window.MediaRecorder && MediaRecorder.isTypeSupported("audio/mp4")) {
    return "audio/mp4";
  }
  return "";
}

async function startVoiceRecording(target) {
  if (!isOpenAiConfigured()) {
    showError(openAiUnavailableMessage());
    applyAiStatus();
    return;
  }

  if (!navigator.mediaDevices || !navigator.mediaDevices.getUserMedia || !window.MediaRecorder) {
    showError("Voice recording is not supported in this browser.");
    return;
  }

  const keys = getVoiceStateKeys(target);
  const recorderKey = keys.recorder;
  const streamKey = keys.stream;
  const chunksKey = keys.chunks;
  const recogKey = keys.recognition;

  try {
    state[chunksKey] = [];
    state[streamKey] = await navigator.mediaDevices.getUserMedia({ audio: true });
    const mimeType = getPreferredAudioMimeType();
    const recorderOptions = mimeType ? { mimeType } : {};
    const recorder = new MediaRecorder(state[streamKey], recorderOptions);
    state[recorderKey] = recorder;

    recorder.addEventListener("dataavailable", (event) => {
      if (event.data && event.data.size > 0) {
        state[chunksKey].push(event.data);
      }
    });

    recorder.addEventListener("stop", () => {
      const chunks = [...state[chunksKey]];
      const recordingType = state[recorderKey] ? state[recorderKey].mimeType : mimeType || "audio/webm";
      state[recorderKey] = null;
      state[chunksKey] = [];
      stopVoiceStream(target);
      stopLiveTranscription(target);
      submitVoiceRecording(target, chunks, recordingType).catch((error) => {
        showError(error.message || "Voice request failed.");
        setVoiceComposerState(target, "idle");
      });
    });

    resetVoiceTranscriptBuffers(target);
    state[recogKey] = startLiveTranscription(target);
    recorder.start();
    setVoiceComposerState(target, "recording");
  } catch (error) {
    stopVoiceStream(target);
    stopLiveTranscription(target);
    state[recorderKey] = null;
    setVoiceComposerState(target, "idle");
    showError(error.message || "Could not start voice recording.");
  }
}

function stopVoiceRecording(target) {
  const recorderKey = getVoiceStateKeys(target).recorder;
  const recorder = state[recorderKey];
  if (!recorder || recorder.state === "inactive") {
    return;
  }
  recorder.stop();
  setVoiceComposerState(target, "processing");
}

async function submitVoiceRecording(target, chunks, mimeType) {
  if (target === "automation") {
    return submitAutomationVoiceRecording(chunks, mimeType);
  }
  if (target === "chat") {
    return submitChatVoiceRecording(chunks, mimeType);
  }
  return submitEventVoiceRecording(chunks, mimeType);
}

async function submitEventVoiceRecording(chunks, mimeType) {
  if (!isOpenAiConfigured()) {
    throw new Error(openAiUnavailableMessage());
  }

  const audioBlob = new Blob(chunks, { type: mimeType || "audio/webm" });
  if (!audioBlob.size) {
    throw new Error("No voice recording was captured.");
  }

  const formData = new FormData();
  const extension = audioBlob.type.includes("mp4") ? "mp4" : "webm";
  formData.append("audio", audioBlob, `calendar-voice.${extension}`);
  formData.append("timezone", Intl.DateTimeFormat().resolvedOptions().timeZone || "Europe/Berlin");
  formData.append("now", new Date().toISOString());

  state.voiceProcessing = true;
  setVoiceComposerState("event", "processing");
  try {
    const response = await fetch("/api/calendar/voice/draft", {
      method: "POST",
      body: formData,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error || "Voice calendar request failed.");
    }
    applyVoiceDraftToOpenModal(payload);
  } finally {
    state.voiceProcessing = false;
    setVoiceComposerState("event", "idle");
  }
}

async function submitChatVoiceRecording(chunks, mimeType) {
  if (!isOpenAiConfigured()) {
    throw new Error(openAiUnavailableMessage());
  }

  const audioBlob = new Blob(chunks, { type: mimeType || "audio/webm" });
  if (!audioBlob.size) {
    throw new Error("No voice recording was captured.");
  }

  const formData = new FormData();
  const extension = audioBlob.type.includes("mp4") ? "mp4" : "webm";
  formData.append("audio", audioBlob, `chat-voice.${extension}`);
  formData.append("timezone", Intl.DateTimeFormat().resolvedOptions().timeZone || "Europe/Berlin");
  formData.append("now", new Date().toISOString());

  state.chatVoiceProcessing = true;
  setVoiceComposerState("chat", "processing");
  let finalStatus = "";
  try {
    const response = await fetch("/api/chat/voice/transcribe", {
      method: "POST",
      body: formData,
    });
    if (!response.ok) {
      throw new Error(await readResponseError(response, "Voice transcription failed."));
    }
    const payload = await response.json().catch(() => ({}));
    const transcript = String(payload.transcript || "").trim();
    if (!transcript) {
      throw new Error("Voice transcription returned no text.");
    }
    if (elements.adviserInput) {
      elements.adviserInput.value = transcript;
      elements.adviserInput.focus();
    }
    setVoiceTranscript("chat", "", false);
    finalStatus = "";
  } catch (error) {
    setVoiceTranscript("chat", "", false);
    finalStatus = error.message || "Voice transcription failed.";
  } finally {
    state.chatVoiceProcessing = false;
    setVoiceComposerState("chat", "idle");
    if (elements.chatVoiceStatus) {
      elements.chatVoiceStatus.textContent = finalStatus;
    }
    syncChatVoiceStatusRow();
  }
}

function applyVoiceDraftToOpenModal(payload) {
  const draft = payload && payload.draft ? payload.draft : {};
  populateEventForm(draft, { preserveEmptyFields: true });

  const transcript = String(payload.transcript || "").trim();
  const missingFields = Array.isArray(payload.missing_fields) ? payload.missing_fields : [];
  const warnings = Array.isArray(payload.warnings) ? payload.warnings : [];

  if (transcript) {
    setVoiceTranscript("event", transcript, false);
  }

  if (elements.voiceDraftPreview) {
    elements.voiceDraftPreview.classList.remove("hidden");
  }
  if (elements.voiceDraftTranscript) {
    elements.voiceDraftTranscript.textContent = transcript ? `Transcript: ${transcript}` : "";
  }
  if (elements.voiceDraftStatus) {
    const statusParts = [];
    if (missingFields.length) {
      statusParts.push(`Needs review: ${missingFields.join(", ")}`);
    }
    if (warnings.length) {
      statusParts.push(warnings.join(" "));
    }
    elements.voiceDraftStatus.textContent = statusParts.join(" ");
  }
}

function handleEventVoiceButtonClick() {
  if (state.voiceProcessing) {
    return;
  }
  if (state.voiceRecorder && state.voiceRecorder.state === "recording") {
    stopVoiceRecording("event");
    return;
  }
  startVoiceRecording("event").catch((error) => {
    showError(error.message || "Could not start voice recording.");
  });
}

function handleAutomationVoiceButtonClick() {
  if (state.automationVoiceProcessing) {
    return;
  }
  const recorder = state.automationVoiceRecorder;
  if (recorder && recorder.state === "recording") {
    stopVoiceRecording("automation");
    return;
  }
  startVoiceRecording("automation").catch((error) => {
    showError(error.message || "Could not start voice recording.");
  });
}

function handleChatVoiceButtonClick() {
  if (state.chatVoiceProcessing) {
    return;
  }
  if (state.chatVoiceRecorder && state.chatVoiceRecorder.state === "recording") {
    stopVoiceRecording("chat");
    return;
  }
  startVoiceRecording("chat").catch((error) => {
    showError(error.message || "Could not start voice recording.");
  });
}

async function openExportModal() {
  const data = await apiFetch(`/api/export?month=${encodeURIComponent(state.currentMonth)}`);
  elements.exportSummary.textContent = data.summary;
  openModal("export-modal", document.getElementById("open-export-modal"));
}

async function copyExportSummary() {
  try {
    await navigator.clipboard.writeText(elements.exportSummary.textContent);
  } catch (error) {
    showError("Clipboard copy failed");
  }
}

function setReportStatus(message = "", variant = "") {
  if (!message) {
    elements.reportStatus.textContent = "";
    elements.reportStatus.classList.add("hidden");
    elements.reportStatus.classList.remove("is-success", "is-error");
    return;
  }

  elements.reportStatus.textContent = message;
  elements.reportStatus.classList.remove("hidden", "is-success", "is-error");
  if (variant === "success") {
    elements.reportStatus.classList.add("is-success");
  } else if (variant === "error") {
    elements.reportStatus.classList.add("is-error");
  }
}

function setReportModalMode(mode = "setup") {
  const isResultsMode = mode === "results";

  if (elements.reportConfigPanel) {
    elements.reportConfigPanel.classList.toggle("hidden", isResultsMode);
  }
  if (elements.reportResultsActions) {
    elements.reportResultsActions.classList.toggle("hidden", !isResultsMode);
  }
  if (elements.reportModal) {
    elements.reportModal.classList.toggle("report-modal-results", isResultsMode);
  }
}

function selectGeneratedReport(report) {
  state.activeGeneratedReport = report || null;

  if (state.activeGeneratedReport) {
    elements.reportPreviewFrame.setAttribute("src", state.activeGeneratedReport.previewUrl);
    elements.reportPreviewWrap.classList.remove("hidden");
  } else {
    elements.reportPreviewWrap.classList.add("hidden");
    elements.reportPreviewFrame.removeAttribute("src");
  }

  renderGeneratedReports();
}

function resetReportWorkflow() {
  populateReportMonthOptions(state.currentMonth);
  elements.reportMonthInput.value = state.currentMonth;
  setSelectedReportTypes(["assistenzbeitrag"]);
  state.generatedReports = [];
  state.activeGeneratedReport = null;
  elements.reportPreviewWrap.classList.add("hidden");
  elements.reportPreviewFrame.removeAttribute("src");
  elements.reportResultsWrap.classList.add("hidden");
  elements.reportResultsList.innerHTML = "";
  setReportStatus("");
  setReportModalMode("setup");
}

function renderGeneratedReports() {
  elements.reportResultsList.innerHTML = "";

  if (!state.generatedReports.length) {
    elements.reportResultsWrap.classList.add("hidden");
    return;
  }

  state.generatedReports.forEach((report) => {
    const card = document.createElement("article");
    card.className = "report-result-card";
    if (
      state.activeGeneratedReport
      && (
        (state.activeGeneratedReport.reportId && state.activeGeneratedReport.reportId === report.reportId)
        || (!state.activeGeneratedReport.reportId && state.activeGeneratedReport.fileName === report.fileName)
      )
    ) {
      card.classList.add("is-active");
    }
    card.tabIndex = 0;
    card.addEventListener("click", () => {
      const sameReport = state.activeGeneratedReport && (
        (state.activeGeneratedReport.reportId && state.activeGeneratedReport.reportId === report.reportId)
        || (!state.activeGeneratedReport.reportId && state.activeGeneratedReport.fileName === report.fileName)
      );
      if (!sameReport) {
        selectGeneratedReport(report);
      }
    });
    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectGeneratedReport(report);
      }
    });

    const title = document.createElement("p");
    title.className = "report-result-title";
    title.textContent = report.label || reportTypeLabelMap[report.type] || report.type;

    const meta = document.createElement("p");
    meta.className = "report-result-meta";
    meta.textContent = report.fileName;

    const actions = document.createElement("div");
    actions.className = "report-actions-row";

    const downloadLink = document.createElement("a");
    downloadLink.className = "secondary-button";
    downloadLink.href = report.downloadUrl;
    downloadLink.download = report.fileName;
    downloadLink.textContent = "Download";
    downloadLink.addEventListener("click", (event) => {
      event.stopPropagation();
    });

    actions.appendChild(downloadLink);
    card.appendChild(title);
    card.appendChild(meta);
    card.appendChild(actions);
    elements.reportResultsList.appendChild(card);
  });

  elements.reportResultsWrap.classList.remove("hidden");
}

function openReportModal(triggerElement = elements.generateReport) {
  resetReportWorkflow();
  openModal("report-modal", triggerElement);
}

async function submitReportForm(event) {
  event.preventDefault();
  const month = elements.reportMonthInput.value;
  if (!month) {
    setReportStatus("Please select a reporting month.", "error");
    return;
  }

  elements.submitReport.disabled = true;
  elements.submitReport.textContent = "Generating...";
  elements.reportPreviewWrap.classList.add("hidden");
  elements.reportPreviewFrame.removeAttribute("src");
  elements.reportResultsWrap.classList.add("hidden");
  elements.reportResultsList.innerHTML = "";
  state.generatedReports = [];
  state.activeGeneratedReport = null;
  setReportStatus("Generating PDF report...", "");

  try {
    const result = await apiFetch("/api/reports/generate", {
      method: "POST",
      body: JSON.stringify({ month, report_types: getSelectedReportTypes() }),
    });

    state.generatedReports = (result.generated_reports || []).map((report) => ({
      reportId: report.report_id || "",
      type: report.type,
      label: report.label || reportTypeLabelMap[report.type] || report.type,
      month,
      fileName: report.file_name,
      downloadUrl: report.download_url,
      previewUrl: report.preview_url || report.download_url,
    }));
    setReportModalMode(state.generatedReports.length ? "results" : "setup");
    selectGeneratedReport(state.generatedReports[0] || null);

    const unavailableMessages = (result.unavailable_reports || [])
      .map((report) => `${report.label}: ${report.message}`)
      .join(" ");

    if (state.generatedReports.length && unavailableMessages) {
      setReportStatus(`Generated available report files. ${unavailableMessages}`, "success");
    } else if (state.generatedReports.length) {
      setReportStatus("Report generated successfully.", "success");
    } else if (unavailableMessages) {
      setReportStatus(unavailableMessages, "error");
    } else {
      setReportStatus("No report was generated.", "error");
    }

  } catch (error) {
    setReportStatus(error.message || "Failed to generate report.", "error");
  } finally {
    elements.submitReport.disabled = false;
    elements.submitReport.textContent = "Generate PDF";
  }
}

async function sendGeneratedReport() {
  if (!state.activeGeneratedReport || (!state.activeGeneratedReport.fileName && !state.activeGeneratedReport.reportId)) {
    setReportStatus("Generate a report first before sending.", "error");
    return;
  }

  if (!elements.sendReport) {
    return;
  }

  elements.sendReport.disabled = true;
  elements.sendReport.textContent = "Sending...";
  setReportStatus("Sending report trigger...", "");

  try {
    await apiFetch("/api/reports/send", {
      method: "POST",
      body: JSON.stringify({
        month: state.activeGeneratedReport.month,
        report_id: state.activeGeneratedReport.reportId || undefined,
        file_name: state.activeGeneratedReport.fileName,
      }),
    });
    setReportStatus(
      "Send trigger submitted. You can connect this endpoint to n8n webhook for email workflows.",
      "success"
    );
  } catch (error) {
    setReportStatus(error.message || "Failed to send report trigger.", "error");
  } finally {
    elements.sendReport.disabled = false;
    elements.sendReport.textContent = "Send Report";
  }
}

function closeDeletePopover() {
  state.pendingDeleteId = null;
  state.pendingEventData = null;
  elements.deletePopover.classList.add("hidden");
}

function positionDeletePopover(targetEl) {
  const rect = targetEl.getBoundingClientRect();
  const top = window.scrollY + rect.bottom + 8;
  const left = Math.max(16, Math.min(window.scrollX + rect.left, window.scrollX + window.innerWidth - 260));
  elements.deletePopover.style.top = `${top}px`;
  elements.deletePopover.style.left = `${left}px`;
}

function handleEventClick(info) {
  info.jsEvent.preventDefault();
  const rawEvent = info.event.extendedProps.rawEvent;
  state.pendingDeleteId = rawEvent.id;
  state.pendingEventData = rawEvent;
  elements.deleteTitle.textContent = rawEvent.title;
  const timeLabel = rawEvent.all_day
    ? "All day"
    : `${rawEvent.time}-${rawEvent.end_time || addMinutesToTime(rawEvent.time, 30)}`;
  elements.deleteMeta.textContent = `${rawEvent.date} ${timeLabel} | ${categoryLabelMap[rawEvent.category] || rawEvent.category}`;
  positionDeletePopover(info.el);
  elements.deletePopover.classList.remove("hidden");
}

async function confirmDelete() {
  if (!state.pendingDeleteId) {
    return;
  }
  await apiFetch(`/api/events/${encodeURIComponent(state.pendingDeleteId)}`, {
    method: "DELETE",
  });
  closeDeletePopover();
  await refreshCalendarData({ force: true });
}

function handleDocumentClick(event) {
  const clickInsidePopover = elements.deletePopover.contains(event.target);
  const clickInsideEvent = event.target.closest(".fc-event");
  if (!clickInsidePopover && !clickInsideEvent) {
    closeDeletePopover();
  }
  if (!event.target.closest(".thread-item") && state.openThreadMenuId) {
    closeThreadMenus();
  }
}

function handleKeydown(event) {
  if (event.key === "Escape") {
    resetChatDragState();
    if (state.openThreadMenuId) {
      closeThreadMenus();
      return;
    }
    if (state.activeModalId) {
      closeModal(state.activeModalId);
      return;
    }
    closeDeletePopover();
  }
}

function wireModalClosers() {
  document.querySelectorAll("[data-close-modal]").forEach((element) => {
    element.addEventListener("click", () => closeModal(element.getAttribute("data-close-modal")));
  });
}

function handleDatesSet() {
  if (!state.calendar) {
    return;
  }
  const activeDate = state.calendar.getDate();
  state.currentMonth = formatMonth(activeDate);
  syncMonthUi();
  refreshDashboardData().catch(() => {});
}

function initCalendar() {
  if (state.calendar) {
    return;
  }
  const calendarEl = document.getElementById("calendar");
  state.calendar = new FullCalendar.Calendar(calendarEl, {
    initialView: state.currentView,
    headerToolbar: false,
    height: "100%",
    initialDate: `${state.currentMonth}-01`,
    nowIndicator: true,
    allDaySlot: true,
    weekends: false,
    hiddenDays: [0, 6],
    slotMinTime: "07:00:00",
    slotMaxTime: "19:00:00",
    scrollTime: "07:00:00",
    slotDuration: "00:30:00",
    forceEventDuration: true,
    eventClick: handleEventClick,
    eventContent: renderEventContent,
    events: fetchEvents,
    datesSet: handleDatesSet,
    eventColor: "transparent",
    eventTextColor: "#1f2b3d",
    eventBorderColor: "transparent",
    dayMaxEventRows: 4,
    views: {
      timeGridWeek: {
        dayHeaderFormat: { weekday: "short", day: "numeric" },
      },
      timeGridDay: {
        dayHeaderFormat: { weekday: "long", month: "short", day: "numeric" },
      },
      dayGridMonth: {
        dayHeaderFormat: { weekday: "short" },
      },
    },
  });
  state.calendar.render();
}

function seedFormDefaults() {
  elements.dateInput.value = `${state.currentMonth}-01`;
  if (elements.allDayInput) {
    elements.allDayInput.checked = false;
  }
  elements.timeInput.value = "09:00";
  elements.endTimeInput.value = "09:30";
  elements.categoryField.value = "assistant";
  if (elements.transportModeInput) {
    elements.transportModeInput.value = "bus_bahn";
  }
  if (elements.transportKilometersInput) {
    elements.transportKilometersInput.value = "0.0";
  }
  if (elements.transportAddressInput) {
    elements.transportAddressInput.value = "";
  }
  elements.recurrenceField.value = "none";
  elements.repeatCountField.value = "0";
  toggleAssistantFields();
  toggleAllDayFields();
  toggleRepeatCountField();
}

function normalizeChatMarkdownText(rawText) {
  return String(rawText || "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(/([^\n])\s+(-\s+\*\*?\d{1,2}[./]\d{1,2}[./]\d{2,4}\*\*?)/g, "$1\n$2")
    .replace(/([^\n])\s+(-\s+\*\*?Total[^:\n]*:)/gi, "$1\n\n$2")
    .trim();
}

function appendInlineMarkdown(parent, rawText) {
  const text = String(rawText || "");
  const tokenPattern = /(\*\*[^*\n][\s\S]*?\*\*|`[^`\n]+`|\[[^\]\n]+\]\((https?:\/\/[^)\s]+)\))/g;
  let cursor = 0;
  let match = tokenPattern.exec(text);

  while (match) {
    if (match.index > cursor) {
      parent.appendChild(document.createTextNode(text.slice(cursor, match.index)));
    }

    const token = match[0];
    if (token.startsWith("**") && token.endsWith("**")) {
      const strong = document.createElement("strong");
      strong.textContent = token.slice(2, -2);
      parent.appendChild(strong);
    } else if (token.startsWith("`") && token.endsWith("`")) {
      const code = document.createElement("code");
      code.textContent = token.slice(1, -1);
      parent.appendChild(code);
    } else {
      const linkMatch = token.match(/^\[([^\]\n]+)\]\((https?:\/\/[^)\s]+)\)$/);
      if (linkMatch) {
        const link = document.createElement("a");
        link.href = linkMatch[2];
        link.target = "_blank";
        link.rel = "noreferrer";
        link.textContent = linkMatch[1];
        parent.appendChild(link);
      } else {
        parent.appendChild(document.createTextNode(token));
      }
    }

    cursor = match.index + token.length;
    match = tokenPattern.exec(text);
  }

  if (cursor < text.length) {
    parent.appendChild(document.createTextNode(text.slice(cursor)));
  }
}

function parseMarkdownList(lines, startIndex, baseIndent) {
  const list = document.createElement("ul");
  list.className = "chat-markdown-list";
  let index = startIndex;
  let lastItem = null;

  while (index < lines.length) {
    const rawLine = lines[index];
    const match = rawLine.match(/^(\s*)-\s+(.+)$/);
    if (!match) {
      break;
    }

    const indent = match[1].length;
    if (indent < baseIndent) {
      break;
    }

    if (indent > baseIndent) {
      if (lastItem) {
        const nested = parseMarkdownList(lines, index, indent);
        lastItem.appendChild(nested.node);
        index = nested.nextIndex;
        continue;
      }
      break;
    }

    const item = document.createElement("li");
    appendInlineMarkdown(item, match[2].trim());
    list.appendChild(item);
    lastItem = item;
    index += 1;
  }

  return { node: list, nextIndex: index };
}

function renderChatMarkdown(messageText) {
  const wrapper = document.createElement("div");
  wrapper.className = "chat-markdown";
  const normalizedText = normalizeChatMarkdownText(messageText);

  if (!normalizedText) {
    return wrapper;
  }

  const lines = normalizedText.split("\n");
  let paragraphLines = [];
  let index = 0;

  function flushParagraph() {
    if (!paragraphLines.length) {
      return;
    }
    const paragraph = document.createElement("p");
    appendInlineMarkdown(paragraph, paragraphLines.join(" ").trim());
    wrapper.appendChild(paragraph);
    paragraphLines = [];
  }

  while (index < lines.length) {
    const line = lines[index];
    const trimmed = line.trim();

    if (!trimmed) {
      flushParagraph();
      index += 1;
      continue;
    }

    if (trimmed.startsWith("```")) {
      flushParagraph();
      const codeLines = [];
      index += 1;
      while (index < lines.length && !lines[index].trim().startsWith("```")) {
        codeLines.push(lines[index]);
        index += 1;
      }
      if (index < lines.length) {
        index += 1;
      }
      const pre = document.createElement("pre");
      const code = document.createElement("code");
      code.textContent = codeLines.join("\n");
      pre.appendChild(code);
      wrapper.appendChild(pre);
      continue;
    }

    const headingMatch = trimmed.match(/^(#{2,4})\s+(.+)$/);
    if (headingMatch) {
      flushParagraph();
      const level = Math.min(headingMatch[1].length, 4);
      const heading = document.createElement(`h${level}`);
      appendInlineMarkdown(heading, headingMatch[2].trim());
      wrapper.appendChild(heading);
      index += 1;
      continue;
    }

    const bulletMatch = line.match(/^(\s*)-\s+(.+)$/);
    if (bulletMatch) {
      flushParagraph();
      const parsedList = parseMarkdownList(lines, index, bulletMatch[1].length);
      wrapper.appendChild(parsedList.node);
      index = parsedList.nextIndex;
      continue;
    }

    if (/^[-*_]{3,}$/.test(trimmed)) {
      flushParagraph();
      wrapper.appendChild(document.createElement("hr"));
      index += 1;
      continue;
    }

    paragraphLines.push(trimmed);
    index += 1;
  }

  flushParagraph();
  return wrapper;
}

function appendChatMessage(role, messageText, policyCard = null, extras = {}) {
  if (!elements.chatThread) {
    return;
  }

  const row = document.createElement("div");
  row.className = `chat-row ${role}`;

  const avatar = document.createElement("div");
  avatar.className = "chat-avatar";
  const avatarIcon = document.createElement("span");
  avatarIcon.className = "material-symbols-outlined";
  avatarIcon.textContent = role === "user" ? "person" : "smart_toy";
  avatar.appendChild(avatarIcon);

  const bubble = document.createElement("div");
  bubble.className = "chat-bubble";

  if (role === "user") {
    const message = document.createElement("p");
    message.textContent = messageText;
    bubble.appendChild(message);
    if (extras && Array.isArray(extras.attachments) && extras.attachments.length) {
      bubble.appendChild(buildChatAttachmentSummary(extras.attachments));
    }
  } else {
    bubble.appendChild(renderChatMarkdown(messageText));
  }

  if (policyCard) {
    const card = document.createElement("div");
    card.className = "chat-policy-card";

    const title = document.createElement("p");
    title.className = "chat-policy-title";
    title.textContent = policyCard.title;
    card.appendChild(title);

    policyCard.rows.forEach((rowData) => {
      const line = document.createElement("p");
      line.className = "chat-policy-row";
      const left = document.createElement("span");
      const right = document.createElement("strong");
      left.textContent = rowData.label;
      right.textContent = rowData.value;
      line.appendChild(left);
      line.appendChild(right);
      card.appendChild(line);
    });

    bubble.appendChild(card);
  }

  if (extras && Array.isArray(extras.citations) && extras.citations.length) {
    bubble.appendChild(buildCitationCard(extras.citations));
  }

  if (extras && Array.isArray(extras.pendingActions) && extras.pendingActions.length) {
    bubble.appendChild(buildPendingActionsCard(extras.pendingActions));
  }

  if (extras && Array.isArray(extras.toolEvents) && extras.toolEvents.length) {
    const completedTools = extras.toolEvents.filter((event) => event && event.status === "completed").length;
    if (completedTools > 0) {
      const toolsMeta = document.createElement("span");
      toolsMeta.className = "chat-tool-meta";
      toolsMeta.textContent = `${completedTools} tool${completedTools === 1 ? "" : "s"} used`;
      bubble.appendChild(toolsMeta);
    }
  }

  const meta = document.createElement("span");
  meta.className = "chat-meta";
  meta.textContent = role === "user" ? "You" : "IV Desk";
  bubble.appendChild(meta);

  row.appendChild(avatar);
  row.appendChild(bubble);
  elements.chatThread.appendChild(row);
  scrollChatToBottom();
}

function buildChatAttachmentSummary(attachments) {
  const wrapper = document.createElement("div");
  wrapper.className = "chat-attachment-summary";
  attachments.slice(0, chatAttachmentMaxFiles).forEach((attachment) => {
    const item = document.createElement("span");
    item.className = "chat-attachment-summary-item";
    const icon = document.createElement("span");
    icon.className = "material-symbols-outlined";
    icon.textContent = String(attachment.mime || "").startsWith("image/") ? "image" : "draft";
    const label = document.createElement("span");
    label.textContent = attachment.file_name || attachment.name || "attachment";
    item.appendChild(icon);
    item.appendChild(label);
    wrapper.appendChild(item);
  });
  return wrapper;
}

function buildCitationCard(citations) {
  const card = document.createElement("div");
  card.className = "chat-citation-card";
  const title = document.createElement("p");
  title.className = "chat-policy-title";
  title.textContent = "Sources";
  card.appendChild(title);

  citations.slice(0, 5).forEach((citation) => {
    const row = document.createElement(citation.url ? "a" : "div");
    row.className = "chat-citation-row";
    if (citation.url) {
      row.href = citation.url;
      row.target = "_blank";
      row.rel = "noreferrer";
    }
    const rowTitle = document.createElement("strong");
    rowTitle.textContent = citation.title || "Source";
    row.appendChild(rowTitle);
    if (citation.snippet) {
      const snippet = document.createElement("span");
      snippet.textContent = citation.snippet;
      row.appendChild(snippet);
    }
    card.appendChild(row);
  });
  return card;
}

function buildPendingActionsCard(actions) {
  const card = document.createElement("div");
  card.className = "chat-action-card";
  const title = document.createElement("p");
  title.className = "chat-policy-title";
  title.textContent = "Bestaetigung ausstehend";
  card.appendChild(title);

  actions.forEach((action) => {
    const row = document.createElement("div");
    row.className = "chat-action-row";

    const copy = document.createElement("div");
    copy.className = "chat-action-copy";
    const label = document.createElement("strong");
    label.textContent = action.title || action.type || "Aktion";
    const type = document.createElement("span");
    type.textContent = formatPendingActionSummary(action);
    copy.appendChild(label);
    copy.appendChild(type);

    const button = document.createElement("button");
    button.className = "secondary-button";
    button.type = "button";
    button.textContent = "Bestaetigen";
    button.addEventListener("click", () => confirmPendingAgentAction(action.action_id, button));

    row.appendChild(copy);
    row.appendChild(button);
    card.appendChild(row);
  });
  return card;
}

function formatPendingActionSummary(action) {
  const payload = action && action.payload && typeof action.payload === "object" ? action.payload : {};
  const type = String((action && action.type) || "").trim();
  const title = payload.title || (payload.matched_event && payload.matched_event.title) || "";
  const date = payload.date || (payload.matched_event && payload.matched_event.date) || "";
  const time = payload.time || (payload.matched_event && payload.matched_event.time) || "";
  const startAt = payload.start_at || "";
  const parts = [type];
  if (title) {
    parts.push(String(title));
  }
  if (date || time) {
    parts.push(`${date}${time ? ` ${time}` : ""}`.trim());
  } else if (startAt) {
    parts.push(String(startAt));
  }
  return parts.filter(Boolean).join(" - ");
}

function scrollChatToBottom() {
  if (!elements.chatThread) {
    return;
  }

  elements.chatThread.scrollTop = elements.chatThread.scrollHeight;
}

function createChatId() {
  return `chat-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function getInvoicesSessionId() {
  let sid = localStorage.getItem("invoices_sid");
  if (!sid) {
    sid = Math.random().toString(36).slice(2, 10);
    localStorage.setItem("invoices_sid", sid);
  }
  return sid;
}

function isSupportedChatAttachment(file) {
  const fileName = String((file && file.name) || "").toLowerCase();
  return Boolean(file && (
    String(file.type || "").startsWith("image/")
    || chatAttachmentMimeTypes.has(file.type)
    || /\.(pdf|png|jpe?g|txt|docx)$/.test(fileName)
  ));
}

function setChatAttachmentStatus(message, variant = "") {
  state.chatAttachmentStatus = message || "";
  state.chatAttachmentStatusVariant = variant || "";
  renderChatAttachments();
}

function fileToBase64Payload(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      resolve(result.includes(",") ? result.split(",").pop() : result);
    };
    reader.onerror = () => reject(reader.error || new Error("Could not read file"));
    reader.readAsDataURL(file);
  });
}

function renderChatAttachments() {
  if (!elements.chatAttachmentTray || !elements.chatAttachmentList || !elements.chatAttachmentStatus) {
    return;
  }

  const hasAttachments = state.chatAttachments.length > 0;
  const hasStatus = Boolean(state.chatAttachmentStatus);
  elements.chatAttachmentTray.classList.toggle("hidden", !hasAttachments && !hasStatus);
  elements.chatAttachmentStatus.textContent = state.chatAttachmentStatus;
  elements.chatAttachmentStatus.dataset.variant = state.chatAttachmentStatusVariant;

  elements.chatAttachmentList.innerHTML = "";
  state.chatAttachments.forEach((attachment) => {
    const chip = document.createElement("span");
    chip.className = "chat-attachment-chip";
    chip.innerHTML = `
      <span class="material-symbols-outlined">${String(attachment.mime || "").startsWith("image/") ? "image" : "picture_as_pdf"}</span>
      <span class="chat-attachment-name">${escapeHtml(attachment.name)}</span>
      <span class="chat-attachment-size">${escapeHtml(formatFileSize(attachment.size))}</span>
      <button type="button" data-chat-attachment-remove="${escapeHtml(attachment.id)}" aria-label="Remove ${escapeHtml(attachment.name)}">
        <span class="material-symbols-outlined">close</span>
      </button>
    `;
    elements.chatAttachmentList.appendChild(chip);
  });
}

async function addChatAttachmentFiles(fileList) {
  const incomingFiles = Array.from(fileList || []);
  if (!incomingFiles.length || state.chatPending) {
    return;
  }

  const accepted = [];
  const rejected = [];
  incomingFiles.forEach((file) => {
    if (!isSupportedChatAttachment(file)) {
      rejected.push(`${file.name || "File"}: unsupported type`);
      return;
    }
    if (file.size > chatAttachmentMaxBytes) {
      rejected.push(`${file.name || "File"}: larger than 10 MB`);
      return;
    }
    accepted.push(file);
  });

  const availableSlots = chatAttachmentMaxFiles - state.chatAttachments.length;
  const filesToRead = accepted.slice(0, Math.max(availableSlots, 0));
  if (accepted.length > filesToRead.length) {
    rejected.push(`Maximum ${chatAttachmentMaxFiles} files can be attached.`);
  }

  if (!filesToRead.length) {
    setChatAttachmentStatus(rejected.join(" ") || "No supported files selected.", "error");
    return;
  }

  setChatAttachmentStatus(
    filesToRead.length === 1 ? "Datei wird vorbereitet..." : "Dateien werden vorbereitet..."
  );

  try {
    for (const file of filesToRead) {
      const contentBase64 = await fileToBase64Payload(file);
      state.chatAttachments.push({
        id: `att-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        type: "file",
        name: file.name || "attachment",
        file_name: file.name || "attachment",
        mime: file.type || "application/octet-stream",
        size: file.size,
        content_base64: contentBase64,
      });
    }
    setChatAttachmentStatus(
      rejected.length ? rejected.join(" ") : `${state.chatAttachments.length} Datei${state.chatAttachments.length === 1 ? "" : "en"} angehaengt. Upload startet beim Senden.`,
      rejected.length ? "error" : "success"
    );
  } catch (error) {
    setChatAttachmentStatus(error.message || "Could not attach file.", "error");
  } finally {
    if (elements.chatFileInput) {
      elements.chatFileInput.value = "";
    }
  }
}

function removeChatAttachment(attachmentId) {
  state.chatAttachments = state.chatAttachments.filter((attachment) => attachment.id !== attachmentId);
  setChatAttachmentStatus(
    state.chatAttachments.length ? `${state.chatAttachments.length} Datei${state.chatAttachments.length === 1 ? "" : "en"} angehaengt. Upload startet beim Senden.` : ""
  );
}

function getChatAttachmentPayload() {
  return state.chatAttachments.map((attachment) => ({
    type: attachment.type,
    name: attachment.name,
    file_name: attachment.file_name,
    mime: attachment.mime,
    size: attachment.size,
    content_base64: attachment.content_base64,
  }));
}

function clearChatAttachments(statusMessage = "") {
  state.chatAttachments = [];
  setChatAttachmentStatus(statusMessage);
}

function handleChatAttachmentListClick(event) {
  const removeButton = event.target.closest("[data-chat-attachment-remove]");
  if (!removeButton) {
    return;
  }
  removeChatAttachment(removeButton.getAttribute("data-chat-attachment-remove"));
}

function isFileDragEvent(event) {
  return Boolean(event.dataTransfer && Array.from(event.dataTransfer.types || []).includes("Files"));
}

function resetChatDragState() {
  state.chatDragDepth = 0;
  if (elements.adviserShell) {
    elements.adviserShell.classList.remove("is-dragging-files");
  }
}

function handleGlobalFileDragOver(event) {
  if (!isFileDragEvent(event)) {
    return;
  }
  event.preventDefault();
}

function handleGlobalFileDrop(event) {
  if (!isFileDragEvent(event)) {
    return;
  }
  event.preventDefault();
  if (!elements.adviserShell || !elements.adviserShell.contains(event.target)) {
    resetChatDragState();
  }
}

function handleChatDragEvent(event) {
  if (!isFileDragEvent(event)) {
    return;
  }
  event.preventDefault();

  if (event.type === "dragenter") {
    state.chatDragDepth += 1;
    elements.adviserShell.classList.add("is-dragging-files");
  } else if (event.type === "dragover") {
    event.dataTransfer.dropEffect = "copy";
  } else if (event.type === "dragleave") {
    const nextTarget = event.relatedTarget;
    if (!nextTarget || !elements.adviserShell.contains(nextTarget)) {
      resetChatDragState();
    } else {
      state.chatDragDepth = Math.max(state.chatDragDepth - 1, 0);
    }
  } else if (event.type === "drop") {
    resetChatDragState();
    addChatAttachmentFiles(event.dataTransfer.files).catch((error) => {
      setChatAttachmentStatus(error.message || "Could not attach dropped files.", "error");
    });
  }
}

async function openChatQrModal(triggerElement) {
  if (state.chatQrLoading) {
    return;
  }

  const sid = getInvoicesSessionId();
  if (elements.chatQrStatus) {
    elements.chatQrStatus.textContent = "QR-Code wird geladen...";
    elements.chatQrStatus.dataset.variant = "";
  }
  if (elements.chatQrImage) {
    elements.chatQrImage.removeAttribute("src");
  }
  openModal("chat-qr-modal", triggerElement || elements.chatQrButton || document.activeElement);

  state.chatQrLoading = true;
  try {
    const response = await fetch(`/api/invoices/${encodeURIComponent(sid)}/scan-url`);
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.error || "QR-Code konnte nicht geladen werden.");
    }
    const url = data.camera_url || data.scan_url;
    if (!url) {
      throw new Error("QR-Code konnte nicht geladen werden.");
    }
    if (elements.chatQrImage) {
      elements.chatQrImage.src = `https://api.qrserver.com/v1/create-qr-code/?size=420x420&margin=8&data=${encodeURIComponent(url)}`;
      elements.chatQrImage.title = url;
    }
    if (elements.chatCameraLink) {
      elements.chatCameraLink.href = url;
      elements.chatCameraLink.title = url;
    }
    if (elements.chatQrStatus) {
      elements.chatQrStatus.textContent = "Bereit zum Scannen.";
      elements.chatQrStatus.dataset.variant = "success";
    }
  } catch (error) {
    if (elements.chatQrStatus) {
      elements.chatQrStatus.textContent = error.message || "QR-Code konnte nicht geladen werden.";
      elements.chatQrStatus.dataset.variant = "error";
    }
  } finally {
    state.chatQrLoading = false;
  }
}

function normalizeChatSession(rawChat, index = 0) {
  const now = new Date().toISOString();
  const fallbackTitle = index === 0 ? "IV Desk planning" : `Chat ${index + 1}`;
  const messages = Array.isArray(rawChat && rawChat.messages)
    ? rawChat.messages
      .map((message) => ({
        role: String((message && message.role) || "").trim(),
        text: String((message && message.text) || "").trim(),
        timestamp: String((message && message.timestamp) || now),
      }))
      .filter((message) => message.role && message.text)
    : [];

  return {
    id: String((rawChat && rawChat.id) || createChatId()),
    title: String((rawChat && rawChat.title) || fallbackTitle).trim() || fallbackTitle,
    threadId: String((rawChat && rawChat.threadId) || "").trim(),
    messages,
    createdAt: String((rawChat && rawChat.createdAt) || now),
    updatedAt: String((rawChat && rawChat.updatedAt) || now),
  };
}

function seedChatSessions() {
  const now = new Date().toISOString();
  return [
    "IV Desk planning",
    "Assistenzbeitrag report",
    "Transport receipts",
    "Calendar cleanup",
  ].map((title, index) => normalizeChatSession({
    id: `seed-chat-${index + 1}`,
    title,
    createdAt: now,
    updatedAt: now,
  }, index));
}

function loadChatSessions() {
  try {
    const parsed = JSON.parse(localStorage.getItem(chatSessionsStorageKey) || "[]");
    state.chats = Array.isArray(parsed) && parsed.length
      ? parsed.map(normalizeChatSession)
      : seedChatSessions();
  } catch (error) {
    state.chats = seedChatSessions();
  }

  if (!state.chats.some((chat) => chat.id === state.activeChatId)) {
    state.activeChatId = state.chats[0] ? state.chats[0].id : "";
  }
  syncActiveChatState();
}

function saveChatSessions() {
  localStorage.setItem(chatSessionsStorageKey, JSON.stringify(state.chats));
  if (state.activeChatId) {
    localStorage.setItem(activeChatStorageKey, state.activeChatId);
  } else {
    localStorage.removeItem(activeChatStorageKey);
  }
}

function getActiveChatSession() {
  return state.chats.find((chat) => chat.id === state.activeChatId) || null;
}

function syncActiveChatState() {
  const activeChat = getActiveChatSession();
  state.threadId = activeChat ? activeChat.threadId : "";
  state.chatHistory = activeChat ? [...activeChat.messages] : [];
  state.chatStarted = state.chatHistory.length > 0;
  if (state.threadId) {
    localStorage.setItem("iv_agent_thread_id", state.threadId);
  } else {
    localStorage.removeItem("iv_agent_thread_id");
  }
  syncChatStage();
}

function formatChatTime(value) {
  const parsed = new Date(value || "");
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  const now = new Date();
  const elapsedMs = now.getTime() - parsed.getTime();
  if (elapsedMs < 60 * 1000) {
    return "now";
  }
  if (elapsedMs < 60 * 60 * 1000) {
    return `${Math.max(1, Math.floor(elapsedMs / (60 * 1000)))}m`;
  }
  if (elapsedMs < 24 * 60 * 60 * 1000) {
    return `${Math.floor(elapsedMs / (60 * 60 * 1000))}h`;
  }
  if (elapsedMs < 7 * 24 * 60 * 60 * 1000) {
    return `${Math.floor(elapsedMs / (24 * 60 * 60 * 1000))}d`;
  }
  return parsed.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function closeThreadMenus() {
  state.openThreadMenuId = "";
  renderChatList();
}

function renderChatList() {
  if (!elements.chatList) {
    return;
  }

  elements.chatList.innerHTML = "";
  state.chats.forEach((chat) => {
    const item = document.createElement("div");
    item.className = "thread-item";
    item.classList.toggle("is-active", chat.id === state.activeChatId);
    item.classList.toggle("is-menu-open", chat.id === state.openThreadMenuId);
    item.dataset.chatId = chat.id;

    const threadButton = document.createElement("button");
    threadButton.className = "thread-link";
    threadButton.type = "button";
    threadButton.dataset.threadTitle = chat.title;

    const title = document.createElement("span");
    title.className = "thread-title";
    title.textContent = chat.title;

    const time = document.createElement("span");
    time.className = "thread-time";
    time.textContent = formatChatTime(chat.updatedAt);

    threadButton.appendChild(title);
    threadButton.appendChild(time);
    threadButton.addEventListener("click", () => switchChatSession(chat.id));

    const menuButton = document.createElement("button");
    menuButton.className = "thread-menu-button";
    menuButton.type = "button";
    menuButton.setAttribute("aria-label", `Options for ${chat.title}`);
    menuButton.setAttribute("aria-expanded", String(chat.id === state.openThreadMenuId));

    const menuIcon = document.createElement("span");
    menuIcon.className = "material-symbols-outlined";
    menuIcon.textContent = "more_horiz";
    menuButton.appendChild(menuIcon);
    menuButton.addEventListener("click", (event) => {
      event.stopPropagation();
      state.openThreadMenuId = state.openThreadMenuId === chat.id ? "" : chat.id;
      renderChatList();
    });

    const menu = document.createElement("div");
    menu.className = "thread-menu";

    const renameButton = document.createElement("button");
    renameButton.type = "button";
    renameButton.dataset.chatAction = "rename";
    renameButton.innerHTML = '<span class="material-symbols-outlined">edit</span><span>Rename</span>';
    renameButton.addEventListener("click", (event) => {
      event.stopPropagation();
      renameChatSession(chat.id);
    });

    const deleteButton = document.createElement("button");
    deleteButton.type = "button";
    deleteButton.className = "danger-menu-item";
    deleteButton.dataset.chatAction = "delete";
    deleteButton.innerHTML = '<span class="material-symbols-outlined">delete</span><span>Delete</span>';
    deleteButton.addEventListener("click", (event) => {
      event.stopPropagation();
      deleteChatSession(chat.id);
    });

    menu.appendChild(renameButton);
    menu.appendChild(deleteButton);
    item.appendChild(threadButton);
    item.appendChild(menuButton);
    item.appendChild(menu);
    elements.chatList.appendChild(item);
  });
}

function renderActiveChatMessages() {
  if (elements.chatThread) {
    elements.chatThread.innerHTML = "";
  }
  state.chatHistory.forEach((message) => {
    const role = message.role === "assistant" ? "bot" : message.role;
    appendChatMessage(role, message.text);
  });
  syncChatStage();
}

function setActiveChatSession(chatId) {
  if (!state.chats.some((chat) => chat.id === chatId)) {
    return;
  }
  state.activeChatId = chatId;
  state.openThreadMenuId = "";
  syncActiveChatState();
  saveChatSessions();
  renderChatList();
  renderActiveChatMessages();
}

function createNewChatSession() {
  const now = new Date().toISOString();
  const chat = normalizeChatSession({
    id: createChatId(),
    title: "New chat",
    createdAt: now,
    updatedAt: now,
  });
  state.chats.unshift(chat);
  setActiveChatSession(chat.id);
}

function makeChatTitle(text) {
  const normalized = String(text || "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "New chat";
  }
  return normalized.length > 34 ? `${normalized.slice(0, 31)}...` : normalized;
}

function renameChatSession(chatId) {
  const chat = state.chats.find((item) => item.id === chatId);
  if (!chat) {
    return;
  }
  const nextTitle = window.prompt("Rename chat", chat.title);
  if (nextTitle === null) {
    closeThreadMenus();
    return;
  }
  const normalizedTitle = nextTitle.trim();
  if (!normalizedTitle) {
    closeThreadMenus();
    return;
  }
  chat.title = normalizedTitle;
  chat.updatedAt = new Date().toISOString();
  state.openThreadMenuId = "";
  saveChatSessions();
  renderChatList();
  if (chat.id === state.activeChatId && elements.activeViewTitle) {
    elements.activeViewTitle.textContent = chat.title;
  }
}

function deleteChatSession(chatId) {
  const chat = state.chats.find((item) => item.id === chatId);
  if (!chat || !window.confirm(`Delete "${chat.title}"?`)) {
    closeThreadMenus();
    return;
  }

  state.chats = state.chats.filter((item) => item.id !== chatId);
  if (!state.chats.length) {
    state.chats.push(normalizeChatSession({
      id: createChatId(),
      title: "New chat",
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    }));
  }
  if (state.activeChatId === chatId) {
    state.activeChatId = state.chats[0].id;
  }
  state.openThreadMenuId = "";
  syncActiveChatState();
  saveChatSessions();
  renderChatList();
  renderActiveChatMessages();
}

function switchChatSession(chatId) {
  if (state.chatPending || chatId === state.activeChatId) {
    closeThreadMenus();
    return;
  }
  setActiveChatSession(chatId);
  switchAppView("adviser").catch(() => {});
  if (elements.activeViewTitle) {
    const activeChat = getActiveChatSession();
    elements.activeViewTitle.textContent = activeChat ? activeChat.title : appViewTitleMap.adviser;
  }
  if (elements.adviserInput) {
    elements.adviserInput.focus();
  }
}

function syncChatStage() {
  if (elements.adviserShell) {
    elements.adviserShell.classList.toggle("is-welcome", !state.chatStarted);
  }
  document.body.classList.toggle("chat-has-started", state.chatStarted);
}

function markChatStarted() {
  if (state.chatStarted) {
    return;
  }
  state.chatStarted = true;
  syncChatStage();
}

function resetChatSession() {
  const activeChat = getActiveChatSession();
  if (activeChat) {
    activeChat.threadId = "";
    activeChat.messages = [];
    activeChat.updatedAt = new Date().toISOString();
  }
  syncActiveChatState();
  saveChatSessions();
  renderChatList();
  renderActiveChatMessages();
}

function removeChatPendingIndicator() {
  const existingIndicator = document.getElementById("chat-pending-indicator");
  if (existingIndicator) {
    existingIndicator.remove();
  }
}

function renderChatPendingIndicator() {
  if (!elements.chatThread || document.getElementById("chat-pending-indicator")) {
    return;
  }

  const row = document.createElement("div");
  row.id = "chat-pending-indicator";
  row.className = "chat-row bot chat-row-pending";

  const avatar = document.createElement("div");
  avatar.className = "chat-avatar";

  const avatarIcon = document.createElement("span");
  avatarIcon.className = "material-symbols-outlined";
  avatarIcon.textContent = "medical_services";
  avatar.appendChild(avatarIcon);

  const bubble = document.createElement("div");
  bubble.className = "chat-bubble";

  const thinking = document.createElement("div");
  thinking.className = "chat-thinking";

  const label = document.createElement("span");
  label.className = "chat-thinking-label";
  label.textContent = "Thinking";

  const dots = document.createElement("span");
  dots.className = "chat-thinking-dots";
  dots.setAttribute("aria-hidden", "true");

  for (let index = 0; index < 3; index += 1) {
    const dot = document.createElement("span");
    dots.appendChild(dot);
  }

  thinking.appendChild(label);
  thinking.appendChild(dots);
  bubble.appendChild(thinking);
  row.appendChild(avatar);
  row.appendChild(bubble);
  elements.chatThread.appendChild(row);
  scrollChatToBottom();
}

function normalizePolicyCard(rawPolicy) {
  if (!rawPolicy || typeof rawPolicy !== "object") {
    return null;
  }

  const title = String(rawPolicy.title || "").trim();
  const rows = Array.isArray(rawPolicy.rows) ? rawPolicy.rows : [];
  if (!title || !rows.length) {
    return null;
  }

  const normalizedRows = rows
    .map((row) => ({
      label: String((row && row.label) || "").trim(),
      value: String((row && row.value) || "").trim(),
    }))
    .filter((row) => row.label && row.value);

  if (!normalizedRows.length) {
    return null;
  }

  return { title, rows: normalizedRows };
}

function extractWebhookReplyText(payload) {
  if (!payload) {
    return "";
  }

  if (typeof payload === "string") {
    return payload.trim();
  }

  if (Array.isArray(payload)) {
    for (const item of payload) {
      const text = extractWebhookReplyText(item);
      if (text) {
        return text;
      }
    }
    return "";
  }

  if (typeof payload === "object") {
    const keys = ["reply", "response", "message", "text", "output", "answer", "content"];
    for (const key of keys) {
      if (Object.prototype.hasOwnProperty.call(payload, key)) {
        const text = extractWebhookReplyText(payload[key]);
        if (text) {
          return text;
        }
      }
    }

    if (payload.data) {
      const nestedText = extractWebhookReplyText(payload.data);
      if (nestedText) {
        return nestedText;
      }
    }
  }

  return "";
}

function stringifyWebhookPayload(payload) {
  if (!payload) {
    return "";
  }

  if (typeof payload === "string") {
    return payload.trim();
  }

  try {
    return JSON.stringify(payload);
  } catch (error) {
    return "";
  }
}

function extractWebhookPolicyCard(payload) {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const directPolicy = normalizePolicyCard(payload.policy);
  if (directPolicy) {
    return directPolicy;
  }

  if (payload.data && typeof payload.data === "object") {
    return normalizePolicyCard(payload.data.policy);
  }

  return null;
}

function setChatPending(isPending) {
  state.chatPending = isPending;
  if (elements.adviserSendButton) {
    elements.adviserSendButton.disabled = isPending;
  }
  if (elements.adviserCancelButton) {
    elements.adviserCancelButton.classList.toggle("hidden", !isPending);
  }
  if (elements.chatAttachButton) {
    elements.chatAttachButton.disabled = isPending;
  }
  if (elements.chatQrButton) {
    elements.chatQrButton.disabled = isPending;
  }
  if (elements.chatVoiceButton) {
    elements.chatVoiceButton.disabled = isPending || state.chatVoiceProcessing || !isOpenAiConfigured();
  }
  if (elements.chatFileInput) {
    elements.chatFileInput.disabled = isPending;
  }
  if (elements.chatChips) {
    elements.chatChips.forEach((button) => {
      button.disabled = isPending;
    });
  }
  if (isPending) {
    renderChatPendingIndicator();
  } else {
    removeChatPendingIndicator();
  }
}

function cancelPendingAdviserRequest() {
  if (!state.chatAbortController) {
    return;
  }

  state.chatAbortController.abort();
}

function pushChatHistory(role, text) {
  const now = new Date().toISOString();
  const activeChat = getActiveChatSession();
  state.chatHistory.push({ role, text, timestamp: now });
  if (state.chatHistory.length > 30) {
    state.chatHistory = state.chatHistory.slice(-30);
  }
  if (activeChat) {
    if (role === "user" && (!activeChat.messages.length || activeChat.title === "New chat")) {
      activeChat.title = makeChatTitle(text);
    }
    activeChat.messages = [...state.chatHistory];
    activeChat.updatedAt = now;
    saveChatSessions();
    renderChatList();
  }
}

async function confirmPendingAgentAction(actionId, button) {
  if (!actionId || !button || button.disabled) {
    return;
  }
  const originalText = button.textContent;
  button.disabled = true;
  button.textContent = "Bestaetige";
  try {
    const payload = await apiFetch(`/api/agent/actions/${encodeURIComponent(actionId)}/confirm`, {
      method: "POST",
      showLoading: false,
      body: JSON.stringify({
        thread_id: state.threadId,
        profile_id: getActiveProfileId(),
        client_context: {
          profile_id: getActiveProfileId(),
          timezone: getBrowserTimezone(),
          now: new Date().toISOString(),
        },
      }),
    });
    button.textContent = "Bestaetigt";
    if (payload && payload.result) {
      appendChatMessage("bot", "Bestaetigt. Ich habe die Aktion ausgefuehrt.");
    }
    clearCalendarDataCache();
    await Promise.all([refreshAutomations(), refreshDashboardData().catch(() => {})]);
    if (state.calendar && (!payload || payload.calendar_updated !== false)) {
      state.calendar.refetchEvents();
      await refreshCalendarData();
    }
  } catch (error) {
    button.disabled = false;
    button.textContent = originalText;
  }
}

async function submitAdviserPrompt(rawPrompt) {
  const prompt = String(rawPrompt || "").trim();
  if (!prompt || state.chatPending) {
    return;
  }

  markChatStarted();
  const attachmentPayload = getChatAttachmentPayload();
  appendChatMessage("user", prompt, null, { attachments: attachmentPayload });
  pushChatHistory("user", prompt);
  const chatAbortController = new AbortController();
  state.chatAbortController = chatAbortController;
  setChatPending(true);
  if (attachmentPayload.length) {
    setChatAttachmentStatus("Datei wird hochgeladen...");
  }

  try {
    const data = await apiFetch("/api/agent/chat", {
      method: "POST",
      showLoading: false,
      suppressErrorBanner: true,
      signal: chatAbortController.signal,
      body: JSON.stringify({
        message: prompt,
        thread_id: state.threadId,
        attachments: attachmentPayload,
        client_context: {
          active_panel: state.activeAppView === "adviser" ? "" : state.activeAppView,
          current_month: state.currentMonth,
          calendar_view: state.currentView,
          profile_id: getActiveProfileId(),
          timezone: getBrowserTimezone(),
          now: new Date().toISOString(),
        },
        history: state.chatHistory,
      }),
    });

    if (data.thread_id) {
      state.threadId = data.thread_id;
      const activeChat = getActiveChatSession();
      if (activeChat) {
        activeChat.threadId = state.threadId;
        activeChat.updatedAt = new Date().toISOString();
        saveChatSessions();
        renderChatList();
      }
      localStorage.setItem("iv_agent_thread_id", state.threadId);
    }

    const webhookPayload = data.webhook_response || data;
    const webhookText = data.answer || extractWebhookReplyText(webhookPayload);
    const webhookPolicy = extractWebhookPolicyCard(webhookPayload);
    const replyText = webhookText || stringifyWebhookPayload(webhookPayload);
    const replyPolicy = webhookPolicy || null;

    if (!replyText) {
      throw new Error("The agent returned an empty response.");
    }

    if (attachmentPayload.length) {
      setChatAttachmentStatus("Dokument wird analysiert...");
      clearChatAttachments("Zusammenfassung fertig.");
    }
    removeChatPendingIndicator();
    appendChatMessage("bot", replyText, replyPolicy, {
      citations: Array.isArray(data.citations) ? data.citations : [],
      pendingActions: Array.isArray(data.pending_actions)
        ? data.pending_actions
        : (Array.isArray(data.structured_actions) ? data.structured_actions : []),
      toolEvents: Array.isArray(data.tool_events) ? data.tool_events : [],
    });
    pushChatHistory("assistant", replyText);
  } catch (error) {
    if (error.name === "AbortError") {
      return;
    }

    const errorText = String(error && error.message ? error.message : error || "").trim()
      || "Failed to get an agent response.";
    removeChatPendingIndicator();
    appendChatMessage("bot", errorText, null);
    pushChatHistory("assistant", errorText);
  } finally {
    if (state.chatAbortController === chatAbortController) {
      state.chatAbortController = null;
    }
    setChatPending(false);
    if (elements.adviserInput) {
      elements.adviserInput.focus();
    }
  }
}

async function handleAdviserSubmit(event) {
  event.preventDefault();
  if (!elements.adviserInput) {
    return;
  }

  if (state.chatPending) {
    return;
  }

  const prompt = elements.adviserInput.value;
  elements.adviserInput.value = "";
  await submitAdviserPrompt(prompt);
}

function loadCommunityMessages() {
  try {
    const savedMessages = JSON.parse(localStorage.getItem(communityStorageKey) || "[]");
    if (Array.isArray(savedMessages)) {
      state.communityMessages = [...communitySeedMessages, ...savedMessages].filter((message) =>
        message && message.id && message.channel && message.author && message.text
      );
      return;
    }
  } catch (error) {
    /* Fall back to seeded messages. */
  }

  state.communityMessages = [...communitySeedMessages];
}

function saveCommunityMessages() {
  const userMessages = state.communityMessages.filter((message) => !String(message.id).startsWith("seed-"));
  localStorage.setItem(communityStorageKey, JSON.stringify(userMessages));
}

function formatCommunityTime(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }

  return parsed.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function scrollCommunityToBottom() {
  if (!elements.communityThread) {
    return;
  }

  elements.communityThread.scrollTop = elements.communityThread.scrollHeight;
}

function renderCommunityMessages() {
  if (!elements.communityThread) {
    return;
  }

  const channelMessages = state.communityMessages
    .filter((message) => message.channel === state.activeCommunityChannel)
    .sort((left, right) => new Date(left.timestamp) - new Date(right.timestamp));

  if (elements.communityTopicTitle) {
    elements.communityTopicTitle.textContent =
      communityChannelTitleMap[state.activeCommunityChannel] || "Community";
  }
  if (elements.communityCount) {
    elements.communityCount.textContent = `${channelMessages.length} ${channelMessages.length === 1 ? "post" : "posts"}`;
  }

  elements.communityThread.innerHTML = "";
  channelMessages.forEach((message) => {
    const item = document.createElement("article");
    item.className = `community-message${message.isOwn ? " is-own" : ""}`;

    const avatar = document.createElement("div");
    avatar.className = "community-avatar";
    avatar.textContent = String(message.author || "?").trim().slice(0, 1).toUpperCase();

    const bubble = document.createElement("div");
    bubble.className = "community-bubble";

    const header = document.createElement("div");
    header.className = "community-message-header";

    const author = document.createElement("strong");
    author.textContent = message.isOwn ? `${message.author} (you)` : message.author;

    const time = document.createElement("span");
    time.textContent = formatCommunityTime(message.timestamp);

    const body = document.createElement("p");
    body.textContent = message.text;

    header.appendChild(author);
    header.appendChild(time);
    bubble.appendChild(header);
    bubble.appendChild(body);
    item.appendChild(avatar);
    item.appendChild(bubble);
    elements.communityThread.appendChild(item);
  });

  scrollCommunityToBottom();
}

function setCommunityChannel(channel) {
  if (!communityChannelTitleMap[channel]) {
    return;
  }

  state.activeCommunityChannel = channel;
  elements.communityChannelButtons.forEach((button) => {
    const isActive = button.dataset.communityChannel === channel;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", String(isActive));
  });
  renderCommunityMessages();
}

function handleCommunitySubmit(event) {
  event.preventDefault();
  if (!elements.communityInput) {
    return;
  }

  const text = elements.communityInput.value.trim();
  if (!text) {
    return;
  }

  const author = (elements.communityNameInput && elements.communityNameInput.value.trim()) || "You";
  state.communityMessages.push({
    id: `local-${Date.now()}`,
    channel: state.activeCommunityChannel,
    author,
    text,
    timestamp: new Date().toISOString(),
    isOwn: true,
  });
  elements.communityInput.value = "";
  saveCommunityMessages();
  renderCommunityMessages();
}

/* ====================== Automations & Reminders ========================= */

const AUTOMATION_PRESETS = {
  month_end_report: {
    title: "Generate Assistenzbeitrag at month-end",
    schedule: "month_end",
    action: "generate_assistenzbeitrag",
    note: "Auto-generate the Assistenzbeitrag PDF on the last day of every month.",
  },
  weekly_review: {
    title: "Weekly entry review",
    schedule: "weekly_sun",
    action: "notify",
    note: "Sunday evening reminder — review missing assistant entries for the week.",
  },
  custom_reminder: {
    title: "",
    schedule: "once",
    action: "notify",
    note: "",
  },
};

const AUTOMATION_SCHEDULE_LABELS = {
  month_end: "Last day of every month",
  weekly_sun: "Every Sunday",
  weekly_mon: "Every Monday",
  daily: "Every day",
  once: "One-time",
};

const AUTOMATION_ACTION_LABELS = {
  notify: "Reminder",
  generate_assistenzbeitrag: "Generate Assistenzbeitrag",
};

function applyAutomationPreset(presetKey) {
  const preset = AUTOMATION_PRESETS[presetKey];
  if (!preset) return;
  state.automationDraftMode = presetKey;
  if (elements.automationTitleInput) elements.automationTitleInput.value = preset.title;
  if (elements.automationScheduleInput) elements.automationScheduleInput.value = preset.schedule;
  if (elements.automationActionInput) elements.automationActionInput.value = preset.action;
  if (elements.automationNoteInput) elements.automationNoteInput.value = preset.note;
  toggleAutomationDateRow();
  if (elements.automationTitleInput) elements.automationTitleInput.focus();
}

function toggleAutomationDateRow() {
  if (!elements.automationDateRow || !elements.automationScheduleInput) return;
  const isOnce = elements.automationScheduleInput.value === "once";
  elements.automationDateRow.classList.toggle("hidden-field", !isOnce);
}

async function openAutomationsModal(triggerElement) {
  resetAutomationForm();
  await refreshAutomations();
  openModal("automations-modal", triggerElement || document.activeElement);
}

function resetAutomationForm() {
  if (elements.automationForm) elements.automationForm.reset();
  if (elements.automationActionInput) elements.automationActionInput.value = "notify";
  if (elements.automationScheduleInput) elements.automationScheduleInput.value = "month_end";
  if (elements.automationTimeInput) elements.automationTimeInput.value = "09:00";
  toggleAutomationDateRow();
  resetVoiceTranscriptBuffers("automation");
  setVoiceComposerState("automation", "idle");
}

async function refreshAutomations() {
  try {
    const data = await apiFetch("/api/reminders", { showLoading: false });
    state.automations = Array.isArray(data.reminders) ? data.reminders : [];
    state.automationsLoaded = true;
    renderAutomations();
  } catch (error) {
    state.automationsLoaded = true;
    renderAutomations();
  }
}

function renderAutomations() {
  const items = state.automations || [];
  if (elements.automationCountPill) {
    elements.automationCountPill.textContent = String(items.length);
  }
  if (elements.automationPanelCount) {
    elements.automationPanelCount.textContent = String(items.length);
  }
  renderAutomationList(items);
  renderAutomationSummary(items);
}

function renderAutomationList(items) {
  renderAutomationListInto(
    elements.automationList,
    items,
    "No automations yet. Use voice or pick a preset above."
  );
  renderAutomationListInto(
    elements.automationPanelList,
    items,
    "No automations yet. Use the button above to add one."
  );
}

function renderAutomationListInto(container, items, emptyMessage) {
  if (!container) return;
  if (!items.length) {
    container.innerHTML = `<p class="muted-copy">${escapeHtml(emptyMessage)}</p>`;
    return;
  }
  container.innerHTML = "";
  items.forEach((item) => {
    container.appendChild(buildAutomationItemNode(item));
  });
}

function renderAutomationSummary(items) {
  renderAutomationSummaryInto(elements.automationSummary, items, "No automations yet. Tap the dial to add one.");
  renderAutomationSummaryInto(elements.automationPanelSummary, items, "No automations yet.");
}

function renderAutomationSummaryInto(container, items, emptyMessage) {
  if (!container) return;
  if (!items.length) {
    container.innerHTML = `<p class="muted-copy">${escapeHtml(emptyMessage)}</p>`;
    return;
  }
  const top = items.slice(0, 3);
  container.innerHTML = "";
  top.forEach((item) => {
    const node = document.createElement("div");
    node.className = "automation-summary-item";
    node.innerHTML = `
      <span class="material-symbols-outlined">${item.action === "generate_assistenzbeitrag" ? "description" : "alarm"}</span>
      <div>
        <div>${escapeHtml(item.title || "(untitled)")}</div>
        <div class="automation-summary-item-meta">${escapeHtml(formatAutomationMeta(item))}</div>
      </div>`;
    container.appendChild(node);
  });
}

function buildAutomationItemNode(item) {
  const node = document.createElement("div");
  node.className = "automation-item";
  node.innerHTML = `
    <span class="automation-item-icon material-symbols-outlined">${item.action === "generate_assistenzbeitrag" ? "description" : "notifications_active"}</span>
    <div class="automation-item-body">
      <span class="automation-item-title">${escapeHtml(item.title || "(untitled)")}</span>
      <span class="automation-item-meta">${escapeHtml(formatAutomationMeta(item))}</span>
    </div>
    <div class="automation-item-actions">
      <button type="button" data-automation-action="run" title="Run now"><span class="material-symbols-outlined">play_arrow</span></button>
      <button type="button" class="danger" data-automation-action="delete" title="Delete"><span class="material-symbols-outlined">delete</span></button>
    </div>`;
  node.querySelectorAll("[data-automation-action]").forEach((button) => {
    button.addEventListener("click", () => handleAutomationItemAction(button.dataset.automationAction, item.id));
  });
  return node;
}

function formatAutomationMeta(item) {
  const scheduleLabel = AUTOMATION_SCHEDULE_LABELS[item.schedule] || item.schedule;
  const actionLabel = AUTOMATION_ACTION_LABELS[item.action] || item.action;
  const nextRun = item.next_run_at ? ` • Next: ${formatAutomationDate(item.next_run_at)}` : "";
  const time = item.run_time ? ` ${item.run_time}` : "";
  return `${actionLabel} · ${scheduleLabel}${time}${nextRun}`;
}

function formatAutomationDate(value) {
  try {
    return new Date(value).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  } catch (e) {
    return String(value);
  }
}

async function handleAutomationItemAction(action, id) {
  if (!id) return;
  if (action === "delete") {
    if (!window.confirm("Delete this automation?")) return;
    try {
      await apiFetch(`/api/reminders/${encodeURIComponent(id)}`, { method: "DELETE" });
      clearCalendarDataCache();
      await refreshAutomations();
    } catch (error) {
      // showError already shown by apiFetch
    }
    return;
  }
  if (action === "run") {
    try {
      const result = await apiFetch(`/api/reminders/${encodeURIComponent(id)}/run`, { method: "POST" });
      if (result && result.message) {
        showError(result.message);
      }
      await refreshAutomations();
      clearCalendarDataCache();
    } catch (error) {
      // ignore — error already shown
    }
  }
}

async function submitAutomationForm(event) {
  event.preventDefault();
  if (!elements.automationTitleInput || !elements.automationTitleInput.value.trim()) {
    showError("Title is required");
    return;
  }
  const payload = {
    title: elements.automationTitleInput.value.trim(),
    action: elements.automationActionInput ? elements.automationActionInput.value : "notify",
    schedule: elements.automationScheduleInput ? elements.automationScheduleInput.value : "month_end",
    note: elements.automationNoteInput ? elements.automationNoteInput.value.trim() : "",
    run_time: elements.automationTimeInput ? elements.automationTimeInput.value : "09:00",
    run_date: elements.automationDateInput ? elements.automationDateInput.value : "",
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "Europe/Berlin",
  };
  try {
    await apiFetch("/api/reminders", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    clearCalendarDataCache();
    resetAutomationForm();
    await refreshAutomations();
  } catch (error) {
    // ignore — error shown
  }
}

async function submitAutomationVoiceRecording(chunks, mimeType) {
  if (!isOpenAiConfigured()) {
    throw new Error(openAiUnavailableMessage());
  }

  const audioBlob = new Blob(chunks, { type: mimeType || "audio/webm" });
  if (!audioBlob.size) {
    throw new Error("No voice recording was captured.");
  }
  const formData = new FormData();
  const extension = audioBlob.type.includes("mp4") ? "mp4" : "webm";
  formData.append("audio", audioBlob, `automation-voice.${extension}`);
  formData.append("timezone", Intl.DateTimeFormat().resolvedOptions().timeZone || "Europe/Berlin");
  formData.append("now", new Date().toISOString());
  state.automationVoiceProcessing = true;
  setVoiceComposerState("automation", "processing");
  try {
    const response = await fetch("/api/reminders/voice", {
      method: "POST",
      body: formData,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload.error || "Voice automation request failed.");
    }
    if (payload.transcript) {
      setVoiceTranscript("automation", payload.transcript, false);
    }
    if (payload.draft) {
      const draft = payload.draft;
      if (elements.automationTitleInput && draft.title) elements.automationTitleInput.value = draft.title;
      if (elements.automationActionInput && draft.action) elements.automationActionInput.value = draft.action;
      if (elements.automationScheduleInput && draft.schedule) elements.automationScheduleInput.value = draft.schedule;
      if (elements.automationNoteInput && draft.note) elements.automationNoteInput.value = draft.note;
      if (elements.automationTimeInput && draft.run_time) elements.automationTimeInput.value = draft.run_time;
      if (elements.automationDateInput && draft.run_date) elements.automationDateInput.value = draft.run_date;
      toggleAutomationDateRow();
    }
    if (payload.created) {
      clearCalendarDataCache();
      await refreshAutomations();
    }
    if (elements.automationVoiceStatus) {
      elements.automationVoiceStatus.textContent = payload.created
        ? "Saved automatically — review below."
        : "Draft prefilled. Adjust and save.";
    }
  } finally {
    state.automationVoiceProcessing = false;
    setVoiceComposerState("automation", "idle");
  }
}

async function tickAutomationsLazy() {
  try {
    await apiFetch("/api/reminders/tick", { method: "POST", showLoading: false });
    clearCalendarDataCache();
  } catch (error) {
    // silent — tick errors should not disturb UI
  }
}

async function refreshStorageBrowser(options = {}) {
  if (!elements.storageBucketList || !elements.storageFileList) {
    return;
  }
  if (!options.force && state.storageBrowser) {
    renderStorageBrowser();
    return;
  }
  if (elements.storageBrowserStatus) {
    elements.storageBrowserStatus.textContent = "Loading storage...";
    elements.storageBrowserStatus.dataset.variant = "";
  }
  try {
    const data = await apiFetch("/api/storage/browser", { showLoading: false });
    state.storageBrowser = data;
    if (!state.activeStorageBucket && data.buckets && data.buckets.length) {
      state.activeStorageBucket = data.buckets[0].id || data.buckets[0].name;
    }
    renderStorageBrowser();
  } catch (error) {
    if (elements.storageBrowserStatus) {
      elements.storageBrowserStatus.textContent = error.message || "Storage could not be loaded.";
      elements.storageBrowserStatus.dataset.variant = "error";
    }
  }
}

function renderStorageBrowser() {
  const data = state.storageBrowser || {};
  const buckets = Array.isArray(data.buckets) ? data.buckets : [];
  if (elements.storageBrowserStatus) {
    if (!data.configured) {
      elements.storageBrowserStatus.textContent = data.message || "Supabase Storage is not configured.";
      elements.storageBrowserStatus.dataset.variant = "muted";
    } else {
      const fileTotal = buckets.reduce((total, bucket) => total + Number(bucket.file_count || 0), 0);
      elements.storageBrowserStatus.textContent = `${buckets.length} bucket${buckets.length === 1 ? "" : "s"} - ${fileTotal} file${fileTotal === 1 ? "" : "s"}`;
      elements.storageBrowserStatus.dataset.variant = "";
    }
  }

  if (!buckets.length) {
    if (elements.storageBucketList) {
      elements.storageBucketList.innerHTML = "";
    }
    if (elements.storageActiveBucket) {
      elements.storageActiveBucket.textContent = "No bucket selected";
    }
    if (elements.storageFileCount) {
      elements.storageFileCount.textContent = "0 files";
    }
    if (elements.storageFileList) {
      elements.storageFileList.innerHTML = `<div class="storage-empty">No buckets or files available.</div>`;
    }
    return;
  }

  const activeBucket = buckets.find((bucket) => (bucket.id || bucket.name) === state.activeStorageBucket) || buckets[0];
  state.activeStorageBucket = activeBucket.id || activeBucket.name;
  if (elements.storageBucketList) {
    elements.storageBucketList.innerHTML = buckets.map((bucket) => renderStorageBucketButton(bucket, activeBucket)).join("");
    elements.storageBucketList.querySelectorAll("[data-storage-bucket]").forEach((button) => {
      button.addEventListener("click", () => {
        state.activeStorageBucket = button.dataset.storageBucket || "";
        renderStorageBrowser();
      });
    });
  }

  const objects = Array.isArray(activeBucket.objects) ? activeBucket.objects : [];
  const files = objects.filter((item) => item.type === "file");
  if (elements.storageActiveBucket) {
    elements.storageActiveBucket.textContent = activeBucket.name || activeBucket.id || "Bucket";
  }
  if (elements.storageFileCount) {
    elements.storageFileCount.textContent = `${files.length} file${files.length === 1 ? "" : "s"}`;
  }
  if (elements.storageFileList) {
    elements.storageFileList.innerHTML = objects.length
      ? objects.map(renderStorageObject).join("")
      : `<div class="storage-empty">This bucket is empty.</div>`;
  }
}

function renderStorageBucketButton(bucket, activeBucket) {
  const bucketId = bucket.id || bucket.name || "";
  const activeId = activeBucket.id || activeBucket.name || "";
  const fileCount = Number(bucket.file_count || 0);
  const isActive = bucketId === activeId;
  return `
    <button class="storage-bucket-button${isActive ? " is-active" : ""}" type="button" data-storage-bucket="${escapeHtml(bucketId)}">
      <span class="material-symbols-outlined">database</span>
      <span class="storage-bucket-copy">
        <strong>${escapeHtml(bucket.name || bucketId || "Bucket")}</strong>
        <span>${fileCount} file${fileCount === 1 ? "" : "s"}${bucket.public ? " - public" : ""}</span>
      </span>
    </button>
  `;
}

function renderStorageObject(item) {
  const isFolder = item.type === "folder";
  const label = item.name || item.path || "Object";
  const meta = isFolder
    ? "Folder"
    : [formatFileSize(item.size), item.content_type, formatStorageTime(item.updated_at)].filter(Boolean).join(" - ");
  return `
    <article class="storage-file-row">
      <span class="storage-file-icon material-symbols-outlined">${isFolder ? "folder" : "draft"}</span>
      <span class="storage-file-copy">
        <strong>${escapeHtml(label)}</strong>
        <span>${escapeHtml(item.path || "")}</span>
        <small>${escapeHtml(meta || "Stored object")}</small>
      </span>
    </article>
  `;
}

function formatStorageTime(value) {
  if (!value) {
    return "";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return parsed.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function bindEvents() {
  if (elements.sidebarToggle) {
    elements.sidebarToggle.addEventListener("click", toggleSidebar);
  }
  if (elements.inspectorToggle) {
    elements.inspectorToggle.addEventListener("click", toggleInspector);
  }
  if (elements.storageToggle) {
    elements.storageToggle.addEventListener("click", () => toggleWorkspacePanel("reports"));
  }
  if (elements.automationsToggle) {
    elements.automationsToggle.addEventListener("click", () => toggleWorkspacePanel("automations"));
  }
  if (elements.refreshStorageBrowserButton) {
    elements.refreshStorageBrowserButton.addEventListener("click", () => refreshStorageBrowser({ force: true }));
  }
  document.getElementById("prev-month").addEventListener("click", () => navigatePeriod(-1));
  document.getElementById("next-month").addEventListener("click", () => navigatePeriod(1));
  const openAddModalButton = document.getElementById("open-add-modal");
  if (openAddModalButton) {
    openAddModalButton.addEventListener("click", (event) => openAddEventModal(event.currentTarget));
  }
  if (elements.voiceComposerButton) {
    elements.voiceComposerButton.addEventListener("click", handleEventVoiceButtonClick);
  }
  if (elements.automationVoiceButton) {
    elements.automationVoiceButton.addEventListener("click", handleAutomationVoiceButtonClick);
  }
  if (elements.openAutomationsCardButton) {
    elements.openAutomationsCardButton.addEventListener("click", () => openAutomationsModal(elements.openAutomationsCardButton));
  }
  if (elements.quickAddAutomationButton) {
    elements.quickAddAutomationButton.addEventListener("click", () => openAutomationsModal(elements.quickAddAutomationButton));
  }
  if (elements.panelNewAutomationButton) {
    elements.panelNewAutomationButton.addEventListener("click", () => openAutomationsModal(elements.panelNewAutomationButton));
  }
  if (elements.automationForm) {
    elements.automationForm.addEventListener("submit", submitAutomationForm);
  }
  if (elements.automationScheduleInput) {
    elements.automationScheduleInput.addEventListener("change", toggleAutomationDateRow);
  }
  if (elements.automationPresetButtons) {
    elements.automationPresetButtons.forEach((button) => {
      button.addEventListener("click", () => applyAutomationPreset(button.dataset.preset));
    });
  }
  document.getElementById("open-export-modal").addEventListener("click", openExportModal);
  document.getElementById("copy-export").addEventListener("click", copyExportSummary);

  elements.viewButtons.forEach((button) => {
    button.addEventListener("click", () => changeCalendarView(button.dataset.view));
  });

  elements.navLinks.forEach((button) => {
    button.addEventListener("click", () => {
      if (!button.dataset.viewTarget) {
        return;
      }
      switchAppView(button.dataset.viewTarget).catch(() => {});
    });
  });
  if (elements.newThreadButton) {
    elements.newThreadButton.addEventListener("click", () => {
      createNewChatSession();
      switchAppView("adviser").catch(() => {});
      if (elements.activeViewTitle) {
        elements.activeViewTitle.textContent = appViewTitleMap.adviser;
      }
      if (elements.adviserInput) {
        elements.adviserInput.focus();
      }
    });
  }
  if (elements.settingsButton) {
    elements.settingsButton.addEventListener("click", () => {
      switchAppView("settings").catch(() => {});
    });
  }
  if (elements.profileForm) {
    elements.profileForm.addEventListener("submit", submitProfileForm);
  }
  if (elements.reloadProfileButton) {
    elements.reloadProfileButton.addEventListener("click", () => {
      loadProfileForm({ force: true }).catch(() => {});
    });
  }

  elements.chatChips.forEach((button) => {
    button.addEventListener("click", () => {
      submitAdviserPrompt(button.dataset.chatPrompt || "").catch(() => {});
    });
  });

  elements.communityChannelButtons.forEach((button) => {
    button.addEventListener("click", () => {
      setCommunityChannel(button.dataset.communityChannel || "daily");
    });
  });

  if (elements.communityForm) {
    elements.communityForm.addEventListener("submit", handleCommunitySubmit);
  }

  if (elements.allDayInput) {
    elements.allDayInput.addEventListener("change", toggleAllDayFields);
  }
  elements.categoryField.addEventListener("change", toggleAssistantFields);
  elements.transportModeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (elements.transportModeInput) {
        elements.transportModeInput.value = button.dataset.transportMode || "bus_bahn";
      }
      updateTransportModeButtons();
    });
  });
  elements.timeInput.addEventListener("change", () => syncEndTimeWithStart(true));
  elements.recurrenceField.addEventListener("change", toggleRepeatCountField);
  elements.reportTypeButtons.forEach((button) => {
    button.addEventListener("click", () => {
      const reportType = button.dataset.reportType;
      const nextSelection = getSelectedReportTypes();
      const typeIndex = nextSelection.indexOf(reportType);

      if (typeIndex >= 0) {
        nextSelection.splice(typeIndex, 1);
      } else {
        nextSelection.push(reportType);
      }

      if (!nextSelection.length) {
        nextSelection.push("assistenzbeitrag");
      }

      setSelectedReportTypes(nextSelection);
    });
  });
  elements.addForm.addEventListener("submit", submitAddEvent);
  elements.reportForm.addEventListener("submit", submitReportForm);
  if (elements.sendReport) {
    elements.sendReport.addEventListener("click", sendGeneratedReport);
  }
  if (elements.generateNewReport) {
    elements.generateNewReport.addEventListener("click", resetReportWorkflow);
  }
  elements.confirmDelete.addEventListener("click", confirmDelete);
  elements.cancelDelete.addEventListener("click", closeDeletePopover);
  if (elements.editEvent) {
    elements.editEvent.addEventListener("click", openEditEventModal);
  }
  elements.generateReport.addEventListener("click", () => openReportModal(elements.generateReport));

  if (elements.dashboardOpenCalendar) {
    elements.dashboardOpenCalendar.addEventListener("click", () => {
      switchAppView("calendar").catch(() => {});
    });
  }

  if (elements.dashboardOpenChat) {
    elements.dashboardOpenChat.addEventListener("click", () => {
      switchAppView("adviser").catch(() => {});
    });
  }

  if (elements.adviserForm) {
    elements.adviserForm.addEventListener("submit", handleAdviserSubmit);
  }

  if (elements.chatAttachButton && elements.chatFileInput) {
    elements.chatAttachButton.addEventListener("click", () => elements.chatFileInput.click());
    elements.chatFileInput.addEventListener("change", () => {
      addChatAttachmentFiles(elements.chatFileInput.files).catch((error) => {
        setChatAttachmentStatus(error.message || "Could not attach files.", "error");
      });
    });
  }

  if (elements.chatQrButton) {
    elements.chatQrButton.addEventListener("click", () => {
      openChatQrModal(elements.chatQrButton).catch(() => {});
    });
  }

  if (elements.chatVoiceButton) {
    elements.chatVoiceButton.addEventListener("click", handleChatVoiceButtonClick);
  }

  if (elements.chatAttachmentList) {
    elements.chatAttachmentList.addEventListener("click", handleChatAttachmentListClick);
  }

  if (elements.adviserShell) {
    ["dragenter", "dragover", "dragleave", "drop"].forEach((eventName) => {
      elements.adviserShell.addEventListener(eventName, handleChatDragEvent);
    });
  }

  if (elements.adviserCancelButton) {
    elements.adviserCancelButton.addEventListener("click", cancelPendingAdviserRequest);
  }

  elements.monthPicker.addEventListener("change", (event) => {
    if (!event.target.value) {
      return;
    }
    state.currentMonth = event.target.value;
    syncMonthUi();
    if (state.calendar) {
      state.calendar.gotoDate(`${state.currentMonth}-01`);
    }
    Promise.all([refreshSidebarData(), refreshDashboardData()]).catch(() => {});
  });

  document.addEventListener("click", handleDocumentClick);
  document.addEventListener("keydown", handleKeydown);
  document.addEventListener("dragover", handleGlobalFileDragOver);
  document.addEventListener("drop", handleGlobalFileDrop);
  document.addEventListener("dragend", resetChatDragState);
  window.addEventListener("blur", resetChatDragState);
  wireModalClosers();
}

async function initialize() {
  syncMonthUi();
  updateViewButtons();
  seedFormDefaults();
  loadCommunityMessages();
  setCommunityChannel(state.activeCommunityChannel);
  loadChatSessions();
  renderChatList();
  renderActiveChatMessages();
  syncChatStage();
  bindEvents();
  setSelectedReportTypes(state.selectedReportTypes);
  setChatPending(false);
  syncEndTimeWithStart(true);
  initInvoices();
  await refreshAiStatus();
  await switchAppView(state.activeAppView);
  tickAutomationsLazy();
  refreshAutomations().catch(() => {});
}

function initInvoices() {
  const qrImg = document.getElementById("invoices-qr");
  const list = document.getElementById("invoices-list");
  const count = document.getElementById("invoices-count");
  const countLabel = document.getElementById("invoices-count-label");
  const cameraLink = document.getElementById("invoices-camera-link");
  const dropZone = document.getElementById("invoice-drop-zone");
  const fileInput = document.getElementById("invoice-file-input");
  const fileButton = document.getElementById("invoice-file-button");
  const uploadStatus = document.getElementById("invoice-upload-status");
  if (!qrImg || !list || !count) return;

  const sid = getInvoicesSessionId();

  fetch(`/api/invoices/${sid}/scan-url`)
    .then((r) => r.json())
    .then((data) => {
      const url = data.camera_url || data.scan_url;
      qrImg.src = `https://api.qrserver.com/v1/create-qr-code/?size=420x420&margin=8&data=${encodeURIComponent(url)}`;
      qrImg.title = url;
      if (cameraLink) {
        cameraLink.href = url;
        cameraLink.title = url;
      }
    })
    .catch(() => {});

  function setUploadStatus(message, variant = "") {
    if (!uploadStatus) {
      return;
    }
    uploadStatus.textContent = message;
    uploadStatus.dataset.variant = variant;
  }

  function fileToBase64(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => {
        const result = String(reader.result || "");
        resolve(result.includes(",") ? result.split(",").pop() : result);
      };
      reader.onerror = () => reject(reader.error || new Error("Could not read file"));
      reader.readAsDataURL(file);
    });
  }

  async function uploadFiles(fileList) {
    const files = Array.from(fileList || []).filter((file) =>
      file.type.startsWith("image/") || file.type === "application/pdf"
    );

    if (!files.length) {
      setUploadStatus("Only images and PDFs are supported.", "error");
      return;
    }

    setUploadStatus(`Uploading ${files.length} document${files.length === 1 ? "" : "s"}...`);

    try {
      for (const file of files) {
        const imageBase64 = await fileToBase64(file);
        const response = await fetch(`/api/invoices/${encodeURIComponent(sid)}/capture`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            image_base64: imageBase64,
            mime: file.type || "application/octet-stream",
            file_name: file.name,
          }),
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(payload.error || `Upload failed for ${file.name}`);
        }
      }

      setUploadStatus("Upload saved to storage.", "success");
      await poll();
    } catch (error) {
      setUploadStatus(error.message || "Upload failed.", "error");
    } finally {
      if (fileInput) {
        fileInput.value = "";
      }
    }
  }

  if (fileButton && fileInput) {
    fileButton.addEventListener("click", () => fileInput.click());
    fileInput.addEventListener("change", () => uploadFiles(fileInput.files));
  }

  if (dropZone) {
    ["dragenter", "dragover"].forEach((eventName) => {
      dropZone.addEventListener(eventName, (event) => {
        event.preventDefault();
        dropZone.classList.add("is-dragging");
      });
    });

    ["dragleave", "drop"].forEach((eventName) => {
      dropZone.addEventListener(eventName, (event) => {
        event.preventDefault();
        dropZone.classList.remove("is-dragging");
      });
    });

    dropZone.addEventListener("drop", (event) => uploadFiles(event.dataTransfer.files));
    dropZone.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        if (fileInput) {
          fileInput.click();
        }
      }
    });
  }

  async function poll() {
    try {
      const r = await fetch(`/api/invoices/${sid}`);
      const data = await r.json();
      const items = data.captures || [];
      count.textContent = String(items.length);
      if (countLabel) {
        countLabel.textContent = items.length === 1 ? "document" : "documents";
      }
      if (!items.length) {
        list.innerHTML = `<div class="documents-empty">No documents yet. Upload files on the left or scan the QR code on the right.</div>`;
        return;
      }
      list.innerHTML = items
        .map((capture, index) => renderInvoiceCapture(capture, index))
        .join("");
    } catch (err) {
      /* ignore */
    }
  }
  poll();
  setInterval(poll, 2500);
}

function renderInvoiceCapture(capture, index) {
  const isImage = String(capture.content_type || "").startsWith("image/");
  const fileUrl = capture.file_url || capture.image_url || "#";
  const summary = capture.summary || (isImage ? "Receipt image saved to storage." : "Document saved to storage.");
  const storageLabels = {
    supabase: "Supabase Storage",
    postgres: "Supabase DB",
    local: "Local",
  };
  const storageLabel = capture.storage_bucket || storageLabels[capture.storage_backend] || capture.storage_backend || "Local";
  const extractionNote = capture.extraction_error
    ? `<div class="invoice-note">${escapeHtml(capture.extraction_error)}</div>`
    : "";
  const preview = isImage
    ? `<img class="document-thumb" src="${escapeHtml(fileUrl)}" alt="${escapeHtml(capture.file_name || `Document ${index + 1}`)}" loading="lazy" />`
    : `<span class="document-file-icon material-symbols-outlined">picture_as_pdf</span>`;

  return `
    <article class="document-row" role="row">
      <div class="document-name-cell" role="cell">
        <a class="document-thumb-link" href="${escapeHtml(fileUrl)}" target="_blank" rel="noreferrer">
          ${preview}
        </a>
        <div class="document-copy">
          <span class="invoice-title">${escapeHtml(capture.file_name || `Document ${index + 1}`)}</span>
          <div class="invoice-summary">${escapeHtml(summary)}</div>
          ${extractionNote}
        </div>
      </div>
      <span role="cell">${escapeHtml(formatInvoiceCaptureTime(capture.created_at))}</span>
      <span role="cell">${escapeHtml(formatFileSize(capture.content_size) || "-")}</span>
      <span role="cell"><span class="storage-badge">${escapeHtml(storageLabel)}</span></span>
      <span role="cell"><a class="document-open-link" href="${escapeHtml(fileUrl)}" target="_blank" rel="noreferrer">Open</a></span>
    </article>
  `;
}

function formatInvoiceCaptureTime(value) {
  if (!value) {
    return "Unknown upload time";
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return parsed.toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  });
}

function formatFileSize(bytes) {
  const size = Number(bytes || 0);
  if (!size) {
    return "";
  }
  if (size < 1024) {
    return `${size} B`;
  }
  if (size < 1024 * 1024) {
    return `${(size / 1024).toFixed(1)} KB`;
  }
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
}

initialize().catch((error) => {
  showError(error.message || "Failed to initialize application");
});
