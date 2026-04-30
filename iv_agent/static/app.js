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
  adviser: "IV-Adviser",
  community: "Community",
  reports: "File Storage",
  settings: "Settings",
};

const communityChannelTitleMap = {
  daily: "Daily Support",
  reimbursement: "Reimbursements",
  therapy: "School & Therapy",
};

const communityStorageKey = "iv_helper_parent_community";

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
  activeAppView: "dashboard",
  activeCommunityChannel: "daily",
  calendar: null,
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
  notificationsOpen: false,
  communityMessages: [],
  aiStatus: null,
  profile: null,
  profileLoaded: false,
};

const elements = {
  monthPicker: document.getElementById("month-picker"),
  heading: document.getElementById("calendar-heading"),
  hoursValue: document.getElementById("hours-value"),
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
  notificationButton: document.getElementById("open-notifications-popover"),
  notificationsPopover: document.getElementById("notifications-popover"),
  notificationsList: document.getElementById("notifications-list"),
  openAutomationsCardButton: document.getElementById("open-automations-card"),
  quickAddAutomationButton: document.getElementById("quick-add-automation"),
  automationsPill: document.getElementById("automations-pill"),
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
  adviserForm: document.getElementById("adviser-form"),
  adviserInput: document.getElementById("adviser-input"),
  adviserSendButton: document.getElementById("adviser-send"),
  adviserCancelButton: document.getElementById("adviser-cancel"),
  chatThread: document.getElementById("chat-thread"),
  chatChips: document.querySelectorAll(".chat-chip"),
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

  [elements.voiceComposerButton, elements.automationVoiceButton].forEach((button) => {
    if (!button) {
      return;
    }
    button.disabled = !configured;
    button.title = configured ? button.getAttribute("aria-label") || "" : message;
  });

  if (!configured) {
    if (elements.voiceComposerStatus) {
      elements.voiceComposerStatus.textContent = message;
    }
    if (elements.automationVoiceStatus) {
      elements.automationVoiceStatus.textContent = message;
    }
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
  const data = await apiFetch(`/api/hours?month=${encodeURIComponent(state.currentMonth)}`);
  elements.hoursValue.textContent = Number(data.total_hours).toFixed(2);
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
  const data = await apiFetch(`/api/events?month=${encodeURIComponent(todayMonth)}`);
  renderFocusSummary(data.events || []);
}

async function refreshSidebarData() {
  syncMonthUi();
  await Promise.all([refreshHours(), refreshFocusSummary()]);
}

async function refreshDashboardData() {
  const [hoursData, eventsData] = await Promise.all([
    apiFetch(`/api/hours?month=${encodeURIComponent(state.currentMonth)}`),
    apiFetch(`/api/events?month=${encodeURIComponent(state.currentMonth)}`),
  ]);

  const monthEvents = eventsData.events || [];
  const assistantEvents = monthEvents.filter((event) => event.category === "assistant");

  if (elements.dashboardHoursValue) {
    elements.dashboardHoursValue.textContent = Number(hoursData.total_hours || 0).toFixed(2);
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
      months.map((month) => apiFetch(`/api/events?month=${encodeURIComponent(month)}`))
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

async function refreshCalendarData() {
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
  state.activeAppView = viewName;

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
    section.classList.toggle("is-active", section.dataset.view === viewName);
  });

  if (elements.activeViewTitle) {
    elements.activeViewTitle.textContent = appViewTitleMap[viewName] || "IV-Helper";
  }
  if (elements.calendarToolbar) {
    elements.calendarToolbar.classList.toggle("hidden", viewName !== "calendar");
  }
  if (elements.defaultToolbar) {
    elements.defaultToolbar.classList.toggle("hidden", viewName === "calendar");
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
  await refreshCalendarData();
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
}

function setVoiceTranscript(target, text, isInterim) {
  const ctx = getVoiceContext(target);
  if (!ctx.transcriptEl) {
    return;
  }
  ctx.transcriptEl.textContent = text || "";
  ctx.transcriptEl.classList.toggle("is-interim", Boolean(isInterim));
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
      let finalText = target === "automation" ? state.automationFinalTranscript : state.voiceFinalTranscript;
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
      if (target === "automation") {
        state.automationFinalTranscript = finalText;
        state.automationInterimTranscript = interimText;
      } else {
        state.voiceFinalTranscript = finalText;
        state.voiceInterimTranscript = interimText;
      }
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
  if (target === "automation") {
    if (state.automationLiveRecognition) {
      try { state.automationLiveRecognition.stop(); } catch (e) { /* ignore */ }
      state.automationLiveRecognition = null;
    }
  } else if (state.voiceLiveRecognition) {
    try { state.voiceLiveRecognition.stop(); } catch (e) { /* ignore */ }
    state.voiceLiveRecognition = null;
  }
}

function resetVoiceTranscriptBuffers(target) {
  if (target === "automation") {
    state.automationFinalTranscript = "";
    state.automationInterimTranscript = "";
  } else {
    state.voiceFinalTranscript = "";
    state.voiceInterimTranscript = "";
  }
  setVoiceTranscript(target, "", false);
}

function stopVoiceStream(target) {
  const streamKey = target === "automation" ? "automationVoiceStream" : "voiceStream";
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

  const recorderKey = target === "automation" ? "automationVoiceRecorder" : "voiceRecorder";
  const streamKey = target === "automation" ? "automationVoiceStream" : "voiceStream";
  const chunksKey = target === "automation" ? "automationVoiceChunks" : "voiceChunks";
  const recogKey = target === "automation" ? "automationLiveRecognition" : "voiceLiveRecognition";

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
  const recorderKey = target === "automation" ? "automationVoiceRecorder" : "voiceRecorder";
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
  await refreshCalendarData();
}

function handleDocumentClick(event) {
  const clickInsidePopover = elements.deletePopover.contains(event.target);
  const clickInsideEvent = event.target.closest(".fc-event");
  if (!clickInsidePopover && !clickInsideEvent) {
    closeDeletePopover();
  }
  const clickInsideNotifications = event.target.closest(".notification-wrap");
  if (!clickInsideNotifications) {
    closeNotificationsPopover();
  }
}

function handleKeydown(event) {
  if (event.key === "Escape") {
    if (state.notificationsOpen) {
      closeNotificationsPopover();
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
    height: "auto",
    initialDate: `${state.currentMonth}-01`,
    nowIndicator: true,
    allDaySlot: true,
    slotMinTime: "06:00:00",
    slotMaxTime: "22:00:00",
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

function appendChatMessage(role, messageText, policyCard = null) {
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

  const message = document.createElement("p");
  message.textContent = messageText;
  bubble.appendChild(message);

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

  const meta = document.createElement("span");
  meta.className = "chat-meta";
  meta.textContent = role === "user" ? "You" : "IV-Helper";
  bubble.appendChild(meta);

  row.appendChild(avatar);
  row.appendChild(bubble);
  elements.chatThread.appendChild(row);
  scrollChatToBottom();
}

function scrollChatToBottom() {
  if (!elements.chatThread) {
    return;
  }

  elements.chatThread.scrollTop = elements.chatThread.scrollHeight;
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
  state.chatHistory.push({ role, text, timestamp: new Date().toISOString() });
  if (state.chatHistory.length > 30) {
    state.chatHistory = state.chatHistory.slice(-30);
  }
}

async function submitAdviserPrompt(rawPrompt) {
  const prompt = String(rawPrompt || "").trim();
  if (!prompt || state.chatPending) {
    return;
  }

  appendChatMessage("user", prompt);
  pushChatHistory("user", prompt);
  const chatAbortController = new AbortController();
  state.chatAbortController = chatAbortController;
  setChatPending(true);

  try {
    const data = await apiFetch("/api/chat", {
      method: "POST",
      showLoading: false,
      suppressErrorBanner: true,
      signal: chatAbortController.signal,
      body: JSON.stringify({
        message: prompt,
        history: state.chatHistory,
      }),
    });

    const webhookPayload = data.webhook_response || data;
    const webhookText = extractWebhookReplyText(webhookPayload);
    const webhookPolicy = extractWebhookPolicyCard(webhookPayload);
    const replyText = webhookText || stringifyWebhookPayload(webhookPayload);
    const replyPolicy = webhookPolicy || null;

    if (!replyText) {
      throw new Error("n8n returned an empty response.");
    }

    removeChatPendingIndicator();
    appendChatMessage("bot", replyText, replyPolicy);
    pushChatHistory("assistant", replyText);
  } catch (error) {
    if (error.name === "AbortError") {
      return;
    }

    const errorText = String(error && error.message ? error.message : error || "").trim()
      || "Failed to get a response from n8n.";
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
    const data = await apiFetch("/api/reminders");
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
  if (elements.automationsPill) {
    elements.automationsPill.textContent = String(items.length);
    elements.automationsPill.classList.toggle("hidden", items.length === 0);
  }
  renderAutomationList(items);
  renderAutomationSummary(items);
  renderNotifications(items);
}

function closeNotificationsPopover() {
  state.notificationsOpen = false;
  if (elements.notificationsPopover) {
    elements.notificationsPopover.classList.add("hidden");
  }
  if (elements.notificationButton) {
    elements.notificationButton.setAttribute("aria-expanded", "false");
  }
}

async function toggleNotificationsPopover() {
  if (!elements.notificationsPopover || !elements.notificationButton) {
    return;
  }
  if (state.notificationsOpen) {
    closeNotificationsPopover();
    return;
  }
  if (!state.automationsLoaded) {
    await refreshAutomations();
  } else {
    renderNotifications(state.automations || []);
  }
  state.notificationsOpen = true;
  elements.notificationsPopover.classList.remove("hidden");
  elements.notificationButton.setAttribute("aria-expanded", "true");
}

function renderNotifications(items) {
  if (!elements.notificationsList) {
    return;
  }
  const notifications = (items || []).filter((item) => item.status !== "completed").slice(0, 5);
  if (!notifications.length) {
    elements.notificationsList.innerHTML = '<p class="muted-copy">No notifications right now.</p>';
    return;
  }
  elements.notificationsList.innerHTML = "";
  notifications.forEach((item) => {
    const node = document.createElement("article");
    node.className = "notification-item";
    node.innerHTML = `
      <span class="notification-icon material-symbols-outlined">${item.action === "generate_assistenzbeitrag" ? "description" : "notifications_active"}</span>
      <div class="notification-body">
        <span class="notification-title">${escapeHtml(item.title || "Reminder")}</span>
        <span class="notification-meta">${escapeHtml(formatNotificationMeta(item))}</span>
      </div>`;
    elements.notificationsList.appendChild(node);
  });
}

function formatNotificationMeta(item) {
  const nextRun = item.next_run_at ? `Next: ${formatAutomationDate(item.next_run_at)}` : "No scheduled time";
  const note = item.note ? ` - ${item.note}` : "";
  return `${nextRun}${note}`;
}

function renderAutomationList(items) {
  if (!elements.automationList) return;
  if (!items.length) {
    elements.automationList.innerHTML = '<p class="muted-copy">No automations yet. Use voice or pick a preset above.</p>';
    return;
  }
  elements.automationList.innerHTML = "";
  items.forEach((item) => {
    elements.automationList.appendChild(buildAutomationItemNode(item));
  });
}

function renderAutomationSummary(items) {
  if (!elements.automationSummary) return;
  if (!items.length) {
    elements.automationSummary.innerHTML = '<p class="muted-copy">No automations yet. Tap the dial to add one.</p>';
    return;
  }
  const top = items.slice(0, 3);
  elements.automationSummary.innerHTML = "";
  top.forEach((item) => {
    const node = document.createElement("div");
    node.className = "automation-summary-item";
    node.innerHTML = `
      <span class="material-symbols-outlined">${item.action === "generate_assistenzbeitrag" ? "description" : "alarm"}</span>
      <div>
        <div>${escapeHtml(item.title || "(untitled)")}</div>
        <div class="automation-summary-item-meta">${escapeHtml(formatAutomationMeta(item))}</div>
      </div>`;
    elements.automationSummary.appendChild(node);
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
    await apiFetch("/api/reminders/tick", { method: "POST" });
  } catch (error) {
    // silent — tick errors should not disturb UI
  }
}

function bindEvents() {
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
  if (elements.notificationButton) {
    elements.notificationButton.addEventListener("click", (event) => {
      event.stopPropagation();
      toggleNotificationsPopover().catch(() => {});
    });
  }
  if (elements.openAutomationsCardButton) {
    elements.openAutomationsCardButton.addEventListener("click", () => openAutomationsModal(elements.openAutomationsCardButton));
  }
  if (elements.quickAddAutomationButton) {
    elements.quickAddAutomationButton.addEventListener("click", () => openAutomationsModal(elements.quickAddAutomationButton));
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
      switchAppView(button.dataset.viewTarget).catch(() => {});
    });
  });
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
  wireModalClosers();
}

async function initialize() {
  syncMonthUi();
  updateViewButtons();
  seedFormDefaults();
  loadCommunityMessages();
  setCommunityChannel(state.activeCommunityChannel);
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

  let sid = localStorage.getItem("invoices_sid");
  if (!sid) {
    sid = Math.random().toString(36).slice(2, 10);
    localStorage.setItem("invoices_sid", sid);
  }

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
  const storageLabel = storageLabels[capture.storage_backend] || capture.storage_backend || "Local";
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
