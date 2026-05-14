import re
from typing import Any


CLARIFICATION_INTRO = "Damit ich dir nicht die falsche Leistung erklaere, brauche ich noch ein paar Angaben."
MAX_CLARIFYING_QUESTIONS = 4
MAX_RETRIEVAL_QUERY_LENGTH = 280

TOPIC_REQUIRED_SLOTS: dict[str, list[str]] = {
    "therapy_medical": ["therapy_type", "iv_status"],
    "travel_costs": ["purpose", "transport_type", "iv_status"],
    "cash_benefits": ["benefit_type", "iv_status"],
    "assistive_devices": ["purpose", "diagnosis_or_limitation", "iv_status"],
    "daily_support": ["benefit_type", "iv_status"],
    "training_reintegration": ["employment_status", "work_capacity", "iv_status"],
    "broad_entitlement": ["benefit_type", "iv_status", "current_income_support"],
}

TOPIC_KEYWORDS: dict[str, tuple[str, ...]] = {
    "therapy_medical": (
        "therapie",
        "psychotherapie",
        "physiotherapie",
        "ergotherapie",
        "logopaedie",
        "medizinisch",
        "behandlung",
        "arzt",
    ),
    "travel_costs": (
        "reisekosten",
        "transportkosten",
        "fahrkosten",
        "fahrt",
        "fahrten",
        "taxi",
        "tixi",
        "oepnv",
        "oev",
        "zug",
        "bus",
        "auto",
    ),
    "cash_benefits": (
        "geld",
        "rente",
        "taggeld",
        "leistung",
        "leistungen",
        "entschaedigung",
        "hilflosenentschaedigung",
        "assistenzbeitrag",
    ),
    "assistive_devices": (
        "hilfsmittel",
        "rollstuhl",
        "hoergeraet",
        "hoerhilfe",
        "prothese",
        "orthese",
        "geraet",
    ),
    "daily_support": (
        "assistenzbeitrag",
        "hilflosenentschaedigung",
        "alltag",
        "betreuung",
        "unterstuetzung zuhause",
        "hilfe zuhause",
        "begleitung",
    ),
    "training_reintegration": (
        "umschulung",
        "eingliederung",
        "arbeitsvermittlung",
        "ausbildung",
        "weiterbildung",
        "arbeitsfaehigkeit",
        "arbeitsplatz",
        "job",
        "beruf",
    ),
    "broad_entitlement": (
        "was kann ich beantragen",
        "welche leistungen",
        "welche leistung",
        "welchen anspruch",
        "welche ansprueche",
        "sonst noch beantragen",
        "uebernimmt die iv",
        "zahlt die iv",
        "mehr geld",
        "allgemein",
    ),
}

TOPIC_QUERY_TERMS: dict[str, tuple[str, ...]] = {
    "therapy_medical": ("Invalidenversicherung Schweiz", "IV Therapie", "medizinische Massnahme", "Kostengutsprache"),
    "travel_costs": ("Invalidenversicherung Schweiz", "IV Reisekosten", "Transportkosten", "Fahrtkosten", "medizinische Massnahme"),
    "cash_benefits": ("Invalidenversicherung Schweiz", "IV Geldleistung", "Rente", "Taggeld", "Hilflosenentschaedigung", "Assistenzbeitrag"),
    "assistive_devices": ("Invalidenversicherung Schweiz", "IV Hilfsmittel", "Kostenuebernahme", "Gebrauch im Alltag"),
    "daily_support": ("Invalidenversicherung Schweiz", "Assistenzbeitrag", "Hilflosenentschaedigung", "Unterstuetzung im Alltag"),
    "training_reintegration": ("Invalidenversicherung Schweiz", "berufliche Massnahmen", "Umschulung", "Eingliederung", "Arbeitsfaehigkeit"),
    "broad_entitlement": ("Invalidenversicherung Schweiz", "IV Leistungen", "Anspruch", "zustaendige Stelle"),
}

TOPIC_QUESTION_TEMPLATES: dict[str, dict[str, str]] = {
    "therapy_medical": {
        "therapy_type": "Um welche Therapie geht es genau, zum Beispiel Physio-, Ergo- oder Psychotherapie?",
        "iv_status": "Bist du bereits bei der IV angemeldet oder gibt es schon eine laufende Massnahme oder Verfuegung?",
        "diagnosis_or_limitation": "Welche gesundheitliche Einschraenkung oder Diagnose steht im Zusammenhang mit der Therapie?",
        "purpose": "Geht es um eine Behandlung, eine Eingliederungsmassnahme oder um Stabilisierung im Alltag oder Beruf?",
    },
    "travel_costs": {
        "purpose": "Wofuer fallen die Fahrten an: Therapie, Abklaerung, IV-Massnahme, Arbeit oder etwas anderes?",
        "transport_type": "Wie faehrst du dorthin: OEV, Auto, Taxi/Tixi oder mit Begleitperson?",
        "iv_status": "Laeuft bei dir schon eine IV-Massnahme oder eine Kostengutsprache, oder ist das noch offen?",
        "frequency": "Wie oft faellt diese Fahrt ungefaehr an, zum Beispiel woechentlich oder mehrmals pro Woche?",
    },
    "cash_benefits": {
        "benefit_type": "Meinst du eine bestimmte Leistung, zum Beispiel Rente, Taggeld, Hilflosenentschaedigung oder Assistenzbeitrag?",
        "iv_status": "Hast du schon eine IV-Verfuegung, eine laufende Anmeldung oder beziehst du bereits eine IV-Leistung?",
        "employment_status": "Arbeitest du aktuell, bist du arbeitslos oder in Ausbildung?",
        "work_capacity": "Wie hoch ist deine aktuelle Arbeitsfaehigkeit, zum Beispiel 50% oder 100% arbeitsunfaehig?",
    },
    "assistive_devices": {
        "purpose": "Wofuer brauchst du das Hilfsmittel konkret im Alltag, in der Therapie oder im Beruf?",
        "diagnosis_or_limitation": "Welche gesundheitliche Einschraenkung oder Diagnose macht das Hilfsmittel notwendig?",
        "iv_status": "Bist du bereits bei der IV angemeldet oder gibt es schon eine Verfuegung oder Massnahme?",
        "provider": "Wer hat das Hilfsmittel empfohlen oder verordnet, zum Beispiel Arzt, Therapie oder Sanitaetshaus?",
    },
    "daily_support": {
        "benefit_type": "Geht es um Assistenzbeitrag, Hilflosenentschaedigung oder eine andere Unterstuetzung im Alltag?",
        "iv_status": "Hast du schon eine IV-Leistung, eine laufende Anmeldung oder eine fruehere Verfuegung?",
        "diagnosis_or_limitation": "Welche Einschraenkungen im Alltag stehen bei dir im Vordergrund?",
        "age": "Geht es um dich selbst oder um ein Kind, und wie alt ist die betroffene Person?",
    },
    "training_reintegration": {
        "employment_status": "Wie ist deine aktuelle Situation: angestellt, arbeitslos, in Ausbildung oder bereits aus dem Beruf draussen?",
        "work_capacity": "Wie hoch ist deine aktuelle Arbeitsfaehigkeit oder Belastbarkeit in Prozent?",
        "iv_status": "Laeuft schon eine IV-Abklaerung, Massnahme oder Verfuegung zu deiner beruflichen Situation?",
        "current_income_support": "Bekommst du aktuell Lohn, Krankentaggeld, Sozialhilfe, EL oder eine andere Unterstuetzung?",
    },
    "broad_entitlement": {
        "benefit_type": "Geht es dir eher um Geldleistungen, Hilfsmittel, Fahrkosten, Therapien oder Unterstuetzung im Alltag?",
        "iv_status": "Hast du schon eine IV-Anmeldung, eine IV-Rente oder eine andere laufende IV-Leistung?",
        "current_income_support": "Bekommst du aktuell schon EL, Sozialhilfe, Taggeld oder eine andere finanzielle Unterstuetzung?",
        "employment_status": "Arbeitest du aktuell, bist du in Ausbildung oder kannst du im Moment nicht arbeiten?",
    },
}

SLOT_FALLBACK_TEMPLATES: dict[str, str] = {
    "benefit_type": "Um welche Leistung geht es genau?",
    "therapy_type": "Welche Therapie oder Behandlung meinst du genau?",
    "age": "Wie alt ist die betroffene Person?",
    "diagnosis_or_limitation": "Welche Diagnose oder welche Einschraenkung ist hier wichtig?",
    "iv_status": "Wie ist dein aktueller Stand mit der IV?",
    "existing_decision": "Gibt es dazu schon eine Verfuegung oder einen frueheren Entscheid?",
    "purpose": "Wofuer wird die Leistung oder Hilfe konkret gebraucht?",
    "provider": "Wer ist die behandelnde Stelle oder der Anbieter?",
    "frequency": "Wie oft faellt das an?",
    "transport_type": "Welches Verkehrsmittel oder welche Fahrtart betrifft es?",
    "employment_status": "Wie ist deine aktuelle Arbeits- oder Ausbildungssituation?",
    "work_capacity": "Wie hoch ist deine aktuelle Arbeitsfaehigkeit?",
    "current_income_support": "Bekommst du aktuell schon eine andere finanzielle Unterstuetzung?",
}

BENEFIT_TERMS: tuple[tuple[str, str], ...] = (
    ("assistenzbeitrag", "Assistenzbeitrag"),
    ("hilflosenentschaedigung", "Hilflosenentschaedigung"),
    ("rente", "IV-Rente"),
    ("taggeld", "Taggeld"),
    ("reisekosten", "Reisekosten"),
    ("transportkosten", "Transportkosten"),
    ("fahrkosten", "Fahrtkosten"),
    ("hilfsmittel", "Hilfsmittel"),
    ("umschulung", "Umschulung"),
    ("arbeitsvermittlung", "Arbeitsvermittlung"),
)

THERAPY_TERMS: tuple[tuple[str, str], ...] = (
    ("psychotherapie", "Psychotherapie"),
    ("physiotherapie", "Physiotherapie"),
    ("ergotherapie", "Ergotherapie"),
    ("logopaedie", "Logopaedie"),
    ("therapie", "Therapie"),
)

TRANSPORT_TERMS: tuple[tuple[str, str], ...] = (
    ("taxi", "Taxi"),
    ("tixi", "Tixi"),
    ("oev", "OEV"),
    ("oepnv", "OEV"),
    ("zug", "Zug"),
    ("bus", "Bus"),
    ("auto", "Auto"),
    ("begleitperson", "Begleitperson"),
)

PROVIDER_TERMS: tuple[tuple[str, str], ...] = (
    ("psychiater", "Psychiater"),
    ("psychologe", "Psychologe"),
    ("physiotherapie", "Physiotherapie"),
    ("ergotherapie", "Ergotherapie"),
    ("klinik", "Klinik"),
    ("spital", "Spital"),
    ("arzt", "Arzt"),
    ("aerztin", "Aerztin"),
    ("sanitaetshaus", "Sanitaetshaus"),
    ("iv-stelle", "IV-Stelle"),
    ("arbeitgeber", "Arbeitgeber"),
)

INCOME_SUPPORT_TERMS: tuple[tuple[str, str], ...] = (
    ("ergaenzungsleistungen", "Ergaenzungsleistungen"),
    (" el ", "EL"),
    ("sozialhilfe", "Sozialhilfe"),
    ("krankentaggeld", "Krankentaggeld"),
    ("taggeld", "Taggeld"),
    ("arbeitslosengeld", "Arbeitslosengeld"),
    ("pensionskasse", "Pensionskasse"),
    ("unfallversicherung", "Unfallversicherung"),
)

EMPLOYMENT_TERMS: tuple[tuple[str, str], ...] = (
    ("angestellt", "angestellt"),
    ("arbeitslos", "arbeitslos"),
    ("selbstaendig", "selbstaendig"),
    ("selbststaendig", "selbstaendig"),
    ("teilzeit", "teilzeit"),
    ("vollzeit", "vollzeit"),
    ("ausbildung", "in Ausbildung"),
    ("lehre", "in Ausbildung"),
)

PURPOSE_TERMS: tuple[tuple[str, str], ...] = (
    ("therapie", "Therapie"),
    ("abklaerung", "Abklaerung"),
    ("arbeit", "Arbeit"),
    ("beruf", "Beruf"),
    ("ausbildung", "Ausbildung"),
    ("alltag", "Alltag"),
    ("arzt", "Arzttermin"),
)

DIAGNOSIS_HINTS: tuple[str, ...] = (
    "depression",
    "angst",
    "adhs",
    "autismus",
    "ptbs",
    "schmerz",
    "ruecken",
    "migraene",
    "sehbehinderung",
    "hoerbehinderung",
    "mobilitaet",
    "psyche",
    "psychisch",
)

IV_STATUS_PATTERNS: tuple[tuple[str, str], ...] = (
    ("ich habe eine iv-rente", "bezieht IV-Rente"),
    ("iv-rente", "bezieht IV-Rente"),
    ("bei der iv angemeldet", "bei der IV angemeldet"),
    ("iv angemeldet", "bei der IV angemeldet"),
    ("laufende iv-massnahme", "laufende IV-Massnahme"),
    ("laufende massnahme", "laufende IV-Massnahme"),
    ("verfuegung", "IV-Verfuegung vorhanden"),
    ("entscheid", "frueherer Entscheid vorhanden"),
    ("abgelehnt", "Leistung wurde abgelehnt"),
    ("zugesprochen", "Leistung wurde zugesprochen"),
)


def _normalize_text(value: Any) -> str:
    text = str(value or "").lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    text = re.sub(r"[^a-z0-9%./,\- ]+", " ", text)
    return f" {re.sub(r'\s+', ' ', text).strip()} "


def _history_entries(history: list[dict[str, Any]] | list[str] | None) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for item in history or []:
        if isinstance(item, dict):
            text = str(item.get("text") or item.get("content") or item.get("message") or "").strip()
            role = str(item.get("role") or "").strip().lower()
        else:
            text = str(item or "").strip()
            role = ""
        if text:
            entries.append({"role": role, "text": text})
    return entries


def _combined_user_text(message: str, history: list[dict[str, Any]] | list[str] | None) -> str:
    parts: list[str] = []
    for entry in _history_entries(history):
        if entry["role"] in {"", "user"}:
            parts.append(entry["text"])
    parts.append(str(message or "").strip())
    return " ".join(part for part in parts if part).strip()


def _extract_terms(text: str, term_map: tuple[tuple[str, str], ...], *, limit: int = 3) -> list[str]:
    found: list[str] = []
    for needle, label in term_map:
        if needle in text and label not in found:
            found.append(label)
        if len(found) >= limit:
            break
    return found


def _first_match(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return ""
    return " ".join(part for part in match.groups() if part).strip()


def _slot_value(values: list[str]) -> str:
    return ", ".join(values[:3]).strip()


def _missing_slots(topic: str, known_slots: dict[str, str]) -> list[str]:
    return [slot for slot in TOPIC_REQUIRED_SLOTS.get(topic, []) if not str(known_slots.get(slot) or "").strip()]


def _topic_score(text: str, topic: str) -> int:
    return sum(2 if keyword in text else 0 for keyword in TOPIC_KEYWORDS.get(topic, ()))


def detect_iv_topic(message: str, history: list[dict[str, Any]] | list[str] | None = None) -> str:
    message_text = _normalize_text(message)
    combined_text = _normalize_text(_combined_user_text(message, history))
    text = message_text if message_text.strip() else combined_text
    if any(keyword in message_text for keyword in ("reisekosten", "transportkosten", "fahrkosten", "taxi", "tixi", "fahrten")):
        return "travel_costs"
    if any(keyword in message_text for keyword in ("assistenzbeitrag", "hilflosenentschaedigung")):
        return "daily_support"
    if any(keyword in message_text for keyword in ("hilfsmittel", "rollstuhl", "hoergeraet", "prothese", "orthese")):
        return "assistive_devices"
    if any(keyword in message_text for keyword in ("umschulung", "eingliederung", "arbeitsvermittlung", "ausbildung")):
        return "training_reintegration"
    if any(keyword in message_text for keyword in ("psychotherapie", "physiotherapie", "ergotherapie", "logopaedie", "therapie")):
        return "therapy_medical"
    if any(keyword in message_text for keyword in ("mehr geld", "taggeld", "rente")):
        return "cash_benefits"
    scores = {topic: _topic_score(text, topic) for topic in TOPIC_KEYWORDS}
    best_topic = max(scores, key=scores.get)
    best_score = scores[best_topic]
    ordered_scores = sorted(scores.values(), reverse=True)
    second_score = ordered_scores[1] if len(ordered_scores) > 1 else 0
    if best_score <= 0:
        fallback_scores = {topic: _topic_score(combined_text, topic) for topic in TOPIC_KEYWORDS}
        fallback_topic = max(fallback_scores, key=fallback_scores.get)
        return fallback_topic if fallback_scores[fallback_topic] > 0 else "broad_entitlement"
    if best_topic != "broad_entitlement" and best_score <= second_score:
        return "broad_entitlement"
    return best_topic


def extract_known_slots(message: str, history: list[dict[str, Any]] | list[str] | None = None) -> dict[str, str]:
    text = _normalize_text(_combined_user_text(message, history))
    slots: dict[str, str] = {}

    benefit_terms = _extract_terms(text, BENEFIT_TERMS)
    if benefit_terms:
        slots["benefit_type"] = _slot_value(benefit_terms)

    therapy_terms = _extract_terms(text, THERAPY_TERMS)
    if therapy_terms:
        slots.setdefault("purpose", "Therapie")
        specific_therapy_terms = [term for term in therapy_terms if term != "Therapie"]
        if specific_therapy_terms:
            slots["therapy_type"] = _slot_value(specific_therapy_terms + (["Therapie"] if "Therapie" in therapy_terms else []))

    age_match = _first_match(text, r"\b(\d{1,2})\s*(?:jahre|jaehrig)\b")
    if age_match:
        slots["age"] = age_match

    diagnosis_terms = [hint for hint in DIAGNOSIS_HINTS if f" {hint} " in text]
    if diagnosis_terms:
        slots["diagnosis_or_limitation"] = _slot_value([term.capitalize() for term in diagnosis_terms])
    else:
        diagnosis_phrase = _first_match(text, r"(?:wegen|aufgrund|mit)\s+([a-z0-9 ,/\-]{4,50})")
        if diagnosis_phrase:
            slots["diagnosis_or_limitation"] = diagnosis_phrase

    for needle, value in IV_STATUS_PATTERNS:
        if f" {needle} " in text:
            slots["iv_status"] = value
            break

    existing_decision = _first_match(text, r"(verfuegung|entscheid|abgelehnt|zugesprochen)")
    if existing_decision:
        slots["existing_decision"] = existing_decision

    purpose_terms = _extract_terms(text, PURPOSE_TERMS)
    if purpose_terms:
        slots["purpose"] = _slot_value(purpose_terms)

    provider_terms = _extract_terms(text, PROVIDER_TERMS)
    if provider_terms:
        slots["provider"] = _slot_value(provider_terms)

    for pattern in (
        r"(\d+\s*x\s*(?:pro|im)\s*(?:woche|monat))",
        r"(woechentlich|monatlich|taeglich|mehrmals pro woche)",
    ):
        frequency = _first_match(text, pattern)
        if frequency:
            slots["frequency"] = frequency
            break

    transport_terms = _extract_terms(text, TRANSPORT_TERMS)
    if transport_terms:
        slots["transport_type"] = _slot_value(transport_terms)

    employment_terms = _extract_terms(text, EMPLOYMENT_TERMS)
    if employment_terms:
        slots["employment_status"] = _slot_value(employment_terms)

    work_capacity = _first_match(text, r"\b(\d{1,3}\s*%)\b")
    if work_capacity:
        slots["work_capacity"] = work_capacity.replace(" ", "")

    support_terms = _extract_terms(text, INCOME_SUPPORT_TERMS)
    if re.search(r"\bel\b", text, flags=re.IGNORECASE) and "EL" not in support_terms:
        support_terms.insert(0, "EL")
    if support_terms:
        slots["current_income_support"] = _slot_value(support_terms)

    return slots


def needs_clarification(user_message: str, context: dict[str, Any]) -> bool:
    topic = str(context.get("topic") or "broad_entitlement")
    known_slots = context.get("known_slots") if isinstance(context.get("known_slots"), dict) else {}
    if topic == "broad_entitlement":
        return True
    return bool(_missing_slots(topic, {str(key): str(value) for key, value in known_slots.items()}))


def generate_clarifying_questions(topic: str, missing_slots: list[str], known_slots: dict[str, str]) -> list[str]:
    templates = TOPIC_QUESTION_TEMPLATES.get(topic, {})
    questions: list[str] = []
    seen: set[str] = set()

    def add_for_slot(slot_name: str) -> None:
        if slot_name in known_slots and str(known_slots.get(slot_name) or "").strip():
            return
        question = templates.get(slot_name) or SLOT_FALLBACK_TEMPLATES.get(slot_name) or ""
        if question and question not in seen:
            seen.add(question)
            questions.append(question)

    for slot_name in missing_slots:
        add_for_slot(slot_name)
        if len(questions) >= MAX_CLARIFYING_QUESTIONS:
            return questions

    if len(questions) < 2:
        for slot_name in TOPIC_REQUIRED_SLOTS.get(topic, []):
            add_for_slot(slot_name)
            if len(questions) >= 2:
                break

    if len(questions) < 2:
        for slot_name in templates:
            add_for_slot(slot_name)
            if len(questions) >= 2:
                break

    return questions[:MAX_CLARIFYING_QUESTIONS]


def build_retrieval_query(original_question: str, topic: str, slots: dict[str, str]) -> str:
    candidates: list[str] = [str(original_question or "").strip()]
    candidates.extend(TOPIC_QUERY_TERMS.get(topic, ()))
    for slot_name in (
        "benefit_type",
        "therapy_type",
        "purpose",
        "transport_type",
        "iv_status",
        "diagnosis_or_limitation",
        "provider",
        "frequency",
        "employment_status",
        "work_capacity",
        "current_income_support",
    ):
        value = str(slots.get(slot_name) or "").strip()
        if value:
            candidates.append(value)

    normalized_seen: set[str] = set()
    parts: list[str] = []
    for candidate in candidates:
        compact = " ".join(str(candidate or "").split()).strip(" ,.-")
        if not compact:
            continue
        normalized = _normalize_text(compact).strip()
        if not normalized or normalized in normalized_seen:
            continue
        normalized_seen.add(normalized)
        parts.append(compact)

    query = ""
    for part in parts:
        proposed = f"{query} {part}".strip()
        if len(proposed) > MAX_RETRIEVAL_QUERY_LENGTH:
            break
        query = proposed
    return query


def analyze_iv_knowledge_request(question: str, history: list[dict[str, Any]] | list[str] | None = None) -> dict[str, Any]:
    topic = detect_iv_topic(question, history)
    known_slots = extract_known_slots(question, history)
    if topic == "travel_costs":
        known_slots.setdefault("benefit_type", "Reisekosten")
    elif topic == "assistive_devices":
        known_slots.setdefault("benefit_type", "Hilfsmittel")
    missing_slots = _missing_slots(topic, known_slots)
    clarification_needed = needs_clarification(question, {"topic": topic, "known_slots": known_slots})
    return {
        "topic": topic,
        "known_slots": known_slots,
        "missing_slots": missing_slots,
        "needs_clarification": clarification_needed,
        "clarifying_questions": generate_clarifying_questions(topic, missing_slots, known_slots) if clarification_needed else [],
        "clarification_intro": CLARIFICATION_INTRO if clarification_needed else "",
        "retrieval_query": build_retrieval_query(question, topic, known_slots),
    }
