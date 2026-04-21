import argparse
import io
import json
import logging
import os
import re
import unicodedata
from datetime import date, datetime
from typing import Dict, Optional

from pypdf import PdfReader, PdfWriter

try:
    from .calendar_manager import ASSISTANT_HOUR_FIELDS, get_assistant_hours, get_assistant_hours_breakdown, get_events
    from .pdf_field_mapping import MONTH_EXPORT_VALUES, month_to_checkbox_value, pdf_field_name
except ImportError:
    from calendar_manager import ASSISTANT_HOUR_FIELDS, get_assistant_hours, get_assistant_hours_breakdown, get_events
    from pdf_field_mapping import MONTH_EXPORT_VALUES, month_to_checkbox_value, pdf_field_name


logger = logging.getLogger(__name__)

STANDARD_RATE = 35.30
DUAL_REPORT_HOURLY_RATE = 35.00
AHV_RATE = 0.053
ALV_RATE = 0.011
ASSISTENZ_SERVICE_PROVIDER = "Pitaqi Drita"
ASSISTENZ_SERVICE_DESCRIPTION = "Assistenzleistung Wohnen"

PROFILE_REQUIRED_FIELDS = (
    "insured_name",
    "ahv_number",
    "street",
    "plz_ort",
    "iban",
    "mitteilungsnummer",
)


def _parse_month(month: str) -> datetime:
    if not month or not str(month).strip():
        raise ValueError("month is required (YYYY-MM)")
    return datetime.strptime(str(month).strip(), "%Y-%m")


def _format_hours(value: float) -> str:
    formatted = f"{value:.2f}"
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    return formatted


def _format_hours_fixed(value: float, zero_as_blank: bool = False) -> str:
    rounded_value = round(float(value or 0.0), 2)
    if zero_as_blank and rounded_value == 0:
        return ""
    return f"{rounded_value:.2f}"


def format_chf(value: float) -> str:
    return f"{value:.2f}"


def _format_date(value: date) -> str:
    return value.strftime("%d.%m.%Y")


def format_swiss_year(month: str) -> str:
    return str(_parse_month(month).year)


def _sanitize_name(value: str) -> str:
    sanitized = re.sub(r"\s+", "_", value.strip())
    sanitized = re.sub(r"[^0-9A-Za-z_\-]", "", sanitized)
    return sanitized or "Unbekannt"


def _compose_plz_ort(zip_code: str, city: str) -> str:
    zip_clean = str(zip_code or "").strip()
    city_clean = str(city or "").strip()
    return " ".join(part for part in (zip_clean, city_clean) if part).strip()


def _normalize_personal_data(source: dict) -> dict:
    if not isinstance(source, dict):
        return {}

    insured = source.get("insuredPerson") if isinstance(source.get("insuredPerson"), dict) else {}
    issuer = source.get("invoiceIssuer") if isinstance(source.get("invoiceIssuer"), dict) else {}
    billing = source.get("billing") if isinstance(source.get("billing"), dict) else {}

    if insured or issuer or billing:
        issuer_same_as_insured = bool(issuer.get("sameAsInsuredPerson"))
        insured_name = str(insured.get("fullName") or "").strip()
        insured_street = str(insured.get("street") or "").strip()
        insured_plz_ort = _compose_plz_ort(insured.get("zip"), insured.get("city"))

        issuer_name = insured_name if issuer_same_as_insured else str(issuer.get("fullName") or "").strip()
        issuer_street = insured_street if issuer_same_as_insured else str(issuer.get("street") or "").strip()
        issuer_plz_ort = (
            insured_plz_ort
            if issuer_same_as_insured
            else _compose_plz_ort(issuer.get("zip"), issuer.get("city"))
        )

        account_holder_name = str(billing.get("accountHolderFullName") or "").strip()
        account_holder_street = str(billing.get("accountHolderStreet") or "").strip()
        account_holder_plz_ort = _compose_plz_ort(
            billing.get("accountHolderZip"),
            billing.get("accountHolderCity"),
        )

        return {
            "insured_name": insured_name,
            "insured_birth_date": str(
                insured.get("dateOfBirth") or insured.get("birthDate") or insured.get("date_of_birth") or ""
            ).strip(),
            "ahv_number": str(insured.get("ahvNumber") or "").strip(),
            "street": insured_street,
            "plz_ort": insured_plz_ort,
            "iban": str(billing.get("iban") or "").strip(),
            "mitteilungsnummer": str(billing.get("referenceNumber") or "").strip(),
            "invoice_issuer_name": issuer_name,
            "invoice_issuer_email": str(issuer.get("email") or "").strip(),
            "invoice_issuer_street": issuer_street,
            "invoice_issuer_plz_ort": issuer_plz_ort,
            "payment_name": account_holder_name or issuer_name or insured_name,
            "payment_street": account_holder_street or issuer_street or insured_street,
            "payment_plz_ort": account_holder_plz_ort or issuer_plz_ort or insured_plz_ort,
            "gln": str(billing.get("gln") or "").strip(),
        }

    return {
        "insured_name": str(source.get("insured_name") or source.get("name") or "").strip(),
        "insured_birth_date": str(
            source.get("insured_birth_date") or source.get("date_of_birth") or source.get("birth_date") or ""
        ).strip(),
        "ahv_number": str(source.get("ahv_number") or "").strip(),
        "street": str(source.get("street") or "").strip(),
        "plz_ort": str(source.get("plz_ort") or "").strip(),
        "iban": str(source.get("iban") or "").strip(),
        "mitteilungsnummer": str(source.get("mitteilungsnummer") or "").strip(),
        "invoice_issuer_name": str(source.get("invoice_issuer_name") or "").strip(),
        "invoice_issuer_email": str(source.get("invoice_issuer_email") or "").strip(),
        "invoice_issuer_street": str(source.get("invoice_issuer_street") or "").strip(),
        "invoice_issuer_plz_ort": str(source.get("invoice_issuer_plz_ort") or "").strip(),
        "payment_name": str(source.get("payment_name") or "").strip(),
        "payment_street": str(source.get("payment_street") or "").strip(),
        "payment_plz_ort": str(source.get("payment_plz_ort") or "").strip(),
        "gln": str(source.get("gln") or "").strip(),
    }


def _validate_profile(profile: dict) -> dict:
    if not isinstance(profile, dict):
        raise ValueError("profile must be a JSON object")

    normalized = _normalize_personal_data(profile)
    missing = [field for field in PROFILE_REQUIRED_FIELDS if not normalized.get(field)]
    if missing:
        logger.warning("Missing required profile fields: %s", ", ".join(missing))
        missing_label = ", ".join(missing)
        raise ValueError(f"profile is missing required field(s): {missing_label}")

    return normalized


def load_profile(profile_path: str) -> dict:
    with open(profile_path, "r", encoding="utf-8") as file:
        raw_profile = json.load(file)
    return _validate_profile(raw_profile)


def load_profile_payload(profile_data: dict) -> dict:
    return _validate_profile(profile_data)


def calculate_payroll(gross_hourly_rate: float, hours: float) -> Dict[str, float]:
    gross_pay = round(gross_hourly_rate * hours, 2)
    ahv = round(gross_pay * AHV_RATE, 2)
    alv = round(gross_pay * ALV_RATE, 2)
    net_pay = round(gross_pay - ahv - alv, 2)
    return {
        "gross_pay": gross_pay,
        "ahv": ahv,
        "alv": alv,
        "net_pay": net_pay,
    }


def sum_assistant_hours(month: str) -> float:
    hours = float(get_assistant_hours(month))
    if hours <= 0:
        logger.warning("No assistant hours found for reporting month %s", month)
    return hours


def get_month_data(month: str, hours_override: Optional[float] = None) -> dict:
    parsed_month = _parse_month(month)
    total_hours = sum_assistant_hours(month)
    if hours_override is not None:
        total_hours = float(hours_override)

    payroll = calculate_payroll(STANDARD_RATE, total_hours)

    return {
        "month": month,
        "year": format_swiss_year(month),
        "month_label": parsed_month.strftime("%B"),
        "month_number": parsed_month.month,
        "month_export_value": month_to_checkbox_value(parsed_month.month),
        "total_hours": total_hours,
        "report_total_chf": payroll["gross_pay"],
        "gross_pay": payroll["gross_pay"],
        "ahv_deduction": payroll["ahv"],
        "alv_deduction": payroll["alv"],
        "net_pay": payroll["net_pay"],
    }


def build_form_payload(month_data: dict, personal_data: dict, invoice_date: Optional[date] = None) -> dict:
    if invoice_date is None:
        invoice_date = date.today()

    payload = {
        pdf_field_name("invoice_date"): _format_date(invoice_date),
        pdf_field_name("insured_name"): personal_data.get("insured_name", ""),
        pdf_field_name("ahv_number"): personal_data.get("ahv_number", ""),
        pdf_field_name("street"): personal_data.get("street", ""),
        pdf_field_name("plz_ort"): personal_data.get("plz_ort", ""),
        pdf_field_name("mitteilungsnummer"): personal_data.get("mitteilungsnummer", ""),
        pdf_field_name("iban"): personal_data.get("iban", ""),
        pdf_field_name("year"): month_data["year"],
        pdf_field_name("month"): month_data["month_export_value"],
        pdf_field_name("hours_standard"): _format_hours(float(month_data["total_hours"])),
        pdf_field_name("rate_standard"): format_chf(STANDARD_RATE),
        pdf_field_name("gross_standard"): format_chf(float(month_data["gross_pay"])),
        pdf_field_name("total_payout"): format_chf(float(month_data["report_total_chf"])),
        pdf_field_name("advance_payment"): format_chf(float(month_data["report_total_chf"])),
        pdf_field_name("invoice_issuer_name"): personal_data.get("invoice_issuer_name", ""),
        pdf_field_name("invoice_issuer_email"): personal_data.get("invoice_issuer_email", ""),
        pdf_field_name("invoice_issuer_street"): personal_data.get("invoice_issuer_street", ""),
        pdf_field_name("invoice_issuer_plz_ort"): personal_data.get("invoice_issuer_plz_ort", ""),
        pdf_field_name("payment_name"): personal_data.get("payment_name", ""),
        pdf_field_name("payment_street"): personal_data.get("payment_street", ""),
        pdf_field_name("payment_plz_ort"): personal_data.get("payment_plz_ort", ""),
        pdf_field_name("gln"): personal_data.get("gln", ""),
        "_meta": {
            "invoice_date": _format_date(invoice_date),
            "insured_name": personal_data.get("insured_name", ""),
            "ahv_number": personal_data.get("ahv_number", ""),
            "month": month_data["month"],
            "total_hours": float(month_data["total_hours"]),
            "gross_pay": float(month_data["gross_pay"]),
            "report_total_chf": float(month_data["report_total_chf"]),
            "ahv_deduction": float(month_data["ahv_deduction"]),
            "alv_deduction": float(month_data["alv_deduction"]),
            "net_pay": float(month_data["net_pay"]),
            "iban": personal_data.get("iban", ""),
        },
    }

    # TODO: Clarify whether "Vorschuss" should differ from total payout by business rule.
    # For now, both "Total_Auszahlung" and Vorschuss row ("Spesen3") receive assistant_hours * CHF 35.30.

    return payload


def preview_payload(payload: dict) -> str:
    meta = payload.get("_meta", {})
    lines = [
        "========================================",
        "  FORM PREVIEW - Assistenzbeitrag",
        "========================================",
        f"  Rechnungsdatum       : {meta.get('invoice_date', '')}",
        f"  Versicherte Person   : {meta.get('insured_name', '')}",
        f"  AHV-Nummer           : {meta.get('ahv_number', '')}",
        f"  Monat                : {meta.get('month', '')}",
        f"  Stunden              : {_format_hours(float(meta.get('total_hours', 0.0)))}",
        f"  Bruttolohn           : CHF {format_chf(float(meta.get('gross_pay', 0.0)))}",
        f"  AHV (5.3%)           : CHF -{format_chf(float(meta.get('ahv_deduction', 0.0)))}",
        f"  ALV (1.1%)           : CHF -{format_chf(float(meta.get('alv_deduction', 0.0)))}",
        f"  Nettolohn            : CHF {format_chf(float(meta.get('net_pay', 0.0)))}",
        f"  IBAN                 : {meta.get('iban', '')}",
        "========================================",
    ]
    return "\n".join(lines)


def preview_dual_payloads(stundenblatt_payload: dict, rechnung_payload: dict) -> str:
    stundenblatt_meta = stundenblatt_payload.get("_meta", {})
    rechnung_meta = rechnung_payload.get("_meta", {})
    lines = [
        "========================================",
        "  FORM PREVIEW - Assistenzbeitrag (Dual)",
        "========================================",
        f"  Monat                : {stundenblatt_meta.get('month', '')}",
        f"  Stundenblatt Tage    : {stundenblatt_meta.get('days_filled', 0)}",
        f"  Total Stunden        : {_format_hours_fixed(float(stundenblatt_meta.get('total_hours', 0.0)))}",
        f"  Rechnungsdatum       : {rechnung_meta.get('invoice_date', '')}",
        f"  Betrag               : CHF {format_chf(float(rechnung_meta.get('total_amount_chf', 0.0)))}",
        "========================================",
    ]
    return "\n".join(lines)


def _normalize_field_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", ascii_value.lower())


def _field_order(field_name: str) -> int:
    match = re.search(r"(\d+)$", str(field_name or ""))
    return int(match.group(1)) if match else 1


def _is_multi_widget_parent(field_definition: dict) -> bool:
    kids = field_definition.get("/Kids") if isinstance(field_definition, dict) else None
    if kids is None:
        return False
    try:
        return len(kids) > 1 and not field_definition.get("/Rect")
    except TypeError:
        return False


def get_assistant_daily_hours(month: str) -> list[dict]:
    grouped_hours: Dict[str, Dict[str, float]] = {}

    for event in get_events(month):
        if event.get("category") != "assistant":
            continue

        event_date = str(event.get("date", "")).strip()
        if not event_date:
            continue

        day_hours = grouped_hours.setdefault(event_date, {field: 0.0 for field in ASSISTANT_HOUR_FIELDS})
        assistant_hours = event.get("assistant_hours") or {}

        for field in ASSISTANT_HOUR_FIELDS:
            day_hours[field] += float(assistant_hours.get(field, 0.0) or 0.0)

    daily_rows = []
    for event_date in sorted(grouped_hours):
        rounded_hours = {field: round(grouped_hours[event_date][field], 2) for field in ASSISTANT_HOUR_FIELDS}
        daily_rows.append(
            {
                "date": event_date,
                **rounded_hours,
                "total_hours": round(sum(rounded_hours.values()), 2),
            }
        )
    return daily_rows


def _default_stundenblatt_layout() -> dict:
    return {
        "date_fields": [f"datum_{index}" for index in range(1, 11)],
        "category_fields": {
            "koerperpflege": [f"koerperpflege_{index}" for index in range(1, 11)],
            "mahlzeiten_zubereiten": [f"mahlzeiten_zubereiten_{index}" for index in range(1, 11)],
            "mahlzeiten_eingeben": [f"mahlzeiten_eingeben_{index}" for index in range(1, 11)],
            "begleitung_therapie": [f"begleitung_therapie_{index}" for index in range(1, 11)],
        },
        "total_field": "total_hours",
    }


def _resolve_stundenblatt_layout(template_fields: Optional[dict]) -> dict:
    if not template_fields:
        return _default_stundenblatt_layout()

    date_fields = []
    category_fields = {field: [] for field in ASSISTANT_HOUR_FIELDS}
    total_field = None

    for field_name, field_definition in template_fields.items():
        token = _normalize_field_token(field_name)
        if not token:
            continue

        if token == "totalhours" or token.startswith("total"):
            total_field = field_name
            continue

        if token.startswith("datum"):
            if _is_multi_widget_parent(field_definition):
                total_field = total_field or field_name
                continue
            date_fields.append((_field_order(field_name), field_name))
            continue

        if "korperpflege" in token:
            category_fields["koerperpflege"].append((_field_order(field_name), field_name))
            continue
        if "mahlzeitenzubereiten" in token:
            category_fields["mahlzeiten_zubereiten"].append((_field_order(field_name), field_name))
            continue
        if "mahlzeiteneingeben" in token:
            category_fields["mahlzeiten_eingeben"].append((_field_order(field_name), field_name))
            continue
        if "begleitungtherapie" in token:
            category_fields["begleitung_therapie"].append((_field_order(field_name), field_name))

    resolved_layout = {
        "date_fields": [field_name for _, field_name in sorted(date_fields)],
        "category_fields": {
            field: [field_name for _, field_name in sorted(items)]
            for field, items in category_fields.items()
        },
        "total_field": total_field,
    }

    if not resolved_layout["date_fields"] or not all(resolved_layout["category_fields"].values()):
        return _default_stundenblatt_layout()

    return resolved_layout


def build_stundenblatt_payload(month: str, template_fields: Optional[dict] = None) -> dict:
    month_data = get_month_data(month)
    daily_rows = get_assistant_daily_hours(month)
    monthly_breakdown = get_assistant_hours_breakdown(month)
    layout = _resolve_stundenblatt_layout(template_fields)

    computed_breakdown = {
        field: round(sum(row[field] for row in daily_rows), 2)
        for field in ASSISTANT_HOUR_FIELDS
    }
    if computed_breakdown != monthly_breakdown:
        logger.warning(
            "Daily assistant breakdown does not match monthly breakdown for %s: %s != %s",
            month,
            computed_breakdown,
            monthly_breakdown,
        )

    max_rows = min(
        len(layout["date_fields"]),
        *(len(layout["category_fields"][field]) for field in ASSISTANT_HOUR_FIELDS),
    )
    if max_rows <= 0:
        raise ValueError("Stundenblatt template does not expose usable date/category fields")

    payload = {}
    for row_index, row in enumerate(daily_rows[:max_rows]):
        payload[layout["date_fields"][row_index]] = _format_date(datetime.strptime(row["date"], "%Y-%m-%d").date())
        for field in ASSISTANT_HOUR_FIELDS:
            payload[layout["category_fields"][field][row_index]] = _format_hours_fixed(row[field], zero_as_blank=True)

    total_hours = round(sum(computed_breakdown.values()), 2)

    if len(daily_rows) > max_rows:
        logger.warning(
            "Stundenblatt template supports %s day rows, truncating %s additional days for %s",
            max_rows,
            len(daily_rows) - max_rows,
            month,
        )

    payload["_meta"] = {
        "month": month_data["month"],
        "days_filled": min(len(daily_rows), max_rows),
        "days_available": len(daily_rows),
        "total_hours": total_hours,
        "breakdown": computed_breakdown,
    }
    return payload


def _format_optional_birth_date(raw_value: str) -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    for pattern in ("%Y-%m-%d", "%d.%m.%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, pattern).strftime("%d.%m.%Y")
        except ValueError:
            continue
    return value


def _resolve_rechnung_layout(template_fields: Optional[dict]) -> dict:
    if not template_fields:
        return {"kind": "modern"}

    if pdf_field_name("insured_name") in template_fields:
        return {"kind": "legacy"}

    provider_fields = []
    description_fields = []
    hours_fields = []
    amount_fields = []
    attachment_fields = []
    month_field = None
    remarks_field = None
    header_field = None
    total_field = None

    for field_name in template_fields:
        token = _normalize_field_token(field_name)
        if token == "bemerkungen":
            remarks_field = field_name
        elif token == "text1":
            header_field = field_name
        elif token == "text2":
            total_field = field_name
        elif token.isdigit():
            month_field = field_name
        elif "bitteauswahlen" in token and "beschreibung" in token:
            description_fields.append((_field_order(field_name), field_name))
        elif "bitteauswahlen" in token and "anzstd" in token:
            hours_fields.append((_field_order(field_name), field_name))
        elif "bitteauswahlen" in token and "chf3500" in token:
            amount_fields.append((_field_order(field_name), field_name))
        elif "bitteauswahlen" in token and "beilagen" in token:
            attachment_fields.append((_field_order(field_name), field_name))
        elif "row" in token:
            provider_fields.append((_field_order(field_name), field_name))

    return {
        "kind": "modern",
        "provider_field": sorted(provider_fields)[0][1] if provider_fields else None,
        "description_field": sorted(description_fields)[0][1] if description_fields else None,
        "hours_field": sorted(hours_fields)[0][1] if hours_fields else None,
        "amount_field": sorted(amount_fields)[0][1] if amount_fields else None,
        "attachment_field": sorted(attachment_fields)[0][1] if attachment_fields else None,
        "month_field": month_field,
        "remarks_field": remarks_field,
        "header_field": header_field,
        "total_field": total_field,
    }


def build_rechnung_payload(
    month: str,
    personal_data: dict,
    total_hours: float,
    template_fields: Optional[dict] = None,
    invoice_date: Optional[date] = None,
) -> dict:
    if invoice_date is None:
        invoice_date = date.today()

    layout = _resolve_rechnung_layout(template_fields)
    total_amount = round(float(total_hours) * DUAL_REPORT_HOURLY_RATE, 2)
    month_data = get_month_data(month, hours_override=total_hours)

    if layout["kind"] == "legacy":
        payload = build_form_payload(month_data, personal_data, invoice_date=invoice_date)
        payload["_meta"]["total_amount_chf"] = total_amount
        payload["_meta"]["hourly_rate_chf"] = DUAL_REPORT_HOURLY_RATE
        return payload

    period_value = f"{month_data['month_number']:02d}/{month_data['year']}"
    birth_date_value = _format_optional_birth_date(personal_data.get("insured_birth_date", ""))
    remark_lines = [
        f"Versicherte Person: {personal_data.get('insured_name', '')}",
        f"Geburtsdatum: {birth_date_value}" if birth_date_value else "",
        "Adresse: " + ", ".join(
            part for part in (personal_data.get("street", ""), personal_data.get("plz_ort", "")) if part
        ),
        f"Rechnungsdatum: {_format_date(invoice_date)}",
        f"Rechnungsperiode: {period_value}",
    ]

    payload = {
        "_meta": {
            "invoice_date": _format_date(invoice_date),
            "period": period_value,
            "total_hours": round(float(total_hours), 2),
            "total_amount_chf": total_amount,
            "hourly_rate_chf": DUAL_REPORT_HOURLY_RATE,
        }
    }

    if layout.get("header_field"):
        payload[layout["header_field"]] = f"Rechnungsdatum: {_format_date(invoice_date)}"
    if layout.get("month_field"):
        payload[layout["month_field"]] = f"{month_data['month_number']:02d}"
    if layout.get("provider_field"):
        payload[layout["provider_field"]] = ASSISTENZ_SERVICE_PROVIDER
    if layout.get("description_field"):
        payload[layout["description_field"]] = ASSISTENZ_SERVICE_DESCRIPTION
    if layout.get("hours_field"):
        payload[layout["hours_field"]] = _format_hours_fixed(total_hours)
    if layout.get("amount_field"):
        payload[layout["amount_field"]] = f"CHF {format_chf(total_amount)}"
    if layout.get("remarks_field"):
        payload[layout["remarks_field"]] = "\n".join(line for line in remark_lines if line)
    if layout.get("total_field"):
        payload[layout["total_field"]] = f"{format_chf(total_amount)}"

    return payload


def merge_pdf_documents_to_bytes(input_documents: list[bytes]) -> bytes:
    writer = PdfWriter()
    try:
        for document in input_documents:
            writer.append(PdfReader(io.BytesIO(document)))

        buffer = io.BytesIO()
        writer.write(buffer)
        return buffer.getvalue()
    finally:
        writer.close()


def merge_pdf_documents(input_documents: list[bytes], output_path: str) -> str:
    output_bytes = merge_pdf_documents_to_bytes(input_documents)
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "wb") as file:
        file.write(output_bytes)
    return output_path


def merge_pdfs(input_paths: list[str], output_path: str) -> str:
    documents = []
    for input_path in input_paths:
        with open(input_path, "rb") as input_file:
            documents.append(input_file.read())
    return merge_pdf_documents(documents, output_path)


def fill_form_to_bytes(template_path: str, payload: dict) -> bytes:
    reader = PdfReader(template_path)
    fields = reader.get_fields() or {}

    writer = PdfWriter()
    try:
        writer.append(reader)

        form_payload = {key: value for key, value in payload.items() if not key.startswith("_")}

        missing_pdf_fields = [field_name for field_name in form_payload if field_name not in fields]
        if missing_pdf_fields:
            logger.warning("PDF template missing fields: %s", ", ".join(sorted(missing_pdf_fields)))

        for page in writer.pages:
            writer.update_page_form_field_values(page, form_payload, auto_regenerate=False)

        buffer = io.BytesIO()
        writer.write(buffer)
        return buffer.getvalue()
    finally:
        writer.close()


def fill_form(template_path: str, output_path: str, payload: dict):
    output_bytes = fill_form_to_bytes(template_path, payload)
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, "wb") as file:
        file.write(output_bytes)


def generate_output_path(name: str, month: str) -> str:
    os.makedirs("output", exist_ok=True)
    return os.path.join("output", f"Assistenzbeitrag_{_sanitize_name(name)}_{month}.pdf")


def generate_report_filename(name: str, month: str) -> str:
    return os.path.basename(generate_output_path(name, month))


def run_pipeline(month: str, template_path: str, config: dict, preview: bool = False):
    month_data = get_month_data(month, config.get("hours") if isinstance(config, dict) else None)
    personal_data = _normalize_personal_data(config or {})
    payload = build_form_payload(month_data, personal_data)

    if preview:
        print(preview_payload(payload))

    output_path = generate_output_path(personal_data.get("insured_name", ""), month)
    fill_form(template_path, output_path, payload)
    print(f"Saved to: {output_path}")
    return output_path


def fill_assistenz_form(template_pdf_path: str, output_path: str, data: dict, preview: bool = False):
    month = str(data["month"]).strip()
    hours_override = None
    if "hours" in data and str(data["hours"]).strip():
        hours_override = float(data["hours"])

    month_data = get_month_data(month, hours_override=hours_override)
    personal_data = _normalize_personal_data(data)
    payload = build_form_payload(month_data, personal_data)

    if preview:
        print(preview_payload(payload))

    fill_form(template_pdf_path, output_path, payload)
    return True


def _resolve_profile(profile_path: Optional[str] = None, profile_data: Optional[dict] = None) -> dict:
    if profile_data is not None:
        return load_profile_payload(profile_data)
    if not profile_path:
        raise ValueError("profile_path or profile_data is required")
    return load_profile(profile_path)


def fill_assistenz_form_auto_bytes(
    template_pdf_path: str,
    month: str,
    profile_path: Optional[str] = None,
    profile_data: Optional[dict] = None,
    preview: bool = False,
) -> bytes:
    profile = _resolve_profile(profile_path=profile_path, profile_data=profile_data)
    month_data = get_month_data(month)
    payload = build_form_payload(month_data, profile)

    if preview:
        print(preview_payload(payload))

    return fill_form_to_bytes(template_pdf_path, payload)


def fill_assistenz_form_auto(
    template_pdf_path: str,
    month: str,
    profile_path: Optional[str] = None,
    output_path: Optional[str] = None,
    profile_data: Optional[dict] = None,
    preview: bool = False,
) -> str:
    profile = _resolve_profile(profile_path=profile_path, profile_data=profile_data)
    resolved_output_path = output_path or generate_output_path(profile["insured_name"], month)
    output_bytes = fill_assistenz_form_auto_bytes(
        template_pdf_path=template_pdf_path,
        month=month,
        profile_data=profile,
        preview=preview,
    )
    output_dir = os.path.dirname(resolved_output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(resolved_output_path, "wb") as file:
        file.write(output_bytes)
    return resolved_output_path


def fill_assistenz_dual_form_auto_bytes(
    stundenblatt_template_pdf_path: str,
    rechnung_template_pdf_path: str,
    month: str,
    profile_path: Optional[str] = None,
    profile_data: Optional[dict] = None,
    preview: bool = False,
) -> bytes:
    profile = _resolve_profile(profile_path=profile_path, profile_data=profile_data)

    stundenblatt_fields = PdfReader(stundenblatt_template_pdf_path).get_fields() or {}
    rechnung_fields = PdfReader(rechnung_template_pdf_path).get_fields() or {}

    stundenblatt_payload = build_stundenblatt_payload(month, template_fields=stundenblatt_fields)
    total_hours = float(stundenblatt_payload["_meta"]["total_hours"])
    rechnung_payload = build_rechnung_payload(
        month=month,
        personal_data=profile,
        total_hours=total_hours,
        template_fields=rechnung_fields,
    )

    if preview:
        print(preview_dual_payloads(stundenblatt_payload, rechnung_payload))

    stundenblatt_pdf = fill_form_to_bytes(stundenblatt_template_pdf_path, stundenblatt_payload)
    rechnung_pdf = fill_form_to_bytes(rechnung_template_pdf_path, rechnung_payload)
    return merge_pdf_documents_to_bytes([stundenblatt_pdf, rechnung_pdf])


def fill_assistenz_dual_form_auto(
    stundenblatt_template_pdf_path: str,
    rechnung_template_pdf_path: str,
    month: str,
    profile_path: Optional[str] = None,
    output_path: Optional[str] = None,
    profile_data: Optional[dict] = None,
    preview: bool = False,
) -> str:
    profile = _resolve_profile(profile_path=profile_path, profile_data=profile_data)
    resolved_output_path = output_path or generate_output_path(profile["insured_name"], month)
    output_bytes = fill_assistenz_dual_form_auto_bytes(
        stundenblatt_template_pdf_path=stundenblatt_template_pdf_path,
        rechnung_template_pdf_path=rechnung_template_pdf_path,
        month=month,
        profile_data=profile,
        preview=preview,
    )
    output_dir = os.path.dirname(resolved_output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(resolved_output_path, "wb") as file:
        file.write(output_bytes)
    return resolved_output_path


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Auto-fill Assistenzbeitrag AcroForm PDF")
    parser.add_argument("--template", required=True, help="Path to PDF template")
    parser.add_argument("--month", required=True, help="Billing month in YYYY-MM format")
    parser.add_argument("--profile", required=True, help="Path to profile JSON")
    parser.add_argument("--output", default=None, help="Optional output PDF path")
    parser.add_argument("--preview", action="store_true", help="Print payload preview before writing")
    return parser


def main() -> None:
    parser = _build_cli_parser()
    args = parser.parse_args()

    output_path = fill_assistenz_form_auto(
        template_pdf_path=args.template,
        month=args.month,
        profile_path=args.profile,
        output_path=args.output,
        preview=args.preview,
    )
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()
