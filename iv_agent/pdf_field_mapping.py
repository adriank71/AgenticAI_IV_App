from dataclasses import dataclass
from typing import Dict, Literal

FieldCategory = Literal["master_profile", "reporting_period", "month_checkbox", "calculation"]


@dataclass(frozen=True)
class PdfFieldMapping:
    key: str
    pdf_field: str
    category: FieldCategory
    required: bool = False
    description: str = ""


MASTER_PROFILE_FIELDS = (
    PdfFieldMapping("insured_name", "1NameVorname", "master_profile", True, "Insured person full name"),
    PdfFieldMapping("ahv_number", "11AHVNr", "master_profile", True, "AHV number"),
    PdfFieldMapping("street", "1Strasse", "master_profile", True, "Insured person street"),
    PdfFieldMapping("plz_ort", "1PLZOrt", "master_profile", True, "Insured person ZIP/city"),
    PdfFieldMapping("mitteilungsnummer", "3_Mitteilungsnummer", "master_profile", True, "Reference number"),
    PdfFieldMapping("iban", "3_IBAN", "master_profile", True, "IBAN"),
    PdfFieldMapping("invoice_issuer_name", "2_Name_Vorname", "master_profile", False, "Invoice issuer full name"),
    PdfFieldMapping("invoice_issuer_email", "2_Email", "master_profile", False, "Invoice issuer email"),
    PdfFieldMapping("invoice_issuer_street", "2_Strasse", "master_profile", False, "Invoice issuer street"),
    PdfFieldMapping("invoice_issuer_plz_ort", "2_PLZ_Ort", "master_profile", False, "Invoice issuer ZIP/city"),
    PdfFieldMapping("payment_name", "3_Name_Vorname", "master_profile", False, "Payment account holder name"),
    PdfFieldMapping("payment_street", "3_Strasse", "master_profile", False, "Payment account holder street"),
    PdfFieldMapping("payment_plz_ort", "3_PLZ_Ort", "master_profile", False, "Payment account holder ZIP/city"),
    PdfFieldMapping("gln", "3_GLN", "master_profile", False, "GLN number"),
)

REPORTING_PERIOD_FIELDS = (
    PdfFieldMapping("invoice_date", "0Rechnungsdatum", "reporting_period", True, "Invoice date (Swiss format)"),
    PdfFieldMapping("year", "Abrechnungsperiode_Jahr", "reporting_period", True, "Reporting year"),
)

# In this template, month is a radio-button group named "Monat".
MONTH_CHECKBOX_FIELDS = (
    PdfFieldMapping("month", "Monat", "month_checkbox", True, "Reporting month selector"),
)

CALCULATION_FIELDS = (
    PdfFieldMapping(
        "hours_standard",
        "Anzahl_Stunden",
        "calculation",
        True,
        "Assistenzleistung mit Standardqualifikation: effektiv erbrachte Stunden",
    ),
    PdfFieldMapping("rate_standard", "Stundenlohn", "calculation", True, "Ansatz in CHF"),
    PdfFieldMapping("gross_standard", "Grundlohn", "calculation", True, "Betrag in CHF for standard row"),
    PdfFieldMapping("total_payout", "Total_Auszahlung", "calculation", False, "Total payout"),
    PdfFieldMapping(
        "advance_payment",
        "Spesen3",
        "calculation",
        False,
        "Vorschuss row amount (used as additional visible total placement)",
    ),
)

PDF_FIELD_MAPPINGS = (
    *MASTER_PROFILE_FIELDS,
    *REPORTING_PERIOD_FIELDS,
    *MONTH_CHECKBOX_FIELDS,
    *CALCULATION_FIELDS,
)

PDF_FIELD_BY_KEY: Dict[str, PdfFieldMapping] = {mapping.key: mapping for mapping in PDF_FIELD_MAPPINGS}

# Export values discovered from the AcroForm "Monat" radio field.
# March intentionally uses the template's internal export value.
MONTH_EXPORT_VALUES: Dict[int, str] = {
    1: "/Januar",
    2: "/Februar",
    3: "/M\u92dcz",
    4: "/April",
    5: "/Mai",
    6: "/Juni",
    7: "/Juli",
    8: "/August",
    9: "/September",
    10: "/Oktober",
    11: "/November",
    12: "/Dezember",
}


def pdf_field_name(key: str) -> str:
    return PDF_FIELD_BY_KEY[key].pdf_field


def month_to_checkbox_value(month_number: int) -> str:
    if month_number not in MONTH_EXPORT_VALUES:
        raise ValueError(f"Unsupported month number for PDF export: {month_number}")
    return MONTH_EXPORT_VALUES[month_number]
