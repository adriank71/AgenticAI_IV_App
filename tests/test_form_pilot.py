import io
import json
import os
import shutil
import unittest
import uuid
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch

from pypdf import PdfReader, PdfWriter

from iv_agent.form_pilot import (
    MONTH_EXPORT_VALUES,
    calculate_payroll,
    build_form_payload,
    build_rechnung_payload,
    build_stundenblatt_payload,
    fill_assistenz_dual_form_auto,
    fill_assistenz_form_auto,
    get_assistant_daily_hours,
    get_month_data,
    load_profile,
)


@contextmanager
def workspace_tempdir():
    base_dir = os.path.join(os.getcwd(), "tests", ".tmp")
    os.makedirs(base_dir, exist_ok=True)
    test_dir = os.path.join(base_dir, f"run_{uuid.uuid4().hex}")
    os.makedirs(test_dir, exist_ok=True)
    try:
        yield test_dir
    finally:
        shutil.rmtree(test_dir, ignore_errors=True)


class FormPilotTests(unittest.TestCase):
    def test_calculate_payroll_is_deterministic(self):
        payroll = calculate_payroll(35.30, 10.0)
        self.assertEqual(payroll["gross_pay"], 353.00)
        self.assertEqual(payroll["ahv"], 18.71)
        self.assertEqual(payroll["alv"], 3.88)
        self.assertEqual(payroll["net_pay"], 330.41)

    def test_month_export_values_cover_all_months(self):
        self.assertEqual(set(MONTH_EXPORT_VALUES.keys()), set(range(1, 13)))
        self.assertEqual(MONTH_EXPORT_VALUES[3], "/M\u92dcz")

    def test_build_form_payload_maps_core_fields(self):
        month_data = {
            "month": "2026-04",
            "year": "2026",
            "month_number": 4,
            "month_export_value": "/April",
            "total_hours": 12.5,
            "report_total_chf": 441.25,
            "gross_pay": 441.25,
            "ahv_deduction": 23.39,
            "alv_deduction": 4.85,
            "net_pay": 413.01,
        }
        personal_data = {
            "insured_name": "Max Muster",
            "ahv_number": "756.1234.5678.97",
            "street": "Musterstrasse 1",
            "plz_ort": "8000 Zuerich",
            "iban": "CH93 0076 2011 6238 5295 7",
            "mitteilungsnummer": "MT-000001",
        }

        payload = build_form_payload(month_data, personal_data)

        self.assertEqual(payload["1NameVorname"], "Max Muster")
        self.assertEqual(payload["11AHVNr"], "756.1234.5678.97")
        self.assertEqual(payload["1Strasse"], "Musterstrasse 1")
        self.assertEqual(payload["1PLZOrt"], "8000 Zuerich")
        self.assertEqual(payload["3_Mitteilungsnummer"], "MT-000001")
        self.assertEqual(payload["3_IBAN"], "CH93 0076 2011 6238 5295 7")
        self.assertEqual(payload["Abrechnungsperiode_Jahr"], "2026")
        self.assertEqual(payload["Monat"], "/April")
        self.assertEqual(payload["Anzahl_Stunden"], "12.5")
        self.assertEqual(payload["Stundenlohn"], "35.30")
        self.assertEqual(payload["Grundlohn"], "441.25")
        self.assertEqual(payload["Total_Auszahlung"], "441.25")
        self.assertEqual(payload["Spesen3"], "441.25")

    def test_load_profile_supports_nested_user_schema(self):
        nested_profile = {
            "insuredPerson": {
                "fullName": "Noah Meier",
                "dateOfBirth": "1997-09-24",
                "ahvNumber": "756.8888.4321.09",
                "street": "Seestrasse 44",
                "zip": "6005",
                "city": "Luzern",
            },
            "invoiceIssuer": {
                "sameAsInsuredPerson": False,
                "fullName": "Sabine Meier",
                "email": "sabine.meier.guardian@example.com",
                "street": "Seestrasse 44",
                "zip": "6005",
                "city": "Luzern",
            },
            "billing": {
                "gln": "7601001000015",
                "referenceNumber": "IV-AB-2025-000932",
                "iban": "CH5604835012345678009",
                "accountHolderFullName": "Sabine Meier",
                "accountHolderStreet": "Seestrasse 44",
                "accountHolderZip": "6005",
                "accountHolderCity": "Luzern",
            },
        }

        with workspace_tempdir() as temp_dir:
            profile_path = os.path.join(temp_dir, "profile.json")
            with open(profile_path, "w", encoding="utf-8") as file:
                json.dump(nested_profile, file)

            normalized = load_profile(profile_path)

        self.assertEqual(normalized["insured_name"], "Noah Meier")
        self.assertEqual(normalized["insured_birth_date"], "1997-09-24")
        self.assertEqual(normalized["ahv_number"], "756.8888.4321.09")
        self.assertEqual(normalized["street"], "Seestrasse 44")
        self.assertEqual(normalized["plz_ort"], "6005 Luzern")
        self.assertEqual(normalized["mitteilungsnummer"], "IV-AB-2025-000932")
        self.assertEqual(normalized["iban"], "CH5604835012345678009")
        self.assertEqual(normalized["invoice_issuer_name"], "Sabine Meier")
        self.assertEqual(normalized["invoice_issuer_email"], "sabine.meier.guardian@example.com")
        self.assertEqual(normalized["payment_name"], "Sabine Meier")
        self.assertEqual(normalized["gln"], "7601001000015")

    def test_load_profile_rejects_missing_required_fields(self):
        with workspace_tempdir() as temp_dir:
            profile_path = os.path.join(temp_dir, "profile.json")
            with open(profile_path, "w", encoding="utf-8") as file:
                json.dump({"insured_name": "Only Name"}, file)

            with self.assertRaises(ValueError):
                load_profile(profile_path)

    def test_non_interactive_auto_fill_never_calls_input(self):
        with workspace_tempdir() as temp_dir:
            profile_path = os.path.join(temp_dir, "profile.json")
            with open(profile_path, "w", encoding="utf-8") as file:
                json.dump(
                    {
                        "insured_name": "Max Muster",
                        "ahv_number": "756.1234.5678.97",
                        "street": "Musterstrasse 1",
                        "plz_ort": "8000 Zuerich",
                        "iban": "CH93 0076 2011 6238 5295 7",
                        "mitteilungsnummer": "MT-000001",
                    },
                    file,
                )

            output_path = os.path.join(temp_dir, "out.pdf")
            with patch("iv_agent.form_pilot.get_assistant_hours", return_value=8.0), patch(
                "iv_agent.form_pilot.fill_form"
            ) as fill_form_mock, patch("builtins.input", side_effect=AssertionError("input was called")):
                resolved = fill_assistenz_form_auto(
                    template_pdf_path="template.pdf",
                    month="2026-04",
                    profile_path=profile_path,
                    output_path=output_path,
                    preview=False,
                )

            self.assertEqual(resolved, output_path)
            fill_form_mock.assert_called_once()

    @unittest.skipUnless(
        os.path.exists(os.environ.get("IV_AGENT_TEMPLATE_PDF", r"c:\Users\trxqz\Desktop\318.536_D_Rechnung_AB_01_2025_V1.pdf")),
        "Template PDF not available for integration smoke test",
    )
    def test_mapping_smoke_fills_core_fields_with_real_template(self):
        template_path = os.environ.get("IV_AGENT_TEMPLATE_PDF", r"c:\Users\trxqz\Desktop\318.536_D_Rechnung_AB_01_2025_V1.pdf")

        with workspace_tempdir() as temp_dir:
            profile_path = os.path.join(temp_dir, "profile.json")
            output_path = os.path.join(temp_dir, "filled.pdf")

            with open(profile_path, "w", encoding="utf-8") as file:
                json.dump(
                    {
                        "insured_name": "Max Muster",
                        "ahv_number": "756.1234.5678.97",
                        "street": "Musterstrasse 1",
                        "plz_ort": "8000 Zuerich",
                        "iban": "CH93 0076 2011 6238 5295 7",
                        "mitteilungsnummer": "MT-000001",
                    },
                    file,
                )

            with patch("iv_agent.form_pilot.get_assistant_hours", return_value=9.5):
                fill_assistenz_form_auto(
                    template_pdf_path=template_path,
                    month="2026-04",
                    profile_path=profile_path,
                    output_path=output_path,
                    preview=False,
                )

            fields = PdfReader(output_path).get_fields() or {}
            self.assertEqual(fields["1NameVorname"].get("/V"), "Max Muster")
            self.assertEqual(fields["11AHVNr"].get("/V"), "756.1234.5678.97")
            self.assertEqual(fields["3_IBAN"].get("/V"), "CH93 0076 2011 6238 5295 7")
            self.assertEqual(str(fields["Monat"].get("/V")), "/April")
            self.assertEqual(fields["Anzahl_Stunden"].get("/V"), "9.5")
            self.assertEqual(fields["Total_Auszahlung"].get("/V"), "335.35")
            self.assertEqual(fields["Spesen3"].get("/V"), "335.35")
            self.assertEqual(fields["2_Name_Vorname"].get("/V"), "")
            self.assertEqual(fields["3_GLN"].get("/V"), "")

    def test_get_month_data_supports_march_export_value(self):
        with patch("iv_agent.form_pilot.get_assistant_hours", return_value=1.0):
            month_data = get_month_data("2026-03")

        self.assertEqual(month_data["month_export_value"], "/M\u92dcz")

    def test_get_assistant_daily_hours_groups_hours_by_date(self):
        events = [
            {
                "date": "2026-04-01",
                "category": "assistant",
                "assistant_hours": {
                    "koerperpflege": 1.0,
                    "mahlzeiten_eingeben": 0.5,
                    "mahlzeiten_zubereiten": 0.0,
                    "begleitung_therapie": 0.25,
                },
            },
            {
                "date": "2026-04-01",
                "category": "assistant",
                "assistant_hours": {
                    "koerperpflege": 0.25,
                    "mahlzeiten_eingeben": 0.0,
                    "mahlzeiten_zubereiten": 0.75,
                    "begleitung_therapie": 0.0,
                },
            },
            {
                "date": "2026-04-02",
                "category": "assistant",
                "assistant_hours": {
                    "koerperpflege": 0.0,
                    "mahlzeiten_eingeben": 1.0,
                    "mahlzeiten_zubereiten": 0.0,
                    "begleitung_therapie": 0.0,
                },
            },
            {
                "date": "2026-04-03",
                "category": "transport",
                "assistant_hours": {},
            },
        ]

        with patch("iv_agent.form_pilot.get_events", return_value=events):
            rows = get_assistant_daily_hours("2026-04")

        self.assertEqual(
            rows,
            [
                {
                    "date": "2026-04-01",
                    "koerperpflege": 1.25,
                    "mahlzeiten_eingeben": 0.5,
                    "mahlzeiten_zubereiten": 0.75,
                    "begleitung_therapie": 0.25,
                    "total_hours": 2.75,
                },
                {
                    "date": "2026-04-02",
                    "koerperpflege": 0.0,
                    "mahlzeiten_eingeben": 1.0,
                    "mahlzeiten_zubereiten": 0.0,
                    "begleitung_therapie": 0.0,
                    "total_hours": 1.0,
                },
            ],
        )

    def test_build_stundenblatt_payload_maps_real_template_fields(self):
        template_fields = {
            "DatumRow1": {},
            "DatumRow5": {},
            "StundenKörperpflege": {},
            "StundenKörperpflege_2": {},
            "StundenMahlzeiten zubereiten": {},
            "StundenMahlzeiten zubereiten_2": {},
            "StundenMahlzeiten eingeben": {},
            "StundenMahlzeiten eingeben_2": {},
            "StundenBegleitung Therapie": {},
            "StundenBegleitung Therapie_2": {},
            "DatumRow25": {"/Kids": [object(), object()]},
        }
        events = [
            {
                "date": "2026-04-01",
                "category": "assistant",
                "assistant_hours": {
                    "koerperpflege": 1.0,
                    "mahlzeiten_eingeben": 0.5,
                    "mahlzeiten_zubereiten": 0.25,
                    "begleitung_therapie": 0.75,
                },
            },
            {
                "date": "2026-04-02",
                "category": "assistant",
                "assistant_hours": {
                    "koerperpflege": 0.5,
                    "mahlzeiten_eingeben": 0.0,
                    "mahlzeiten_zubereiten": 0.0,
                    "begleitung_therapie": 0.0,
                },
            },
        ]

        with patch("iv_agent.form_pilot.get_assistant_hours", return_value=3.0), patch(
            "iv_agent.form_pilot.get_events", return_value=events
        ), patch(
            "iv_agent.form_pilot.get_assistant_hours_breakdown",
            return_value={
                "koerperpflege": 1.5,
                "mahlzeiten_eingeben": 0.5,
                "mahlzeiten_zubereiten": 0.25,
                "begleitung_therapie": 0.75,
            },
        ):
            payload = build_stundenblatt_payload("2026-04", template_fields=template_fields)

        self.assertEqual(payload["DatumRow1"], "01.04.2026")
        self.assertEqual(payload["StundenKörperpflege"], "1.00")
        self.assertEqual(payload["StundenMahlzeiten eingeben"], "0.50")
        self.assertEqual(payload["StundenMahlzeiten zubereiten"], "0.25")
        self.assertEqual(payload["StundenBegleitung Therapie"], "0.75")
        self.assertEqual(payload["DatumRow5"], "02.04.2026")
        self.assertEqual(payload["StundenKörperpflege_2"], "0.50")
        self.assertNotIn("DatumRow25", payload)

    def test_build_rechnung_payload_maps_modern_template_fields(self):
        template_fields = {
            "Text1": {},
            "2026": {},
            "Pitaqi DritaRow1": {},
            "Beschreibung der erbrachten Leistung z B administrative Unterstützung Bitte auswählen": {},
            "Anz Std Min in 1100h Bitte auswählen": {},
            "CHF 3500 Bitte auswählen": {},
            "Beilagen Bestätigung der bezahlten Rechnung inkl Unterschrift des Leistungs erbringers Bitte auswählen": {},
            "Bemerkungen": {},
            "Text2": {},
        }
        personal_data = {
            "insured_name": "Noah Meier",
            "insured_birth_date": "1997-09-24",
            "street": "Seestrasse 44",
            "plz_ort": "6005 Luzern",
        }

        with patch("iv_agent.form_pilot.get_assistant_hours", return_value=4.5):
            payload = build_rechnung_payload(
                month="2026-04",
                personal_data=personal_data,
                total_hours=4.5,
                template_fields=template_fields,
                invoice_date=date(2026, 4, 12),
            )

        self.assertEqual(payload["Text1"], "Rechnungsdatum: 12.04.2026")
        self.assertEqual(payload["2026"], "04")
        self.assertEqual(payload["Pitaqi DritaRow1"], "Pitaqi Drita")
        self.assertEqual(
            payload["Beschreibung der erbrachten Leistung z B administrative Unterstützung Bitte auswählen"],
            "Assistenzleistung Wohnen",
        )
        self.assertEqual(payload["Anz Std Min in 1100h Bitte auswählen"], "4.50")
        self.assertEqual(payload["CHF 3500 Bitte auswählen"], "CHF 157.50")
        self.assertEqual(payload["Text2"], "157.50")
        self.assertIn("Noah Meier", payload["Bemerkungen"])
        self.assertIn("24.09.1997", payload["Bemerkungen"])

    def test_fill_assistenz_dual_form_auto_merges_intermediate_pdfs(self):
        with workspace_tempdir() as temp_dir:
            profile_path = os.path.join(temp_dir, "profile.json")
            stundenblatt_template_path = os.path.join(temp_dir, "stundenblatt.pdf")
            rechnung_template_path = os.path.join(temp_dir, "rechnung.pdf")
            output_path = os.path.join(temp_dir, "merged.pdf")

            with open(profile_path, "w", encoding="utf-8") as file:
                json.dump(
                    {
                        "insured_name": "Max Muster",
                        "ahv_number": "756.1234.5678.97",
                        "street": "Musterstrasse 1",
                        "plz_ort": "8000 Zuerich",
                        "iban": "CH93 0076 2011 6238 5295 7",
                        "mitteilungsnummer": "MT-000001",
                    },
                    file,
                )

            for template_path in (stundenblatt_template_path, rechnung_template_path):
                writer = PdfWriter()
                writer.add_blank_page(width=200, height=200)
                with open(template_path, "wb") as file:
                    writer.write(file)

            def fake_fill_form_to_bytes(_template_path, _payload):
                writer = PdfWriter()
                try:
                    writer.add_blank_page(width=200, height=200)
                    with io.BytesIO() as buffer:
                        writer.write(buffer)
                        return buffer.getvalue()
                finally:
                    writer.close()

            with patch(
                "iv_agent.form_pilot.build_stundenblatt_payload",
                return_value={"datum_1": "01.04.2026", "_meta": {"total_hours": 4.5}},
            ), patch(
                "iv_agent.form_pilot.build_rechnung_payload",
                return_value={"row_1": "Assistenzleistung Wohnen", "_meta": {"total_amount_chf": 157.5}},
            ), patch("iv_agent.form_pilot.fill_form_to_bytes", side_effect=fake_fill_form_to_bytes):
                resolved = fill_assistenz_dual_form_auto(
                    stundenblatt_template_pdf_path=stundenblatt_template_path,
                    rechnung_template_pdf_path=rechnung_template_path,
                    month="2026-04",
                    profile_path=profile_path,
                    output_path=output_path,
                )

            self.assertEqual(resolved, output_path)
            self.assertTrue(os.path.exists(output_path))
            self.assertEqual(len(PdfReader(output_path).pages), 2)


if __name__ == "__main__":
    unittest.main()
