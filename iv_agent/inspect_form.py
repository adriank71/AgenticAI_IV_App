import sys

from pypdf import PdfReader


def inspect_form(template_path: str) -> None:
    reader = PdfReader(template_path)
    fields = reader.get_fields()
    if fields:
        for name, field in fields.items():
            print(f"Field: {name!r}")
            print(f"  Type:  {field.get('/FT')}")
            print(f"  Value: {field.get('/V')}")
            print()
    else:
        print("No AcroForm fields found.")


if __name__ == "__main__":
    template_path = sys.argv[1] if len(sys.argv) > 1 else "318_536_D_Rechnung_AB_01_2025_V1.pdf"
    inspect_form(template_path)
