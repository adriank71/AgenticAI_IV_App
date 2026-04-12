import os

try:
    from .calendar_manager import (
        add_event,
        delete_event,
        display_month,
        export_month_plan,
        get_assistant_hours,
        get_events,
    )
    from .form_pilot import (
        calculate_payroll,
        fill_assistenz_form,
        generate_report_filename,
    )
except ImportError:
    from calendar_manager import (
        add_event,
        delete_event,
        display_month,
        export_month_plan,
        get_assistant_hours,
        get_events,
    )
    from form_pilot import (
        calculate_payroll,
        fill_assistenz_form,
        generate_report_filename,
    )


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")


def prompt_float(label: str, default: float = 0.0) -> float:
    raw_value = input(label).strip()
    if not raw_value:
        return default
    return float(raw_value)


def calendar_menu() -> None:
    while True:
        print("\nCalendar Manager")
        print("1. Add event")
        print("2. View month")
        print("3. Export month plan")
        print("4. Delete event")
        print("5. Back")

        choice = input("Choose an option: ").strip()

        if choice == "1":
            date = input("Date (YYYY-MM-DD): ").strip()
            time = input("Time (HH:MM): ").strip()
            category = input("Category (assistant/transport/other): ").strip()
            title = input("Title: ").strip()
            notes = input("Notes (optional): ").strip()
            hours = prompt_float("Hours (assistant entries only, optional): ", 0.0)
            event = add_event(date, time, category, title, notes, hours)
            print(f"Added event {event['id']}")
        elif choice == "2":
            month = input("Month (YYYY-MM): ").strip()
            display_month(month)
            print(f"Assistant hours: {get_assistant_hours(month):.2f}")
        elif choice == "3":
            month = input("Month (YYYY-MM): ").strip()
            print(export_month_plan(month))
        elif choice == "4":
            month = input("Month (YYYY-MM, optional): ").strip()
            if month:
                events = get_events(month)
                for event in events:
                    print(f"{event['id']} | {event['date']} {event['time']} | {event['category']} | {event['title']}")
            event_id = input("Event ID to delete: ").strip()
            deleted = delete_event(event_id)
            if deleted:
                print("Event deleted")
            else:
                print("Event not found")
        elif choice == "5":
            return
        else:
            print("Invalid option")


def formpilot_menu() -> None:
    print("\nFormPilot")
    template_pdf_path = input("Template PDF path: ").strip()
    name = input("Report holder name: ").strip()
    month = input("Month (YYYY-MM): ").strip()
    assistant_name = input("Assistant name: ").strip()
    signature_date = input("Signature date (YYYY-MM-DD): ").strip()
    gross_hourly_rate = prompt_float("Gross hourly rate: ")

    default_hours = get_assistant_hours(month)
    raw_hours = input(f"Hours [{default_hours:.2f}]: ").strip()
    hours = float(raw_hours) if raw_hours else default_hours

    payroll = calculate_payroll(gross_hourly_rate, hours)
    filename = generate_report_filename(name, month)
    output_path = os.path.join(OUTPUT_DIR, filename)

    data = {
        "name": name,
        "month": month,
        "hours": f"{hours:.2f}",
        "gross_pay": f"{payroll['gross_pay']:.2f}",
        "ahv": f"{payroll['ahv']:.2f}",
        "alv": f"{payroll['alv']:.2f}",
        "net_pay": f"{payroll['net_pay']:.2f}",
        "assistant_name": assistant_name,
        "signature_date": signature_date,
    }

    fill_assistenz_form(template_pdf_path, output_path, data)
    print(f"Output path: {output_path}")


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    while True:
        print("\nIV Agent")
        print("1. Calendar Manager")
        print("2. FormPilot")
        print("3. Exit")

        choice = input("Choose an option: ").strip()

        if choice == "1":
            calendar_menu()
        elif choice == "2":
            formpilot_menu()
        elif choice == "3":
            break
        else:
            print("Invalid option")


if __name__ == "__main__":
    main()
