import csv
from datetime import datetime, timezone
from processmining.data_models import ProcessEvent

class FileInput:
    def read_ordered_events(self, input_path):
        events = []
        with open(input_path, newline="", encoding="utf-8") as input_file:
            reader = csv.DictReader(input_file)
            if reader.fieldnames is None or not {"case", "activity", "timestamp"}.issubset(
                    reader.fieldnames
            ):
                raise ValueError("CSV must contain case, activity, and timestamp columns")

            for row_number, row in enumerate(reader, start=2):
                case = (row.get("case") or "").strip()
                activity = (row.get("activity") or "").strip()
                timestamp_text = (row.get("timestamp") or "").strip()
                if not case or not activity or not timestamp_text:
                    raise ValueError(f"CSV row {row_number} has an empty required field")

                timestamp = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                events.append(
                    ProcessEvent(
                        case=case,
                        activity=activity,
                        timestamp=timestamp,
                        sequence=-1,
                    )
                )

        ordered_events = sorted(events, key=lambda event: event.timestamp)
        for sequence, event in enumerate(ordered_events):
            event.sequence = sequence
        return ordered_events
