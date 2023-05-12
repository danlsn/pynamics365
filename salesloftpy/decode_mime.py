import base64
from email.mime.text import MIMEText
from email.parser import Parser
from pathlib import Path
import json


mime_emails_path = Path("../data/salesloft-extract/full/mime_email_payloads")


def load_mime_emails():
    for path in mime_emails_path.glob("*.jsonl"):
        with open(path, "r") as f:
            for line in f:
                yield json.loads(line)


def main():
    mime_emails = load_mime_emails()
    decoded_mime_emails = []
    for mime_email in mime_emails:
        # b64 decode raw field
        raw = mime_email.get("raw", None)
        if raw is None:
            continue
        raw = raw.encode("utf-8")
        raw = base64.b64decode(raw)
        raw = raw.decode("utf-8")
        mime_email["raw"] = raw
        decoded_mime_emails.append(mime_email)
        email_msg = Parser().parsestr(raw)
        for part in email_msg.walk():
            if part.get_content_type() == "text/plain":
                mime_text = MIMEText(part.get_payload(), "plain")
        ...

    ...


if __name__ == "__main__":
    main()
