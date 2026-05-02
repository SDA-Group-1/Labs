import os
import time
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 5))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")
API_BASE_URL = os.getenv("MZINGA_URL", "http://localhost:3000")
ADMIN_EMAIL = os.getenv("MZINGA_EMAIL")
ADMIN_PASSWORD = os.getenv("MZINGA_PASSWORD")

class MzingaAPIClient:
    def __init__(self):
        self.token = None

    def login(self):
        url = f"{API_BASE_URL}/api/users/login"
        payload = {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}
        response = requests.post(url, json=payload)
        response.raise_for_status()
        self.token = response.json().get("token")

    def request(self, method, endpoint, payload=None):
        if not self.token:
            self.login()
        
        url = f"{API_BASE_URL}{endpoint}"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        response = requests.request(method, url, json=payload, headers=headers)
        
        if response.status_code == 401:
            self.login()
            headers["Authorization"] = f"Bearer {self.token}"
            response = requests.request(method, url, json=payload, headers=headers)
            
        response.raise_for_status()
        return response.json()

class EmailService:
    def send_email(self, to_addresses, subject, html, cc_addresses=None, bcc_addresses=None):
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(to_addresses)
        if cc_addresses:
            msg["Cc"] = ", ".join(cc_addresses)
        
        msg.attach(MIMEText(html, "html"))
        all_recipients = to_addresses + (cc_addresses or []) + (bcc_addresses or [])
        
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.sendmail(EMAIL_FROM, all_recipients, msg.as_string())

def slate_to_html(nodes):
    html = ""
    for node in nodes or []:
        if node.get("type") == "paragraph":
            html += f"<p>{slate_to_html(node.get('children', []))}</p>"
        elif node.get("type") == "h1":
            html += f"<h1>{slate_to_html(node.get('children', []))}</h1>"
        elif node.get("type") == "h2":
            html += f"<h2>{slate_to_html(node.get('children', []))}</h2>"
        elif node.get("type") == "ul":
            html += f"<ul>{slate_to_html(node.get('children', []))}</ul>"
        elif node.get("type") == "li":
            html += f"<li>{slate_to_html(node.get('children', []))}</li>"
        elif node.get("type") == "link":
            url = node.get("url", "#")
            html += f'<a href="{url}">{slate_to_html(node.get("children", []))}</a>'
        elif "text" in node:
            text = node["text"]
            if node.get("bold"):
                text = f"<strong>{text}</strong>"
            if node.get("italic"):
                text = f"<em>{text}</em>"
            html += text
        else:
            html += slate_to_html(node.get("children", []))
    return html

def extract_emails(relationships):
    if not relationships:
        return []
    return [r["value"]["email"] for r in relationships if r.get("value") and r["value"].get("email")]

class CommunicationsWorker:
    def __init__(self):
        self.api = MzingaAPIClient()
        self.mailer = EmailService()

    def start(self):
        log.info(f"Worker REST started. Polling every {POLL_INTERVAL}s")
        while True:
            try:
                response = self.api.request("GET", "/api/communications?where[status][equals]=pending&depth=1")
                docs = response.get("docs", [])
                
                if not docs:
                    time.sleep(POLL_INTERVAL)
                    continue
                    
                for doc in docs:
                    self.process_document(doc)
            except Exception as e:
                log.error(f"Polling error: {e}")
                time.sleep(POLL_INTERVAL)

    def process_document(self, doc):
        doc_id = doc["id"]
        log.info(f"Processing communication {doc_id}")
        
        try:
            self.api.request("PATCH", f"/api/communications/{doc_id}", payload={"status": "processing"})
            
            to_emails = extract_emails(doc.get("tos"))
            cc_emails = extract_emails(doc.get("ccs"))
            bcc_emails = extract_emails(doc.get("bccs"))
            
            if not to_emails:
                raise ValueError("No valid 'to' email addresses found")
                
            html_body = slate_to_html(doc.get("body"))
            
            self.mailer.send_email(to_emails, doc.get("subject", ""), html_body, cc_emails, bcc_emails)
            
            self.api.request("PATCH", f"/api/communications/{doc_id}", payload={"status": "sent"})
            log.info(f"Communication {doc_id} sent successfully")
            
        except Exception as e:
            log.error(f"Failed to process communication {doc_id}: {e}")
            self.api.request("PATCH", f"/api/communications/{doc_id}", payload={"status": "failed"})

if __name__ == "__main__":
    worker = CommunicationsWorker()
    worker.start()