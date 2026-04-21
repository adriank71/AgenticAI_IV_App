import socket
import uuid
from flask import Flask, jsonify, request
from urllib.parse import quote

app = Flask(__name__)

# session_id -> list[str]
RECEIPTS: dict[str, list[str]] = {}


def lan_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


DESKTOP_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><title>Receipt scanner</title>
<style>
body{font-family:system-ui;max-width:640px;margin:40px auto;padding:0 16px}
.receipt{border:1px solid #ccc;padding:12px;margin:8px 0;border-radius:6px;white-space:pre-wrap}
img{border:1px solid #eee}
</style></head><body>
<h1>Scan receipt</h1>
<p>Open this on your phone (same wifi):</p>
<img src="https://api.qrserver.com/v1/create-qr-code/?size=240x240&data={QR_DATA}" alt="qr">
<p><a href="{SCAN_URL}">{SCAN_URL}</a></p>
<h2>Receipts</h2>
<div id="list"></div>
<script>
const sid = "{SID}";
async function poll(){
  const r = await fetch("/api/receipts/" + sid);
  const j = await r.json();
  document.getElementById("list").innerHTML =
    j.receipts.map((t,i)=>`<div class="receipt"><b>Receipt ${i+1}</b>\\n${t}</div>`).join("");
}
setInterval(poll, 2000); poll();
</script></body></html>"""

SCAN_HTML = """<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Scan receipt</title>
<style>body{font-family:system-ui;max-width:480px;margin:20px auto;padding:0 16px}
button,input{font-size:18px;padding:10px;width:100%;margin:8px 0}
#status{margin-top:12px;white-space:pre-wrap;font-family:monospace;font-size:12px}
</style></head><body>
<h2>Take photo of receipt</h2>
<input id="file" type="file" accept="image/*" capture="environment">
<div id="status"></div>
<script src="https://cdn.jsdelivr.net/npm/tesseract.js@5/dist/tesseract.min.js"></script>
<script>
const sid = "{SID}";
const status = document.getElementById("status");
document.getElementById("file").addEventListener("change", async (e) => {
  const f = e.target.files[0]; if(!f) return;
  status.textContent = "Running OCR... (this takes 10-30s)";
  try {
    const { data: { text } } = await Tesseract.recognize(f, "eng", {
      logger: m => { if(m.status==="recognizing text") status.textContent = "OCR " + Math.round(m.progress*100) + "%"; }
    });
    const parsed = parse(text);
    if(!parsed){ status.textContent = "Could not make sense of it. Try again."; return; }
    const r = await fetch("/api/receipts/" + sid, {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({text: parsed})
    });
    status.textContent = r.ok ? "Added! Check desktop.\\n\\n" + parsed : "Upload failed";
  } catch(err){ status.textContent = "Error: " + err.message; }
});
function parse(t){
  if(!t || t.trim().length < 10) return null;
  const lines = t.split(/\\n/).map(s=>s.trim()).filter(Boolean);
  const totalLine = lines.find(l => /total|summe|betrag|chf|eur|\\$/i.test(l) && /\\d/.test(l));
  const merchant = lines[0] || "unknown";
  const dateMatch = t.match(/\\b\\d{1,2}[./-]\\d{1,2}[./-]\\d{2,4}\\b/);
  if(!totalLine && !dateMatch) return null;
  return `Merchant: ${merchant}\\nDate: ${dateMatch ? dateMatch[0] : "?"}\\nTotal: ${totalLine || "?"}`;
}
</script></body></html>"""


@app.get("/")
def desktop():
    sid = uuid.uuid4().hex[:8]
    RECEIPTS[sid] = []
    scan_url = f"http://{lan_ip()}:5001/scan/{sid}"
    return DESKTOP_HTML.replace("{SID}", sid) \
        .replace("{SCAN_URL}", scan_url) \
        .replace("{QR_DATA}", quote(scan_url, safe=""))


@app.get("/scan/<sid>")
def scan(sid: str):
    return SCAN_HTML.replace("{SID}", sid)


@app.get("/api/receipts/<sid>")
def list_receipts(sid: str):
    return jsonify({"receipts": RECEIPTS.get(sid, [])})


@app.post("/api/receipts/<sid>")
def add_receipt(sid: str):
    text = (request.get_json(silent=True) or {}).get("text", "").strip()
    if not text:
        return jsonify({"error": "empty"}), 400
    RECEIPTS.setdefault(sid, []).append(text)
    return jsonify({"ok": True})


if __name__ == "__main__":
    print(f"Desktop: http://localhost:5001/")
    print(f"Phone:   http://{lan_ip()}:5001/  (must be on same wifi)")
    app.run(host="0.0.0.0", port=5001, debug=True)
