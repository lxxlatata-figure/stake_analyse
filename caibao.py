import argparse
import concurrent.futures
import json
import os
import re
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from PyPDF2 import PdfReader
from flask import Flask, jsonify, render_template, request, send_from_directory


BASE_DIR = Path(__file__).resolve().parent
PDF_DIR = BASE_DIR / "pdfs"
DATA_DIR = BASE_DIR / "data"
TEMPLATE_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
RESULTS_FILE = DATA_DIR / "report_results.json"
COMPANY_FILE = BASE_DIR / "hangzhou_companies.txt"

CNINFO_QUERY_URL = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
CNINFO_DOWNLOAD_BASE = "https://static.cninfo.com.cn/"
DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
MAX_FETCH_WORKERS = int(os.getenv("MAX_FETCH_WORKERS", "10"))
MAX_ANALYZE_WORKERS = int(os.getenv("MAX_ANALYZE_WORKERS", "6"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "25"))
FAST_PAGE_LIMIT = int(os.getenv("FAST_PAGE_LIMIT", "16"))
PAGE_CHAR_LIMIT = int(os.getenv("PAGE_CHAR_LIMIT", "2600"))

REPORT_KEYWORDS = ("年度报告", "半年度报告", "第一季度报告", "第三季度报告")
EXCLUDE_KEYWORDS = (
    "摘要",
    "英文版",
    "提示性公告",
    "披露提示",
    "补充公告",
    "制度",
    "审计报告",
    "法律意见书",
    "取消",
    "已取消",
)
ANCHOR_KEYWORDS = (
    "主要会计数据和财务指标",
    "管理层讨论与分析",
    "报告期内公司经营情况概述",
    "主营业务分析",
    "公司面临的风险和应对措施",
)
SUMMARY_KEYWORDS = (
    "营业收入",
    "归属于上市公司股东的净利润",
    "扣除非经常性损益的净利润",
    "经营活动产生的现金流量净额",
    "基本每股收益",
    "加权平均净资产收益率",
    "资产负债率",
    "研发投入",
    "毛利率",
    "风险",
    "同比",
)

FILE_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()

PIPELINE_STATE = {
    "status": "idle",
    "phase": "waiting",
    "message": "等待执行",
    "fetch_total": 0,
    "fetched": 0,
    "analyze_total": 0,
    "analyzed": 0,
    "selected_limit": "all",
    "last_run": None,
}

app = Flask(__name__, template_folder=str(TEMPLATE_DIR), static_folder=str(STATIC_DIR))
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0


def ensure_dirs() -> None:
    PDF_DIR.mkdir(exist_ok=True)
    DATA_DIR.mkdir(exist_ok=True)
    TEMPLATE_DIR.mkdir(exist_ok=True)
    STATIC_DIR.mkdir(exist_ok=True)


def load_company_codes(limit: Optional[int] = None) -> List[str]:
    with COMPANY_FILE.open("r", encoding="utf-8") as file:
        codes = [line.strip() for line in file if line.strip()]
    return codes[:limit] if limit else codes


def _read_results_unlocked() -> List[Dict]:
    if not RESULTS_FILE.exists():
        return []
    with RESULTS_FILE.open("r", encoding="utf-8") as file:
        return json.load(file)


def _write_results_unlocked(records: List[Dict]) -> None:
    with RESULTS_FILE.open("w", encoding="utf-8") as file:
        json.dump(records, file, ensure_ascii=False, indent=2)


def read_results() -> List[Dict]:
    ensure_dirs()
    with FILE_LOCK:
        return _read_results_unlocked()


def clear_results() -> None:
    ensure_dirs()
    with FILE_LOCK:
        _write_results_unlocked([])


def upsert_record(record: Dict) -> None:
    ensure_dirs()
    with FILE_LOCK:
        records = _read_results_unlocked()
        record.setdefault("updated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        key = (record.get("stock_code"), record.get("title"), record.get("published_at"))
        replaced = False
        for index, item in enumerate(records):
            item_key = (item.get("stock_code"), item.get("title"), item.get("published_at"))
            if item_key == key:
                merged = dict(item)
                merged.update(record)
                records[index] = merged
                replaced = True
                break
        if not replaced:
            records.append(record)
        records.sort(
            key=lambda item: (
                item.get("updated_at", ""),
                item.get("announcement_time", 0),
            ),
            reverse=True,
        )
        _write_results_unlocked(records)


def update_state(**kwargs) -> None:
    with STATE_LOCK:
        PIPELINE_STATE.update(kwargs)


def get_state() -> Dict:
    with STATE_LOCK:
        return dict(PIPELINE_STATE)


def reset_state() -> None:
    with STATE_LOCK:
        PIPELINE_STATE.update({
            "status": "idle",
            "phase": "waiting",
            "message": "等待执行",
            "fetch_total": 0,
            "fetched": 0,
            "analyze_total": 0,
            "analyzed": 0,
            "selected_limit": "all",
        })


def cninfo_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Referer": "https://www.cninfo.com.cn/",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/plain, */*",
    }


def normalize_text(text: str) -> str:
    text = (text or "").replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def strip_html(text: str) -> str:
    return re.sub(r"<.*?>", "", text or "").strip()


def safe_filename(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|]+', "_", name)
    return name[:180].strip(" ._")


def format_ts(timestamp_ms: Optional[int]) -> str:
    if not timestamp_ms:
        return ""
    return datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")


def should_keep_report(title: str) -> bool:
    if not any(keyword in title for keyword in REPORT_KEYWORDS):
        return False
    if any(keyword in title for keyword in EXCLUDE_KEYWORDS):
        return False
    if "关于" in title and "报告" not in title[:20]:
        return False
    return True


def query_latest_report(stock_code: str, session: requests.Session) -> Optional[Dict]:
    announcements: List[Dict] = []
    seen_ids = set()
    for keyword in REPORT_KEYWORDS:
        payload = {
            "pageNum": 1,
            "pageSize": 20,
            "column": "szse",
            "tabName": "fulltext",
            "plate": "",
            "stock": "",
            "searchkey": f"{stock_code} {keyword}",
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": "",
            "sortName": "time",
            "sortType": "desc",
            "isHLtitle": "true",
        }
        response = session.post(CNINFO_QUERY_URL, data=payload, headers=cninfo_headers(), timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        for item in response.json().get("announcements", []) or []:
            announcement_id = item.get("announcementId")
            if announcement_id in seen_ids:
                continue
            seen_ids.add(announcement_id)
            announcements.append(item)

    candidates = [
        item
        for item in announcements
        if item.get("secCode") == stock_code and should_keep_report(strip_html(item.get("announcementTitle", "")))
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda item: item.get("announcementTime", 0), reverse=True)
    top = candidates[0]
    return {
        "stock_code": top.get("secCode", stock_code),
        "company_name": strip_html(top.get("secName", "")),
        "title": strip_html(top.get("announcementTitle", "")),
        "published_at": format_ts(top.get("announcementTime")),
        "announcement_time": top.get("announcementTime"),
        "adjunct_url": top.get("adjunctUrl", ""),
        "stage": "queued",
    }


def build_download_url(adjunct_url: str) -> str:
    if adjunct_url.startswith(("http://", "https://")):
        return adjunct_url
    return f"{CNINFO_DOWNLOAD_BASE}{adjunct_url.lstrip('/')}"


def download_report(report: Dict, session: requests.Session) -> Dict:
    filename = safe_filename(f"{report['company_name']}_{report['title']}") + ".pdf"
    path = PDF_DIR / filename
    report["pdf_filename"] = filename
    report["pdf_path"] = str(path)
    if not path.exists() or path.stat().st_size == 0:
        response = session.get(
            build_download_url(report["adjunct_url"]),
            headers=cninfo_headers(),
            timeout=REQUEST_TIMEOUT,
            stream=True,
        )
        response.raise_for_status()
        with path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    file.write(chunk)
    report["stage"] = "downloaded"
    report["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return report


def choose_page_indexes(page_count: int) -> List[int]:
    head = list(range(min(8, page_count)))
    body = list(range(8, min(FAST_PAGE_LIMIT, page_count)))
    tail = list(range(max(page_count - 2, 0), page_count))
    ordered = []
    seen = set()
    for idx in head + body + tail:
        if 0 <= idx < page_count and idx not in seen:
            seen.add(idx)
            ordered.append(idx)
    return ordered


def extract_fast_text(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    texts = []
    for idx in choose_page_indexes(len(reader.pages)):
        page_text = normalize_text(reader.pages[idx].extract_text() or "")
        if page_text:
            texts.append(f"[PAGE {idx + 1}]\n{page_text[:PAGE_CHAR_LIMIT]}")
    return "\n\n".join(texts)


def split_sentences(text: str) -> List[str]:
    rough = re.split(r"[。；;！？!\n\r]", text)
    return [item.strip() for item in rough if len(item.strip()) >= 10]


def metric_lines(text: str, limit: int = 12) -> List[str]:
    lines = [normalize_text(line).replace(" ", "") for line in text.splitlines()]
    output = []
    seen = set()
    for index, line in enumerate(lines):
        if len(line) < 6 or len(line) > 160:
            continue
        joined = line
        if not re.search(r"[\d%]", line) and index + 1 < len(lines):
            joined = f"{line} {lines[index + 1]}"
        if not any(keyword in joined for keyword in SUMMARY_KEYWORDS):
            continue
        if not re.search(r"[\d%]", joined):
            continue
        short = joined[:160]
        if short not in seen:
            seen.add(short)
            output.append(short)
        if len(output) >= limit:
            break
    return output


def keyword_sentences(text: str, limit: int = 10) -> List[str]:
    output = []
    seen = set()
    for sentence in split_sentences(text):
        if len(sentence) > 150:
            continue
        if any(keyword in sentence for keyword in SUMMARY_KEYWORDS) or any(anchor in sentence for anchor in ANCHOR_KEYWORDS):
            if sentence not in seen:
                seen.add(sentence)
                output.append(sentence)
        if len(output) >= limit:
            break
    return output


def targeted_windows(text: str, limit: int = 4, window_size: int = 1200) -> List[str]:
    windows = []
    for anchor in ANCHOR_KEYWORDS:
        pos = text.find(anchor)
        if pos != -1:
            windows.append(normalize_text(text[pos:pos + window_size]))
        if len(windows) >= limit:
            break
    if not windows:
        windows.append(normalize_text(text[:window_size]))
    return [item for item in windows if item]


def build_local_summary(text: str) -> str:
    metrics = "\n".join(f"- {item}" for item in metric_lines(text)) or "- 暂未提取到稳定财务指标"
    highlights = "\n".join(f"- {item}" for item in keyword_sentences(text)) or "- 暂未提取到明确经营要点"
    windows = "\n".join(f"[片段{i + 1}] {item[:260]}" for i, item in enumerate(targeted_windows(text)))
    return f"核心财务指标：\n{metrics}\n\n经营与风险要点：\n{highlights}\n\n关键片段：\n{windows}"[:4200]


def parse_deepseek_json(content: str) -> Dict:
    content = content.strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", content, re.S)
        if match:
            return json.loads(match.group(0))
    return {
        "price_view": "震荡",
        "expected_range": "-3% ~ +3%",
        "confidence": 35,
        "short_reason": "模型未返回标准 JSON，已回退解析。",
        "bull_points": [],
        "bear_points": [],
        "price_prediction": content[:500],
    }


def analyze_with_deepseek(report: Dict, extracted_text: str, local_summary: str) -> Dict:
    if not DEEPSEEK_API_KEY:
        return {
            "price_view": "未配置",
            "expected_range": "未知",
            "confidence": 0,
            "short_reason": "未设置 DEEPSEEK_API_KEY。",
            "bull_points": [],
            "bear_points": [],
            "price_prediction": "当前未执行 DeepSeek 分析。",
        }

    sample_text = "\n\n".join(targeted_windows(extracted_text, limit=5, window_size=1300))[:5200]
    prompt = f"""
你是一名A股财报分析助手。请只输出 JSON，不要加任何解释。

任务：基于财报摘要，给出未来1-3个月的股价预估倾向。这里不是绝对价格预测，而是相对当前股价的方向与区间判断。

JSON 字段固定如下：
{{
  "price_view": "偏多/中性/承压",
  "expected_range": "例如 -8% ~ +5%",
  "confidence": 0-100,
  "short_reason": "一句话概括",
  "bull_points": ["最多3条利多因素"],
  "bear_points": ["最多3条利空因素"],
  "price_prediction": "80到160字，重点写股价预估逻辑和需要关注的触发因素"
}}

公司：{report.get('company_name', '')}
代码：{report.get('stock_code', '')}
财报：{report.get('title', '')}
日期：{report.get('published_at', '')}

摘要：
{local_summary}

关键文本：
{sample_text}
""".strip()

    response = requests.post(
        DEEPSEEK_URL,
        headers={
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": "deepseek-chat",
            "temperature": 0.1,
            "max_tokens": 360,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=REQUEST_TIMEOUT + 20,
    )
    response.raise_for_status()
    return parse_deepseek_json(response.json()["choices"][0]["message"]["content"])


def fetch_reports(limit: Optional[int], force: bool) -> List[Dict]:
    codes = load_company_codes(limit=limit)
    update_state(phase="fetching", message="正在抓取财报", fetch_total=len(codes), fetched=0)
    fetched_reports: List[Dict] = []

    def worker(code: str) -> Optional[Dict]:
        with requests.Session() as session:
            report = query_latest_report(code, session)
            if not report:
                return None
            report = download_report(report, session)
            if not force:
                for item in read_results():
                    if (
                        item.get("stock_code") == report.get("stock_code")
                        and item.get("title") == report.get("title")
                        and item.get("published_at") == report.get("published_at")
                        and item.get("stage") == "done"
                    ):
                        report["stage"] = "done"
                        report["local_summary"] = item.get("local_summary", "")
                        report["deepseek_result"] = item.get("deepseek_result", {})
                        break
            upsert_record(report)
            return report

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as executor:
        futures = [executor.submit(worker, code) for code in codes]
        done_count = 0
        for future in concurrent.futures.as_completed(futures):
            done_count += 1
            update_state(fetched=done_count)
            try:
                item = future.result()
                if item:
                    fetched_reports.append(item)
            except Exception:
                continue
    return fetched_reports


def analyze_reports(reports: List[Dict], force: bool) -> None:
    todo = [item for item in reports if force or item.get("stage") != "done"]
    update_state(phase="analyzing", message="正在分析财报", analyze_total=len(todo), analyzed=0)

    def worker(report: Dict) -> Dict:
        record = dict(report)
        record["stage"] = "analyzing"
        record["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        upsert_record(record)
        try:
            extracted_text = extract_fast_text(Path(record["pdf_path"]))
            record["text_length"] = len(extracted_text)
            record["local_summary"] = build_local_summary(extracted_text)
            record["deepseek_result"] = analyze_with_deepseek(record, extracted_text, record["local_summary"])
            record["stage"] = "done"
            record["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        except Exception as exc:
            record["stage"] = "error"
            record["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record["deepseek_result"] = {
                "price_view": "异常",
                "expected_range": "未知",
                "confidence": 0,
                "short_reason": f"分析失败：{exc}",
                "bull_points": [],
                "bear_points": ["任务异常"],
                "price_prediction": "该财报分析失败。",
            }
        upsert_record(record)
        return record

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_ANALYZE_WORKERS) as executor:
        futures = [executor.submit(worker, item) for item in todo]
        completed = 0
        for _ in concurrent.futures.as_completed(futures):
            completed += 1
            update_state(analyzed=completed)


def run_pipeline(force: bool = False, limit_value: str = "all") -> None:
    limit = None if limit_value == "all" else int(limit_value)
    update_state(
        status="running",
        phase="booting",
        message="任务启动中",
        fetch_total=0,
        fetched=0,
        analyze_total=0,
        analyzed=0,
        selected_limit=limit_value,
        last_run=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    try:
        reports = fetch_reports(limit=limit, force=force)
        analyze_reports(reports, force=force)
        update_state(status="idle", phase="finished", message=f"任务完成，共抓取 {len(reports)} 份财报")
    except Exception as exc:
        update_state(status="error", phase="failed", message=f"执行失败：{exc}")


def launch_pipeline_async(force: bool = False, limit_value: str = "all") -> bool:
    if get_state()["status"] == "running":
        return False
    thread = threading.Thread(target=run_pipeline, kwargs={"force": force, "limit_value": limit_value}, daemon=True)
    thread.start()
    return True


@app.route("/")
def index():
    return render_template("index.html")


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/api/state")
def api_state():
    return jsonify(get_state())


@app.route("/api/reports")
def api_reports():
    return jsonify(read_results())


@app.route("/api/run", methods=["POST"])
def api_run():
    payload = request.get_json(silent=True) or {}
    limit_value = str(payload.get("limit", "all"))
    if limit_value not in {"5", "10", "all"}:
        limit_value = "all"
    started = launch_pipeline_async(force=bool(payload.get("force")), limit_value=limit_value)
    if started:
        return jsonify({"ok": True, "message": f"后台任务已启动，抓取范围：{limit_value}。"})
    return jsonify({"ok": False, "message": "已有任务在执行中。"})


@app.route("/api/clear", methods=["POST"])
def api_clear():
    if get_state()["status"] == "running":
        return jsonify({"ok": False, "message": "当前任务仍在执行中，请等待完成后再清空。"}), 409
    clear_results()
    reset_state()
    return jsonify({"ok": True, "message": "当前列表和 JSON 缓存已清空。"})


@app.route("/pdfs/<path:filename>")
def serve_pdf(filename: str):
    return send_from_directory(PDF_DIR, filename, as_attachment=False)


def main() -> None:
    ensure_dirs()
    parser = argparse.ArgumentParser(description="杭州上市公司财报抓取与 DeepSeek 分析平台")
    parser.add_argument("--run-once", action="store_true", help="执行一次任务后退出")
    parser.add_argument("--limit", default="all", choices=["5", "10", "all"], help="抓取数量")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=5000, type=int)
    args = parser.parse_args()

    if args.run_once:
        run_pipeline(force=False, limit_value=args.limit)
        return

    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
