"""
VIP客服日报 - Web版
==================
Flask Web 界面，选日期点按钮即可生成日报。

启动方式：
  python app.py
  然后浏览器打开 http://localhost:5001
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import logging
import traceback
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template, request, jsonify, send_file

from config import OUTPUT_DIR
from service import generate_report

app = Flask(__name__)
logger = logging.getLogger("web_app")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


@app.route("/")
def index():
    today = datetime.now().strftime("%Y-%m-%d")
    return render_template("index.html", today=today)


@app.route("/generate", methods=["POST"])
def generate():
    date_str = request.json.get("date")
    if not date_str:
        return jsonify({"success": False, "error": "请选择日期"}), 400

    try:
        report_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"success": False, "error": "日期格式不正确"}), 400

    try:
        result = generate_report(report_date)

        txt_path = result.builder.save_text()

        xlsx_filename = None
        try:
            xlsx_path = result.builder.save_excel()
            xlsx_filename = Path(xlsx_path).name
        except Exception:
            logger.error(f"Excel生成失败:\n{traceback.format_exc()}")

        return jsonify({
            "success": True,
            "report": result.report_text,
            "txt_filename": Path(txt_path).name,
            "xlsx_filename": xlsx_filename,
            "stats": {
                "daily_count": len(result.daily_tickets),
                "pending_count": len(result.pending_tickets),
                "total_sessions": result.total_sessions,
            },
        })

    except Exception as e:
        logger.error(f"生成日报失败: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/download/<filename>")
def download(filename):
    safe_name = Path(filename).name
    filepath = Path(OUTPUT_DIR) / safe_name
    if not filepath.exists():
        return jsonify({"error": "文件不存在"}), 404
    return send_file(filepath, as_attachment=True)


@app.route("/history")
def history():
    output = Path(OUTPUT_DIR)
    files = []
    for f in sorted(output.glob("VIP客服日报_*.txt"), reverse=True):
        files.append({
            "name": f.name,
            "date": f.stem.replace("VIP客服日报_", ""),
            "size": f.stat().st_size,
        })
    return jsonify(files[:30])


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=5001)
    a = p.parse_args()
    print(f"\n{'='*45}")
    print(f"  VIP客服日报 - Web版")
    print(f"  打开浏览器访问: http://localhost:{a.port}")
    print(f"{'='*45}\n")
    app.run(host="0.0.0.0", port=a.port, debug=False)
