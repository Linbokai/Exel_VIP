"""
VIP客服日报 - Web版
==================
Flask Web 界面 + RESTful API。

启动方式：
  python app.py
  然后浏览器打开 http://localhost:5001

增强功能：
  - Basic Auth 认证（可选）
  - 结构化 JSON API（供前端渲染和下游系统消费）
  - 历史日报筛选/分页
  - PDF 下载
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import json
import logging
import traceback
import threading
import queue
from datetime import datetime
from pathlib import Path
from functools import wraps

from flask import Flask, render_template, request, jsonify, send_file, Response

from config import (
    OUTPUT_DIR, WEB_AUTH_ENABLED, WEB_AUTH_USERNAME, WEB_AUTH_PASSWORD,
)
from service import generate_report

app = Flask(__name__)
logger = logging.getLogger("web_app")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


# ==================== Basic Auth ====================

def check_auth(username, password):
    return username == WEB_AUTH_USERNAME and password == WEB_AUTH_PASSWORD


def auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not WEB_AUTH_ENABLED:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return Response(
                "需要登录认证", 401,
                {"WWW-Authenticate": 'Basic realm="VIP客服日报系统"'},
            )
        return f(*args, **kwargs)
    return decorated


# ==================== 页面路由 ====================

@app.route("/")
@auth_required
def index():
    today = datetime.now().strftime("%Y-%m-%d")
    return render_template("index.html", today=today)


# ==================== API 路由 ====================

@app.route("/generate", methods=["POST"])
@auth_required
def generate():
    """生成日报（返回结构化数据）"""
    date_str = request.json.get("date")
    if not date_str:
        return jsonify({"success": False, "error": "请选择日期"}), 400

    try:
        report_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"success": False, "error": "日期格式不正确"}), 400

    try:
        result = generate_report(report_date)

        txt_filename = None
        xlsx_filename = None
        pdf_filename = None

        if result.builder is not None:
            try:
                txt_path = result.builder.save_text()
                txt_filename = Path(txt_path).name
            except Exception:
                logger.error(f"文本保存失败:\n{traceback.format_exc()}")

            try:
                xlsx_path = result.builder.save_excel()
                xlsx_filename = Path(xlsx_path).name
            except Exception:
                logger.error(f"Excel生成失败:\n{traceback.format_exc()}")

            try:
                pdf_path = result.builder.save_pdf()
                pdf_filename = Path(pdf_path).name
            except Exception:
                logger.error(f"PDF生成失败:\n{traceback.format_exc()}")
        else:
            logger.error("日报构建器为空，跳过文件保存")

        return jsonify({
            "success": True,
            "report": result.report_text,
            "structured": result.structured,
            "txt_filename": txt_filename,
            "xlsx_filename": xlsx_filename,
            "pdf_filename": pdf_filename,
            "stats": result.structured.get("stats", {}),
            "trend": result.trend_data,
            "errors": result.errors,
        })

    except Exception as e:
        logger.error(f"生成日报失败: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/generate-stream", methods=["POST"])
@auth_required
def generate_stream():
    """SSE 流式生成日报（实时推送进度）"""
    date_str = request.json.get("date")
    if not date_str:
        return jsonify({"success": False, "error": "请选择日期"}), 400

    try:
        report_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"success": False, "error": "日期格式不正确"}), 400

    progress_queue = queue.Queue()

    def on_progress(step, total, desc):
        progress_queue.put({"type": "progress", "step": step, "total": total, "desc": desc})

    def run_generation():
        try:
            result = generate_report(report_date, on_progress=on_progress)

            txt_filename = None
            xlsx_filename = None
            pdf_filename = None

            if result.builder is not None:
                try:
                    txt_path = result.builder.save_text()
                    txt_filename = Path(txt_path).name
                except Exception:
                    logger.error(f"文本保存失败:\n{traceback.format_exc()}")

                try:
                    xlsx_path = result.builder.save_excel()
                    xlsx_filename = Path(xlsx_path).name
                except Exception:
                    logger.error(f"Excel生成失败:\n{traceback.format_exc()}")

                try:
                    pdf_path = result.builder.save_pdf()
                    pdf_filename = Path(pdf_path).name
                except Exception:
                    logger.error(f"PDF生成失败:\n{traceback.format_exc()}")
            else:
                logger.error("日报构建器为空，跳过文件保存")

            progress_queue.put({
                "type": "done",
                "data": {
                    "success": True,
                    "report": result.report_text,
                    "structured": result.structured,
                    "txt_filename": txt_filename,
                    "xlsx_filename": xlsx_filename,
                    "pdf_filename": pdf_filename,
                    "stats": result.structured.get("stats", {}),
                    "trend": result.trend_data,
                    "errors": result.errors,
                },
            })
        except Exception as e:
            logger.error(f"生成日报失败: {e}", exc_info=True)
            progress_queue.put({
                "type": "done",
                "data": {"success": False, "error": str(e)},
            })

    thread = threading.Thread(target=run_generation, daemon=True)
    thread.start()

    def event_stream():
        while True:
            try:
                msg = progress_queue.get(timeout=600)
                yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"
                if msg["type"] == "done":
                    break
            except queue.Empty:
                break

    return Response(event_stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/report", methods=["GET"])
@auth_required
def api_report():
    """RESTful API：获取指定日期的结构化日报数据"""
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "缺少 date 参数（格式 YYYY-MM-DD）"}), 400

    try:
        report_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"error": "日期格式不正确"}), 400

    try:
        result = generate_report(report_date)
        return jsonify({
            "success": True,
            "data": result.structured,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/download/<filename>")
@auth_required
def download(filename):
    safe_name = Path(filename).name
    filepath = Path(OUTPUT_DIR) / safe_name
    if not filepath.exists():
        return jsonify({"error": "文件不存在"}), 404
    return send_file(filepath, as_attachment=True)


@app.route("/history")
@auth_required
def history():
    """历史日报列表（支持日期范围筛选和分页）"""
    output = Path(OUTPUT_DIR)

    # 日期范围筛选
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 30))

    files = []
    for f in sorted(output.glob("VIP客服日报_*.txt"), reverse=True):
        date_str = f.stem.replace("VIP客服日报_", "")
        # 日期筛选
        if start_date and date_str < start_date.replace("-", ""):
            continue
        if end_date and date_str > end_date.replace("-", ""):
            continue

        # 检查对应的 xlsx 和 pdf 文件
        xlsx_file = f.with_suffix(".xlsx")
        pdf_file = f.with_suffix(".pdf")
        files.append({
            "name": f.name,
            "date": date_str,
            "size": f.stat().st_size,
            "has_xlsx": xlsx_file.exists(),
            "has_pdf": pdf_file.exists(),
            "xlsx_name": xlsx_file.name if xlsx_file.exists() else None,
            "pdf_name": pdf_file.name if pdf_file.exists() else None,
        })

    # 分页
    total = len(files)
    start = (page - 1) * per_page
    end = start + per_page
    paginated = files[start:end]

    return jsonify({
        "files": paginated,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if total > 0 else 0,
    })


@app.route("/api/cache/clear", methods=["POST"])
@auth_required
def clear_cache():
    """清除缓存"""
    try:
        from cache import TicketCache
        cache = TicketCache()
        cache.clear_all()
        return jsonify({"success": True, "message": "缓存已清除"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=5001)
    a = p.parse_args()
    print(f"\n{'='*45}")
    print(f"  VIP客服日报 - Web版")
    print(f"  打开浏览器访问: http://localhost:{a.port}")
    if WEB_AUTH_ENABLED:
        print(f"  认证: {WEB_AUTH_USERNAME} / ***")
    print(f"{'='*45}\n")
    app.run(host="0.0.0.0", port=a.port, debug=False)
