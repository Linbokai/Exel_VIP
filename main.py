"""
VIP客服日报自动生成工具 - CLI版
===============================
从网易七鱼 OpenAPI 获取VIP用户工单和统计数据，生成标准化客诉日报。

使用方式：
  python main.py
  python main.py --date 2026-03-11
  python main.py --text-only
  python main.py --no-trends      # 跳过趋势对比（更快）
  python main.py --debug
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

import logging
import argparse
from datetime import datetime

from tqdm import tqdm

from config import OUTPUT_DIR
from service import generate_report


def setup_logging(debug=False):
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="VIP客服日报自动生成工具")
    parser.add_argument("--date", type=str, default=None,
                        help="日报日期，格式 YYYY-MM-DD（默认今天）")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="输出目录")
    parser.add_argument("--text-only", action="store_true",
                        help="仅输出文本格式，不生成Excel/PDF")
    parser.add_argument("--no-trends", action="store_true",
                        help="跳过趋势对比数据获取（更快）")
    parser.add_argument("--no-sessions", action="store_true",
                        help="跳过会话数据导出")
    parser.add_argument("--no-cache", action="store_true",
                        help="不使用缓存")
    parser.add_argument("--debug", action="store_true",
                        help="显示调试日志")
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.debug)

    if args.date:
        try:
            report_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"错误：日期格式不正确 '{args.date}'，请使用 YYYY-MM-DD 格式")
            sys.exit(1)
    else:
        report_date = datetime.now()

    date_str = report_date.strftime("%Y-%m-%d")
    print(f"\n{'='*45}")
    print(f"  VIP客服日报自动生成工具")
    print(f"  日报日期：{date_str}")
    print(f"{'='*45}\n")

    # 带进度条生成日报
    progress_bar = tqdm(total=5, desc="生成日报", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} {postfix}", ncols=60)

    def on_progress(step, total, desc):
        progress_bar.set_postfix_str(desc)
        progress_bar.update(1)

    result = generate_report(
        report_date,
        on_progress=on_progress,
        use_cache=not args.no_cache,
        fetch_trends=not args.no_trends,
        fetch_sessions=not args.no_sessions,
    )
    progress_bar.close()

    # 错误提示
    if result.errors:
        print(f"\n[!] 以下数据获取不完整：{'、'.join(result.errors)}")

    # 输出到控制台
    print("\n" + result.report_text)

    # 保存文件
    txt_path = result.builder.save_text()
    print(f"\n>>> 文本日报已保存: {txt_path}")

    if not args.text_only:
        try:
            xlsx_path = result.builder.save_excel()
            print(f">>> Excel日报已保存: {xlsx_path}")
        except ImportError:
            print(">>> 提示：安装 openpyxl 后可生成Excel (pip install openpyxl)")
        except Exception as e:
            logging.getLogger(__name__).warning(f"Excel生成失败: {e}", exc_info=True)

        try:
            pdf_path = result.builder.save_pdf()
            print(f">>> PDF日报已保存: {pdf_path}")
        except ImportError:
            print(">>> 提示：安装 reportlab 后可生成PDF (pip install reportlab)")
        except Exception as e:
            logging.getLogger(__name__).warning(f"PDF生成失败: {e}", exc_info=True)

    print("\n>>> 日报生成完成！")


if __name__ == "__main__":
    main()
