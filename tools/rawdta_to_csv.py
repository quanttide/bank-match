import pandas as pd
import os
import csv
import re
import platform
import traceback

# ======================== 配置区（直接修改这里的参数即可）========================
# 输入文件夹列表：支持多个文件夹（新增/删除只需改这个列表）
INPUT_DIRS = [
    r"../data/raw/call",  # 第一个dta文件夹
    r"../data/raw/dealscan"  # 第二个dta文件夹
]
# 对应的输出目录列表
OUTPUT_DIRS = [
    r"../data/raw/call_csv",  # 第一个输出目录
    r"../data/raw/dealscan_csv"  # 第二个输出目录
]
ENCODING = "gbk"  # CSV编码：Excel兼容用gbk，通用用utf-8
SKIP_EXISTING_CSV = True  # 是否跳过已存在的CSV文件（避免重复转换）
CHUNKSIZE = 10000  # 大文件分块大小（默认1万行/块，0表示不分块）
RECURSIVE = True  # 是否递归遍历子文件夹中的dta文件
# ================================================================================

# 适配系统路径分隔符
SEP = '\\' if platform.system() == 'Windows' else '/'


# 强制检查并安装pyreadstat（核心修复：解决老旧DTA解析问题）
def install_pyreadstat():
    """自动安装pyreadstat（如果未安装）"""
    try:
        import pyreadstat
        print("✅ pyreadstat已安装，版本：", pyreadstat.__version__)
        return True
    except ImportError:
        print("📌 正在安装pyreadstat（解决DTA解析问题）...")
        try:
            import subprocess
            import sys
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pyreadstat"])
            import pyreadstat
            print("✅ pyreadstat安装成功！")
            return True
        except Exception as e:
            print(f"❌ pyreadstat安装失败：{e}")
            print("请手动执行：pip install pyreadstat")
            return False


# 初始化pyreadstat
PYREADSTAT_AVAILABLE = install_pyreadstat()


def clean_special_chars(text):
    """清理无法编码的特殊字符（如\xa0、\u200b等）"""
    if pd.isna(text) or text == 'nan' or text is None:
        return ""
    if not isinstance(text, str):
        try:
            return str(text)
        except:
            return ""
    # 替换非断行空格、零宽空格等特殊字符为普通空格
    text = re.sub(r'[\xa0\u200b\u200c\u200d\u2060\u3000]', ' ', text)
    # 移除无法编码的字符（根据指定编码过滤）
    try:
        text.encode(ENCODING)
    except UnicodeEncodeError:
        text = ''.join([c for c in text if c.encode(ENCODING, errors='ignore')])
    return text.strip()


def validate_file_size(file_path, min_size=10):
    """校验文件大小（至少10字节，避免空文件）"""
    if not os.path.exists(file_path):
        return False
    return os.path.getsize(file_path) >= min_size


def dta_to_csv(dta_file_path, csv_file_path, encoding='utf-8-sig'):
    """
    修复版转换函数（针对中文路径优化）：
    通过临时切换工作目录，解决 pyreadstat 无法读取中文路径的问题。
    """
    import os
    import pandas as pd

    # 记录当前工作目录，以便稍后切回来
    original_cwd = os.getcwd()

    # 分离出 文件夹路径 和 文件名
    file_dir = os.path.dirname(dta_file_path)
    file_name = os.path.basename(dta_file_path)

    try:
        # 1. 尝试使用 Pandas 原生读取 (Pandas 对中文路径支持较好)
        try:
            df = pd.read_stata(dta_file_path)
        except Exception:
            # 2. 如果 Pandas 失败，使用 pyreadstat (配合“切换目录大法”解决中文路径问题)
            if not PYREADSTAT_AVAILABLE:
                return False, "Pandas读取失败且未安装pyreadstat"

            import pyreadstat

            # 【关键步骤】切换到数据文件所在的目录
            os.chdir(file_dir)

            # 只读取文件名 (不带中文路径)
            df, meta = pyreadstat.read_dta(file_name)

        # --- 以下是通用的清洗和保存逻辑 ---
        if df.empty:
            return False, "DTA文件为空"

        # 清洗特殊字符
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(clean_special_chars)

        # 写入 CSV
        df.to_csv(
            csv_file_path,
            index=False,
            encoding=encoding,
            sep=',',
            quoting=csv.QUOTE_MINIMAL,
            errors='replace'
        )

        return True, f"成功（行数：{len(df)}）"

    except Exception as e:
        return False, f"转换异常：{str(e)}"

    finally:
        # 【重要】无论成功失败，必须切回原来的工作目录，否则后续路径全乱
        os.chdir(original_cwd)

def batch_convert_multi_dirs():
    """批量转换多个输入文件夹的dta文件"""
    # 全局统计
    total_success = 0
    total_fail = 0
    total_skip = 0
    total_files = 0

    print(f"📁 系统路径分隔符：{SEP}")
    print(f"📁 待处理文件夹列表：{INPUT_DIRS}")
    print(f"📁 对应输出目录列表：{OUTPUT_DIRS}\n")

    # 校验输入输出目录数量匹配
    if len(INPUT_DIRS) != len(OUTPUT_DIRS):
        print(f"❌ 错误：输入文件夹数量({len(INPUT_DIRS)})与输出文件夹数量({len(OUTPUT_DIRS)})不匹配")
        return

    # 遍历每个输入文件夹和对应的输出目录
    for idx, (input_dir, output_dir) in enumerate(zip(INPUT_DIRS, OUTPUT_DIRS)):
        # 标准化路径
        input_dir = os.path.abspath(input_dir)
        output_dir = os.path.abspath(output_dir)

        print(f"\n=== 处理第{idx + 1}组文件夹 ===")
        print(f"🔍 输入目录(绝对路径): {input_dir}")
        print(f"🔍 输出目录(绝对路径): {output_dir}")
        print(f"   输入目录是否存在: {os.path.isdir(input_dir)}")

        # 检查当前输入文件夹是否存在
        if not os.path.isdir(input_dir):
            print(f"⚠️ 输入文件夹不存在，跳过：{input_dir}")
            continue

        # 显示目录基本信息
        try:
            all_files = os.listdir(input_dir)
            dta_files_in_dir = [f for f in all_files if f.lower().endswith('.dta')]
            print(f"   输入目录文件总数: {len(all_files)}")
            print(f"   输入目录.dta文件数: {len(dta_files_in_dir)}")
            if dta_files_in_dir:
                print(f"   前5个.dta文件: {dta_files_in_dir[:5]}")
        except Exception as e:
            print(f"   ❌ 无法读取输入目录内容: {e}")
            continue

        # 创建输出目录
        os.makedirs(output_dir, exist_ok=True)
        print(f"   输出目录已创建/存在: {output_dir}")

        # 收集当前文件夹下的所有dta文件
        dta_files = []
        walk_func = os.walk(input_dir) if RECURSIVE else [(input_dir, [], os.listdir(input_dir))]

        for root, _, files in walk_func:
            for file in files:
                if file.lower().endswith('.dta'):
                    dta_file_path = os.path.abspath(os.path.join(root, file))
                    # 保持子文件夹结构
                    relative_path = os.path.relpath(root, input_dir)
                    output_subdir = os.path.join(output_dir, relative_path)
                    os.makedirs(output_subdir, exist_ok=True)
                    # 构建csv路径
                    csv_file_name = os.path.splitext(file)[0] + '.csv'
                    csv_file_path = os.path.abspath(os.path.join(output_subdir, csv_file_name))
                    dta_files.append((dta_file_path, csv_file_path))

        # 统计当前文件夹文件数
        total_files += len(dta_files)
        print(f"   最终待转换.dta文件数: {len(dta_files)}")

        if not dta_files:
            print(f"⚠️ 在文件夹 {input_dir} 中未找到任何.dta文件")
            continue

        # 转换当前文件夹的dta文件
        success_count = 0
        fail_count = 0
        skip_count = 0
        print(f"🔄 开始处理文件夹 {input_dir}（共{len(dta_files)}个dta文件）...")

        for dta_path, csv_path in dta_files:
            dta_filename = os.path.basename(dta_path)
            # 跳过已存在的CSV
            if SKIP_EXISTING_CSV and os.path.exists(csv_path) and validate_file_size(csv_path):
                skip_count += 1
                print(f"⏭️ {dta_filename} - CSV已存在且非空，跳过")
                continue

            # 执行转换
            success, msg = dta_to_csv(dta_path, csv_path, ENCODING)
            if success:
                success_count += 1
                print(f"✅ {dta_filename} → {os.path.basename(csv_path)} {msg}")
            else:
                fail_count += 1
                print(f"❌ {dta_filename} - {msg}")

        # 累加全局统计
        total_success += success_count
        total_fail += fail_count
        total_skip += skip_count

        print(f"✅ 文件夹 {input_dir} 处理完成：成功{success_count} | 失败{fail_count} | 跳过{skip_count}")

    # 最终汇总
    print("\n" + "=" * 60)
    print(f"📊 全部文件夹处理完成！")
    print(f"📈 总计扫描：{total_files} 个dta文件")
    print(f"✅ 总计成功：{total_success} 个")
    print(f"❌ 总计失败：{total_fail} 个")
    print(f"⏭️ 总计跳过：{total_skip} 个")
    print("=" * 60)


if __name__ == '__main__':
    # 主程序入口
    print("📌 老旧DTA文件转换工具（强制使用pyreadstat）")
    try:
        batch_convert_multi_dirs()
    except Exception as e:
        print(f"\n❌ 程序异常：{str(e)}")
        traceback.print_exc()
    input("\n按回车键退出...")  # 防止Windows运行后直接关闭窗口