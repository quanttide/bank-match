import requests
import re
import json


def clean_and_construct_query(raw_name: str) -> str:
    """
    清洗名称并构造“转义空格”的查询字符串。
    输入: "Bank of America N.A."
    输出: NAME:*BANK\ OF\ AMERICA*
    """
    if not raw_name:
        return ""

    # 1. 转大写
    clean = raw_name.upper()

    # 2. 移除无意义的法律后缀 (注意：保留 'BANK')
    # \b 确保匹配单词边界，避免误删单词内部字符
    suffixes = [
        r'\s+N\.A\.', r'\s+NA\b',
        r'\s+INC\.', r'\s+INC\b',
        r'\s+CORP\.', r'\s+CORP\b',
        r'\s+LTD\.', r'\s+LTD\b',
        r'\s+LLC\.', r'\s+LLC\b',
        r'\s+CO\.', r'\s+CO\b',
        r'\s+GROUP\b'
    ]
    for suffix in suffixes:
        clean = re.sub(suffix, '', clean)

    # 3. 去除特殊字符，只保留字母、数字和空格
    clean = re.sub(r'[^A-Z0-9\s]', '', clean).strip()

    # 4. 合并多余空格
    clean = re.sub(r'\s+', ' ', clean)

    if not clean:
        return ""

    # 5. 关键步骤：转义空格 (Space Escaping)
    # 将 "BANK OF AMERICA" 变为 "BANK\ OF\ AMERICA"
    # 告诉 API 这是一个不可分割的短语
    escaped_name = clean.replace(' ', r'\ ')

    # 6. 构造最终 Filter：前后加通配符
    return f"NAME:*{escaped_name}*"


def query_fdic_bank(bank_name: str):
    API_KEY = "PkYpDNXoUzShXRGTdIYneA8ovevBA1B3jQJCcaDo"
    url = "https://banks.data.fdic.gov/api/institutions"

    # 1. 构造优化的查询 Filter
    filters = clean_and_construct_query(bank_name)

    if not filters:
        print(f"❌ 名称无效: {bank_name}")
        return

    print(f"🔍 输入名称: '{bank_name}'")
    print(f"🔒 锁定Query: {filters}")  # 调试用，观察转义情况

    # 2. 修正后的参数
    params = {
        "filters": filters,
        # ST 改为 STALP (FDIC 标准字段), 增加 CERT (证书号)
        "fields": "NAME,CITY,STALP,ACTIVE,FILDATE,ASSET,ZIP,CERT",
        "limit": 10,
        "offset": 0,
        "sort_by": "ASSET",  # 按资产排序，确保存活的大银行排前面
        "sort_order": "DESC",
        "format": "json"
    }

    headers = {
        "User-Agent": "Research Script/2.0",
        "Accept": "application/json",
        "Authorization": f"Bearer {API_KEY}"
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        response_data = resp.json()

        # 获取数据列表
        data_list = response_data.get("data", [])
        meta = response_data.get("meta", {})
        total_results = meta.get("total", 0)

        if not data_list:
            print(f"❌ 未找到与 '{bank_name}' 匹配的银行信息")
            print(f"   (尝试去掉 'Bank' 或使用更短的关键词重试)")
            return

        print(f"🎉 检索完成！共匹配 {total_results} 条结果（按资产降序显示前 {len(data_list)} 条）：\n")

        for idx, outer_item in enumerate(data_list, 1):
            bank_data = outer_item.get("data", {})

            # 提取字段
            name = bank_data.get("NAME", "未命名").strip()
            city = bank_data.get("CITY", "未知城市").strip()
            state = bank_data.get("STALP", "未知州").strip()  # 修正为 STALP
            zip_code = bank_data.get("ZIP", "").strip()
            cert_id = bank_data.get("CERT", "N/A")
            active = bank_data.get("ACTIVE", 0)
            fail_date = bank_data.get("FILDATE", None)
            asset_size = bank_data.get("ASSET", 0)

            # 格式化资产
            try:
                asset_val = float(asset_size) if asset_size else 0
                asset_str = f"{int(asset_val):,}"
            except:
                asset_str = "0"

            # 状态图标
            if str(active) == '1':
                status = "✅ 运营中"
            elif fail_date:
                status = f"❌ 已倒闭 ({fail_date})"
            else:
                status = "⚠️ 非活跃 (并购/更名)"

            # 打印结果
            print(f"[{idx}] {name}")
            print(f"    🆔 证书号: {cert_id}")
            print(f"    📍 位置: {city}, {state} {zip_code}")
            print(f"    💰 资产: ${asset_str} (千美元)")
            print(f"    📊 状态: {status}")
            print("-" * 60)

    except requests.exceptions.HTTPError as e:
        print(f"\n🚨 HTTP请求错误: {e}")
        # print(f"URL: {resp.url}") # 调试用
    except Exception as e:
        print(f"\n💥 发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # --- 测试区 ---

    # 测试 1: 带有空格和后缀的标准查询
    target = "Bank of America N.A."
    query_fdic_bank(target)

    print("\n" + "=" * 80 + "\n")

    # 测试 2: 容易产生歧义的查询 (测试空格转义是否生效)
    # 如果没转义，这个查询通常会把所有包含 'Alliance' 或 'Bank' 的全搜出来
    target2 = "First Republic"
    query_fdic_bank(target2)