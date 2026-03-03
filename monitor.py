#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
轻量级监控执行脚本
OpenClaw HEARTBEAT调用入口
"""

import sys
import json
from datetime import datetime
from data_layer import InvestmentDB, AkShareDataSource, DynamicThresholdCalculator

def run_monitoring_check(check_type="scheduled"):
    """
    执行监控检查
    
    Args:
        check_type: scheduled(定时) | manual(手动)
    """
    print(f"[监控] 启动投资监控检查 | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 类型: {check_type}")
    
    # 初始化数据库
    db = InvestmentDB()
    ak = AkShareDataSource(db)
    calculator = DynamicThresholdCalculator(db)
    
    # 更新波动率缓存（每日首次检查时）
    if check_type == "scheduled" and datetime.now().hour < 12:
        print("[数据] 更新历史波动率缓存...")
        ak.update_volatility_cache()
    
    # 获取所有持仓
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT internal_code, exchange_code, name, category, sub_category, alert_threshold FROM holdings WHERE status = '持有'")
    holdings = cursor.fetchall()
    conn.close()
    
    alerts = []
    reminders = []
    
    print(f"[信息] 监控持仓数量: {len(holdings)}")
    
    for holding in holdings:
        internal_code, exchange_code, name, category, sub_category, base_threshold = holding
        
        # 获取动态阈值
        thresholds = calculator.calculate_thresholds(internal_code)
        price_threshold = thresholds['price_change_pct']
        
        # 获取实时行情
        quote = None
        if category == '联接基金':
            # 联接基金：获取跟踪ETF行情，估算净值
            quote = ak.get_etf_realtime_quote(exchange_code)
            if quote:
                # 估算联接基金净值
                fund_code = internal_code.replace('FUND_', '')
                nav_estimate = ak.get_fund_estimate_nav(fund_code, exchange_code)
                if nav_estimate:
                    quote['estimated_nav'] = nav_estimate['estimated_nav']
        else:
            # 场内ETF或个股
            if len(exchange_code) == 6:
                quote = ak.get_stock_realtime_quote(exchange_code)
        
        if not quote:
            print(f"  [警告] {name}: 获取行情失败")
            continue
        
        change_pct = quote.get('change_pct', 0)
        
        print(f"  {name}: {change_pct:+.2f}% (阈值: ±{price_threshold}%)")
        
        # 判断告警级别
        if abs(change_pct) >= price_threshold:
            if abs(change_pct) >= price_threshold * 1.5:
                severity = "[重要]"
                alerts.append({
                    'name': name,
                    'change_pct': change_pct,
                    'price': quote.get('price'),
                    'threshold': price_threshold,
                    'reason': f"价格变动{change_pct:+.2f}%，超过阈值{price_threshold}%的1.5倍"
                })
            else:
                severity = "[提醒]"
                reminders.append({
                    'name': name,
                    'change_pct': change_pct,
                    'price': quote.get('price'),
                    'threshold': price_threshold,
                    'reason': f"价格变动{change_pct:+.2f}%，超过阈值{price_threshold}%"
                })
    
    # 生成报告
    report = generate_report(alerts, reminders, check_type)
    
    # 输出报告
    print("\n" + "="*60)
    print(report)
    print("="*60)
    
    # 保存到数据库
    save_report_to_db(db, alerts, reminders)
    
    return len(alerts) > 0 or len(reminders) > 0

def generate_report(alerts, reminders, check_type):
    """生成监控报告"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    report_lines = [f"[报告] 投资监控简报 | {now}"]
    
    if alerts:
        report_lines.append("\n【重要】")
        for alert in alerts:
            report_lines.append(f"• {alert['name']} - 今日{alert['change_pct']:+.2f}%")
            report_lines.append(f"  触发: {alert['reason']}")
            # T+1提醒
            if check_type == "scheduled" and datetime.now().hour >= 14:
                report_lines.append(f"  [T+1] 提示: 如需操作，需在今日15:00前下单")
    
    if reminders:
        report_lines.append("\n【提醒】")
        for reminder in reminders:
            report_lines.append(f"• {reminder['name']} - {reminder['change_pct']:+.2f}%")
    
    if not alerts and not reminders:
        report_lines.append("\n[正常] 今日无重要变化")
        report_lines.append("各持仓均在正常波动范围内")
    
    # T+1时间提示
    current_hour = datetime.now().hour
    if current_hour == 11:
        report_lines.append("\n[时间] 午盘提示: 14:30前为今日最后操作窗口")
    elif current_hour == 14:
        report_lines.append("\n[时间] 收盘提示: 15:00收盘后需等待T+1交割")
    
    return "\n".join(report_lines)

def save_report_to_db(db, alerts, reminders):
    """保存报告到数据库"""
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    
    # 保存事件
    for alert in alerts:
        cursor.execute('''
        INSERT INTO event_log (event_type, severity, related_codes, event_title, event_content, triggered_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            'price_alert',
            '🔴重要',
            alert['name'],
            f"{alert['name']} 价格异动",
            alert['reason'],
            datetime.now().isoformat()
        ))
    
    for reminder in reminders:
        cursor.execute('''
        INSERT INTO event_log (event_type, severity, related_codes, event_title, event_content, triggered_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            'price_alert',
            '🟡提醒',
            reminder['name'],
            f"{reminder['name']} 价格波动",
            reminder['reason'],
            datetime.now().isoformat()
        ))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # 支持命令行参数
    check_type = sys.argv[1] if len(sys.argv) > 1 else "scheduled"
    has_alert = run_monitoring_check(check_type)
    
    # 返回退出码，供OpenClaw判断
    sys.exit(1 if has_alert else 0)
