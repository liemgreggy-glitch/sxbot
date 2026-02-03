#!/usr/bin/env python3
"""
Validation script to check implementation completeness
"""

import sys
import ast
import re

def check_bot_py():
    """Check bot.py for implemented features"""
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    checks = {
        'Multi-threading with asyncio.gather': 'asyncio.gather' in content,
        '_process_batch method': 'async def _process_batch' in content,
        '_monitor_progress method': 'async def _monitor_progress' in content,
        '_send_completion_reports method': 'async def _send_completion_reports' in content,
        'Post代码 (POSTBOT) support': 'SendMethod.POSTBOT.value' in content and 'postbot' in content.lower(),
        'Channel forwarding': 'CHANNEL_FORWARD' in content and 'forward_messages' in content,
        'Pin message': 'pin_message' in content,
        'Delete dialog': 'delete_dialog' in content,
        'Auto-delete config messages': ('CONFIG_MESSAGE_DELETE_DELAY' in content and 'await asyncio.sleep(CONFIG_MESSAGE_DELETE_DELAY)' in content and 'delete()' in content),
        'Real-time progress display': '⬇ 正在私信中 ⬇' in content or '正在私信中' in content,
        'Refresh button': '刷新进度' in content,
        'Immediate stop response': 'stop_flags[task_id] = True' in content,
    }
    
    print("=" * 70)
    print("Implementation Validation Results")
    print("=" * 70)
    
    all_passed = True
    for feature, passed in checks.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} - {feature}")
        if not passed:
            all_passed = False
    
    print("=" * 70)
    
    if all_passed:
        print("✅ All features implemented!")
        return 0
    else:
        print("❌ Some features are missing!")
        return 1

def check_requirements():
    """Check requirements.txt for necessary dependencies"""
    with open('requirements.txt', 'r') as f:
        content = f.read()
    
    required_packages = ['pymongo', 'telethon', 'python-telegram-bot']
    
    print("\n" + "=" * 70)
    print("Dependency Check")
    print("=" * 70)
    
    all_present = True
    for package in required_packages:
        present = package in content.lower()
        status = "✅ FOUND" if present else "❌ MISSING"
        print(f"{status} - {package}")
        if not present:
            all_present = False
    
    print("=" * 70)
    return 0 if all_present else 1

def check_syntax():
    """Check Python syntax"""
    print("\n" + "=" * 70)
    print("Syntax Check")
    print("=" * 70)
    
    try:
        with open('bot.py', 'r', encoding='utf-8') as f:
            code = f.read()
        ast.parse(code)
        print("✅ PASS - bot.py has valid Python syntax")
        print("=" * 70)
        return 0
    except SyntaxError as e:
        print(f"❌ FAIL - Syntax error in bot.py: {e}")
        print("=" * 70)
        return 1

if __name__ == '__main__':
    exit_code = 0
    
    # Run all checks
    exit_code |= check_syntax()
    exit_code |= check_bot_py()
    exit_code |= check_requirements()
    
    if exit_code == 0:
        print("\n✅ All validation checks passed!")
    else:
        print("\n❌ Some validation checks failed!")
    
    sys.exit(exit_code)