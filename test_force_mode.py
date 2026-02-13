#!/usr/bin/env python3
"""
Test script to verify force private mode implementation
"""

import sys
import ast
import re


def test_no_retry_in_force_mode():
    """Verify that force mode disables retry logic"""
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Find _send_message function
    match = re.search(
        r'async def _send_message\(self, task, target, account\):.*?(?=\n    async def |\nclass |\Z)',
        content,
        re.DOTALL
    )
    
    if not match:
        print("❌ FAIL: Could not find _send_message function")
        return False
    
    function_code = match.group(0)
    
    # Check for force_private_mode check
    if 'force_private_mode' not in function_code:
        print("❌ FAIL: _send_message does not check force_private_mode")
        return False
    
    # Check that retry_count is set to 0 when force_private_mode is True
    if not re.search(r'if.*force_private_mode.*:\s*retry_count\s*=\s*0', function_code, re.DOTALL):
        print("❌ FAIL: _send_message does not set retry_count=0 for force_private_mode")
        return False
    
    print("✅ PASS: Force mode correctly disables retry logic")
    return True


def test_default_consecutive_limit():
    """Verify that DEFAULT_CONSECUTIVE_FAILURE_LIMIT is set to 10"""
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    match = re.search(r'DEFAULT_CONSECUTIVE_FAILURE_LIMIT\s*=\s*(\d+)', content)
    
    if not match:
        print("❌ FAIL: Could not find DEFAULT_CONSECUTIVE_FAILURE_LIMIT")
        return False
    
    limit = int(match.group(1))
    
    if limit != 10:
        print(f"❌ FAIL: DEFAULT_CONSECUTIVE_FAILURE_LIMIT is {limit}, expected 10")
        return False
    
    print("✅ PASS: DEFAULT_CONSECUTIVE_FAILURE_LIMIT is correctly set to 10")
    return True


def test_force_mode_implementation():
    """Verify that _execute_force_send_mode exists and uses asyncio.gather"""
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check for _execute_force_send_mode function
    if 'async def _execute_force_send_mode' not in content:
        print("❌ FAIL: _execute_force_send_mode function not found")
        return False
    
    # Find the function
    match = re.search(
        r'async def _execute_force_send_mode\(.*?\):.*?(?=\n    async def |\nclass |\Z)',
        content,
        re.DOTALL
    )
    
    if not match:
        print("❌ FAIL: Could not parse _execute_force_send_mode function")
        return False
    
    function_code = match.group(0)
    
    # Check for asyncio.gather (multi-threading)
    if 'asyncio.gather' not in function_code:
        print("❌ FAIL: _execute_force_send_mode does not use asyncio.gather for concurrency")
        return False
    
    # Check for _process_account_force_mode (the function that handles each account)
    if 'async def _process_account_force_mode' not in content:
        print("❌ FAIL: _process_account_force_mode function not found")
        return False
    
    # Find _process_account_force_mode
    match = re.search(
        r'async def _process_account_force_mode\(.*?\):.*?(?=\n    async def |\nclass |\Z)',
        content,
        re.DOTALL
    )
    
    if not match:
        print("❌ FAIL: Could not parse _process_account_force_mode function")
        return False
    
    account_function_code = match.group(0)
    
    # Check for consecutive failure logic in _process_account_force_mode
    if 'consecutive_failures' not in account_function_code.lower():
        print("❌ FAIL: _process_account_force_mode does not implement consecutive failure tracking")
        return False
    
    # Check that counter resets on success
    if 'consecutive_failures = 0' not in account_function_code:
        print("❌ FAIL: _process_account_force_mode does not reset consecutive_failures on success")
        return False
    
    # Check that counter increments on failure
    if 'consecutive_failures += 1' not in account_function_code:
        print("❌ FAIL: _process_account_force_mode does not increment consecutive_failures on failure")
        return False
    
    print("✅ PASS: Force mode implementation verified")
    return True


def test_spambot_checking():
    """Verify that @spambot checking is implemented"""
    with open('bot.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check for check_account_real_status function
    if 'async def check_account_real_status' not in content:
        print("❌ FAIL: check_account_real_status function not found")
        return False
    
    # Check for @spambot query
    if 'spambot' not in content.lower():
        print("❌ FAIL: No reference to spambot found")
        return False
    
    # Find check_account_real_status function
    match = re.search(
        r'async def check_account_real_status\(.*?\):.*?(?=\nasync def |\nclass |\Z)',
        content,
        re.DOTALL
    )
    
    if not match:
        print("❌ FAIL: Could not parse check_account_real_status function")
        return False
    
    function_code = match.group(0)
    
    # Check for /start command to spambot
    if '/start' not in function_code:
        print("❌ FAIL: check_account_real_status does not send /start to spambot")
        return False
    
    # Check for status detection (active, limited, banned)
    has_active = 'active' in function_code
    has_limited = 'limited' in function_code
    has_banned = 'banned' in function_code
    
    if not (has_active and has_limited and has_banned):
        print("❌ FAIL: check_account_real_status does not check for all statuses (active, limited, banned)")
        return False
    
    print("✅ PASS: @spambot checking correctly implemented")
    return True


def main():
    print("=" * 70)
    print("Force Private Mode Implementation Tests")
    print("=" * 70)
    print()
    
    all_passed = True
    
    # Run tests
    all_passed = all_passed and test_no_retry_in_force_mode()
    all_passed = all_passed and test_default_consecutive_limit()
    all_passed = all_passed and test_force_mode_implementation()
    all_passed = all_passed and test_spambot_checking()
    
    print()
    print("=" * 70)
    
    if all_passed:
        print("✅ All tests PASSED!")
        return 0
    else:
        print("❌ Some tests FAILED!")
        return 1


if __name__ == '__main__':
    sys.exit(main())
