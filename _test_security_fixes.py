"""安全漏洞修复验证脚本"""
import os

def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

content = read_file('reply_server.py')
db_content = read_file('db_manager.py')
utils_content = read_file('utils/xianyu_utils.py')
start_content = read_file('Start.py')
config_content = read_file('global_config.yml')

passed = 0
failed = 0

def check(name, condition):
    global passed, failed
    if condition:
        print(f'  [PASS] {name}')
        passed += 1
    else:
        print(f'  [FAIL] {name}')
        failed += 1

print('=== P0: 紧急修复 ===')
check('测试后门 zhinina_test_key 已删除', 'zhinina_test_key' not in content)
check('邮件API dy.zhinianboke.com 已删除', 'dy.zhinianboke.com' not in db_content)
check('_send_email_via_api 方法已删除', '_send_email_via_api' not in db_content)
check('默认密码检测 must_change_password', 'must_change_password' in content)
check('_is_default_password 函数', '_is_default_password' in content)
check('明文密码已从日志移除', '密码: admin123' not in db_content)

print('\n=== P1: 高优先级 ===')
check('passlib bcrypt 已导入', 'from passlib.hash import bcrypt' in db_content)
check('bcrypt_hash.hash() 密码创建', 'bcrypt_hash.hash(' in db_content)
check('bcrypt_hash.verify() 密码验证', 'bcrypt_hash.verify(' in db_content)
check('SHA256自动升级逻辑', '自动升级为bcrypt' in db_content)
check('数据库下载改为POST', "app.post('/admin/backup/download')" in content)
check('二次密码验证 confirm_password', 'confirm_password' in content)
check('默认密码禁止下载', '使用默认密码时禁止下载' in content)
check('硬编码 xianyu_api_secret_2024 已移除', 'xianyu_api_secret_2024' not in content)
check('硬编码 xianyu_qq_reply_2024 已移除', 'xianyu_qq_reply_2024' not in db_content)
check('API密钥改为环境变量', "os.getenv('API_SECRET_KEY'" in content)

print('\n=== P2: 中优先级 ===')
check('SQL注入白名单 ALLOWED_TABLES', 'ALLOWED_TABLES' in db_content)
check('SQL注入 _validate_table_name', '_validate_table_name' in db_content)
check('版本检查外连已禁用', 'xianyu.zhinianblog.cn' not in content)
check('版本检查返回本地信息', '版本检查已禁用外部连接' in content)

print('\n=== P3: 低优先级 ===')
check('登录速率限制 _check_login_rate_limit', '_check_login_rate_limit' in content)
check('登录失败记录 _record_login_failure', '_record_login_failure' in content)
check('登录成功清除 _clear_login_attempts', '_clear_login_attempts' in content)
check('安全随机数 secrets.randbelow', 'secrets.randbelow' in utils_content)
check('random.random 已替换', 'random.random' not in utils_content)
check('Start.py 默认绑定127.0.0.1', "'127.0.0.1'" in start_content)
check('global_config 默认绑定127.0.0.1', 'host: 127.0.0.1' in config_content)

print(f'\n=== 结果: {passed} 通过, {failed} 失败, 共 {passed+failed} 项 ===')
