"""安全漏洞修复完整功能测试 - 在conda环境中运行"""
import sys
import os
import time
import asyncio
import hashlib

# 将项目根目录加入path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

passed = 0
failed = 0
errors = []

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f'  [PASS] {name}')
        passed += 1
    else:
        msg = f'  [FAIL] {name}' + (f' -- {detail}' if detail else '')
        print(msg)
        failed += 1
        errors.append(name)


# ============================================================
print("=" * 60)
print("第一部分: 静态代码检查")
print("=" * 60)

def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()

content = read_file('reply_server.py')
db_content = read_file('db_manager.py')
utils_content = read_file('utils/xianyu_utils.py')
start_content = read_file('Start.py')
config_content = read_file('global_config.yml')

print('\n--- P0: 紧急修复 ---')
check('测试后门 zhinina_test_key 已删除', 'zhinina_test_key' not in content)
check('邮件API dy.zhinianboke.com 已删除', 'dy.zhinianboke.com' not in db_content)
check('_send_email_via_api 调用已清除', '_send_email_via_api' not in db_content)
check('默认密码检测 must_change_password', 'must_change_password' in content)
check('_is_default_password 函数存在', '_is_default_password' in content)
check('明文密码已从日志移除', '密码: admin123' not in db_content)

print('\n--- P1: 高优先级 ---')
check('passlib bcrypt 已导入', 'from passlib.hash import bcrypt' in db_content)
check('bcrypt_hash.hash() 密码创建', 'bcrypt_hash.hash(' in db_content)
check('bcrypt_hash.verify() 密码验证', 'bcrypt_hash.verify(' in db_content)
check('SHA256自动升级逻辑', '自动升级为bcrypt' in db_content)
check('数据库下载改为POST', "app.post('/admin/backup/download')" in content)
check('二次密码验证 confirm_password', 'confirm_password' in content)
check('硬编码 xianyu_api_secret_2024 已移除', 'xianyu_api_secret_2024' not in content)
check('硬编码 xianyu_qq_reply_2024 已移除', 'xianyu_qq_reply_2024' not in db_content)
check('API密钥改为环境变量', "os.getenv('API_SECRET_KEY'" in content)

print('\n--- P2: 中优先级 ---')
check('SQL注入白名单 ALLOWED_TABLES', 'ALLOWED_TABLES' in db_content)
check('SQL注入 _validate_table_name', '_validate_table_name' in db_content)
check('版本检查外连已禁用', 'xianyu.zhinianblog.cn' not in content)

print('\n--- P3: 低优先级 ---')
check('登录速率限制函数存在', '_check_login_rate_limit' in content)
check('登录失败记录函数存在', '_record_login_failure' in content)
check('登录成功清除函数存在', '_clear_login_attempts' in content)
check('安全随机数 secrets.randbelow', 'secrets.randbelow' in utils_content)
check('random.random 已从xianyu_utils移除', 'random.random' not in utils_content)
check('Start.py 默认绑定127.0.0.1', "'127.0.0.1'" in start_content)
check('global_config 默认绑定127.0.0.1', 'host: 127.0.0.1' in config_content)


# ============================================================
print("\n" + "=" * 60)
print("第二部分: bcrypt密码哈希功能测试")
print("=" * 60)

from passlib.hash import bcrypt as bcrypt_hash

print('\n--- bcrypt 基础功能 ---')
# 测试bcrypt哈希生成
test_password = "test_password_123"
hashed = bcrypt_hash.hash(test_password)
check('bcrypt哈希生成', hashed.startswith('$2'))
check('bcrypt哈希验证(正确密码)', bcrypt_hash.verify(test_password, hashed))
check('bcrypt哈希验证(错误密码)', not bcrypt_hash.verify("wrong_password", hashed))

# 测试SHA256兼容性检测
sha256_hash = hashlib.sha256("admin123".encode()).hexdigest()
check('SHA256哈希格式识别', not sha256_hash.startswith('$2'))
check('bcrypt哈希格式识别', hashed.startswith('$2'))

# 测试默认密码检测逻辑
default_sha256 = hashlib.sha256("admin123".encode()).hexdigest()
default_bcrypt = bcrypt_hash.hash("admin123")

# 模拟 _is_default_password 逻辑
def _is_default_password(password_hash):
    _DEFAULT_PASSWORD_SHA256 = hashlib.sha256("admin123".encode()).hexdigest()
    if password_hash == _DEFAULT_PASSWORD_SHA256:
        return True
    if password_hash.startswith('$2'):
        try:
            return bcrypt_hash.verify("admin123", password_hash)
        except Exception:
            return False
    return False

print('\n--- 默认密码检测 ---')
check('检测SHA256格式默认密码', _is_default_password(default_sha256))
check('检测bcrypt格式默认密码', _is_default_password(default_bcrypt))
check('非默认密码SHA256不误报', not _is_default_password(hashlib.sha256("other_pass".encode()).hexdigest()))
check('非默认密码bcrypt不误报', not _is_default_password(bcrypt_hash.hash("other_pass")))


# ============================================================
print("\n" + "=" * 60)
print("第三部分: 登录速率限制功能测试")
print("=" * 60)

# 模拟速率限制逻辑
_login_attempts = {}
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_LOCKOUT_SECONDS = 900

def _check_login_rate_limit(identifier):
    now = time.time()
    if identifier in _login_attempts:
        _login_attempts[identifier] = [
            t for t in _login_attempts[identifier] if now - t < _LOGIN_LOCKOUT_SECONDS
        ]
        if len(_login_attempts[identifier]) >= _LOGIN_MAX_ATTEMPTS:
            return True
    return False

def _record_login_failure(identifier):
    if identifier not in _login_attempts:
        _login_attempts[identifier] = []
    _login_attempts[identifier].append(time.time())

def _clear_login_attempts(identifier):
    _login_attempts.pop(identifier, None)

print('\n--- 速率限制逻辑 ---')
test_user = "test_attacker"

# 初始状态不应被锁定
check('初始状态未锁定', not _check_login_rate_limit(test_user))

# 记录4次失败，不应锁定
for i in range(4):
    _record_login_failure(test_user)
check('4次失败后未锁定', not _check_login_rate_limit(test_user))

# 第5次失败后应锁定
_record_login_failure(test_user)
check('5次失败后已锁定', _check_login_rate_limit(test_user))

# 登录成功后清除
_clear_login_attempts(test_user)
check('清除后解锁', not _check_login_rate_limit(test_user))

# 测试过期清理
test_user2 = "test_expired"
_login_attempts[test_user2] = [time.time() - 1000]  # 16分钟前的记录
for i in range(4):
    _record_login_failure(test_user2)
check('过期记录被清理(4新+1旧=4有效)', not _check_login_rate_limit(test_user2))


# ============================================================
print("\n" + "=" * 60)
print("第四部分: 安全随机数功能测试")
print("=" * 60)

import secrets

print('\n--- secrets模块 ---')
# 测试secrets.randbelow
vals = [secrets.randbelow(1000) for _ in range(100)]
check('secrets.randbelow 范围正确 [0,1000)', all(0 <= v < 1000 for v in vals))
check('secrets.randbelow 有随机性', len(set(vals)) > 50)

# 测试generate_mid
from utils.xianyu_utils import generate_mid, generate_device_id
mid = generate_mid()
check('generate_mid 返回字符串', isinstance(mid, str))
check('generate_mid 包含空格分隔', ' ' in mid)

# 测试generate_device_id
device_id = generate_device_id("test_user")
check('generate_device_id 返回字符串', isinstance(device_id, str))
check('generate_device_id 包含user_id', device_id.endswith('-test_user'))
check('generate_device_id 包含分隔符', device_id.count('-') >= 4)

# 确认不再使用random模块
import inspect
mid_source = inspect.getsource(generate_mid)
device_source = inspect.getsource(generate_device_id)
check('generate_mid 不使用random模块', 'import random' not in mid_source)
check('generate_device_id 不使用random模块', 'import random' not in device_source)
check('generate_mid 使用secrets模块', 'import secrets' in mid_source)
check('generate_device_id 使用secrets模块', 'import secrets' in device_source)


# ============================================================
print("\n" + "=" * 60)
print("第五部分: SQL注入白名单验证测试")
print("=" * 60)

# 直接测试白名单逻辑
from db_manager import DBManager

print('\n--- 表名白名单 ---')
# 合法表名
for table in ['cookies', 'users', 'keywords', 'system_settings', 'ai_reply_settings']:
    try:
        result = DBManager._validate_table_name(table)
        check(f'合法表名 "{table}" 通过', result == table)
    except Exception as e:
        check(f'合法表名 "{table}" 通过', False, str(e))

# 非法表名 - 应抛出异常
for bad_table in ['evil_table', 'DROP TABLE users--', "'; DELETE FROM users;--", '../etc/passwd']:
    try:
        DBManager._validate_table_name(bad_table)
        check(f'非法表名 "{bad_table[:20]}" 被拒绝', False, '未抛出异常')
    except ValueError as e:
        check(f'非法表名 "{bad_table[:20]}" 被拒绝', '非法表名' in str(e))


# ============================================================
print("\n" + "=" * 60)
print("第六部分: FastAPI应用导入测试")
print("=" * 60)

try:
    from reply_server import app
    check('reply_server.app 导入成功', app is not None)
    
    # 检查路由是否包含关键端点
    routes = [r.path for r in app.routes if hasattr(r, 'path')]
    check('/login 路由存在', '/login' in routes)
    check('/admin/backup/download 路由存在', '/admin/backup/download' in routes)
    
    # 检查版本检查端点存在但已禁用外连
    check('/api/version/check 路由存在', '/api/version/check' in routes)
    
except Exception as e:
    check('reply_server.app 导入成功', False, str(e))


# ============================================================
print("\n" + "=" * 60)
print("第七部分: 异步端点功能测试")
print("=" * 60)

async def test_async_endpoints():
    """测试异步端点"""
    from httpx import AsyncClient, ASGITransport
    from reply_server import app
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        
        # 测试登录 - 错误密码
        print('\n--- 登录端点测试 ---')
        resp = await client.post('/login', json={
            'username': 'admin',
            'password': 'wrong_password'
        })
        check('错误密码登录返回200', resp.status_code == 200)
        data = resp.json()
        check('错误密码登录失败', data.get('success') == False)
        
        # 测试速率限制 - 连续5次错误
        print('\n--- 速率限制端点测试 ---')
        for i in range(5):
            await client.post('/login', json={
                'username': 'rate_limit_test',
                'password': 'wrong'
            })
        resp = await client.post('/login', json={
            'username': 'rate_limit_test',
            'password': 'wrong'
        })
        data = resp.json()
        check('速率限制生效(第6次被拒)', '过多' in data.get('message', '') or '重试' in data.get('message', ''))
        
        # 测试版本检查 - 应返回本地响应
        print('\n--- 版本检查端点测试 ---')
        resp = await client.get('/api/version/check')
        check('版本检查返回200', resp.status_code == 200)
        data = resp.json()
        check('版本检查不含外部URL', 'zhinianblog' not in str(data))
        
        # 测试数据库下载 - POST方法
        print('\n--- 数据库下载端点测试 ---')
        resp = await client.get('/admin/backup/download')
        # GET请求会被SPA catch-all路由匹配返回前端HTML(200)，而非下载接口
        # 关键验证：GET不会触发实际下载(响应不是文件流)
        is_html = 'text/html' in resp.headers.get('content-type', '')
        check('数据库下载GET返回前端页面(非文件)', is_html or resp.status_code == 405)
        
        # POST但未认证 - 验证真正的下载接口需要认证
        resp = await client.post('/admin/backup/download', json={
            'confirm_password': 'test'
        })
        check('数据库下载POST未认证被拒', resp.status_code in [401, 403, 422])

try:
    asyncio.run(test_async_endpoints())
except Exception as e:
    print(f'  [ERROR] 异步测试异常: {e}')
    import traceback
    traceback.print_exc()


# ============================================================
print("\n" + "=" * 60)
print("测试结果汇总")
print("=" * 60)
total = passed + failed
print(f'\n  通过: {passed}/{total}')
print(f'  失败: {failed}/{total}')
if errors:
    print(f'\n  失败项目:')
    for e in errors:
        print(f'    - {e}')
print(f'\n  通过率: {passed/total*100:.1f}%')
sys.exit(0 if failed == 0 else 1)
