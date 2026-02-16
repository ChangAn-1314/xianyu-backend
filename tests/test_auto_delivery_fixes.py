"""
单元测试：自动发货相关修复
覆盖3个修复点：
1. _is_auto_delivery_trigger - 付款触发关键词匹配
2. _extract_order_id - 订单ID提取（含卡片更新消息）
3. 非聊天消息付款检测逻辑
4. PostgreSQL boolean SQL 兼容性
"""
import sys
import os
import re
import json
import unittest
from unittest.mock import MagicMock, patch, AsyncMock

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MockXianyuLive:
    """模拟 XianyuLive 实例，仅包含测试所需的方法"""

    def __init__(self):
        self.cookie_id = "test_cookie_123"
        self.myid = "test_my_id"

    def _safe_str(self, obj):
        return str(obj)

    def _is_auto_delivery_trigger(self, message: str) -> bool:
        """从 XianyuAutoAsync.py 复制的触发检测逻辑"""
        auto_delivery_keywords = [
            '[我已付款，等待你发货]',
            '[已付款，待发货]',
            '我已付款，等待你发货',
            '[记得及时发货]',
        ]
        for keyword in auto_delivery_keywords:
            if keyword in message:
                return True
        return False

    def _extract_order_id(self, message: dict) -> str:
        """从 XianyuAutoAsync.py 复制的订单ID提取逻辑（简化版，去除logger）"""
        try:
            order_id = None
            message_1 = message.get('1', {})
            content_json_str = ''

            if isinstance(message_1, dict):
                message_1_6 = message_1.get('6', {})
                if isinstance(message_1_6, dict):
                    content_json_str = message_1_6.get('3', {}).get('5', '') if isinstance(message_1_6.get('3', {}), dict) else ''

            # 方法1: 从content_json_str中提取
            if content_json_str:
                try:
                    content_data = json.loads(content_json_str)
                    target_url = content_data.get('dxCard', {}).get('item', {}).get('main', {}).get('exContent', {}).get('button', {}).get('targetUrl', '')
                    if target_url:
                        order_match = re.search(r'orderId=(\d+)', target_url)
                        if order_match:
                            order_id = order_match.group(1)
                    if not order_id:
                        main_target_url = content_data.get('dxCard', {}).get('item', {}).get('main', {}).get('targetUrl', '')
                        if main_target_url:
                            order_match = re.search(r'order_detail\?id=(\d+)', main_target_url)
                            if order_match:
                                order_id = order_match.group(1)
                except Exception:
                    pass

            # 方法3: 在整个消息字符串中搜索
            if not order_id:
                try:
                    message_str = str(message)
                    patterns = [
                        r'orderId[=:](\d{10,})',
                        r'order_detail\?id=(\d{10,})',
                        r'"id"\s*:\s*"?(\d{10,})"?',
                        r'bizOrderId[=:](\d{10,})',
                    ]
                    for pattern in patterns:
                        matches = re.findall(pattern, message_str)
                        if matches:
                            order_id = matches[0]
                            break
                except Exception:
                    pass

            return order_id
        except Exception:
            return None


# ==================== 测试1: 自动发货触发关键词匹配 ====================

class TestIsAutoDeliveryTrigger(unittest.TestCase):
    """测试 _is_auto_delivery_trigger 方法"""

    def setUp(self):
        self.live = MockXianyuLive()

    def test_trigger_exact_match_paid(self):
        """付款消息完整匹配"""
        self.assertTrue(self.live._is_auto_delivery_trigger('[我已付款，等待你发货]'))

    def test_trigger_exact_match_paid_short(self):
        """短格式付款消息"""
        self.assertTrue(self.live._is_auto_delivery_trigger('[已付款，待发货]'))

    def test_trigger_without_brackets(self):
        """无方括号的付款消息"""
        self.assertTrue(self.live._is_auto_delivery_trigger('我已付款，等待你发货'))

    def test_trigger_reminder(self):
        """发货提醒消息"""
        self.assertTrue(self.live._is_auto_delivery_trigger('[记得及时发货]'))

    def test_trigger_contained_in_longer_text(self):
        """关键词包含在更长文本中"""
        self.assertTrue(self.live._is_auto_delivery_trigger('系统通知: [我已付款，等待你发货] 请尽快处理'))

    def test_no_trigger_normal_chat(self):
        """普通聊天消息不触发"""
        self.assertFalse(self.live._is_auto_delivery_trigger('你好，这个还在吗'))

    def test_no_trigger_unpaid(self):
        """未付款消息不触发"""
        self.assertFalse(self.live._is_auto_delivery_trigger('[我已拍下，待付款]'))

    def test_no_trigger_refund(self):
        """退款消息不触发"""
        self.assertFalse(self.live._is_auto_delivery_trigger('[我发起了退款申请]'))

    def test_no_trigger_empty(self):
        """空消息不触发"""
        self.assertFalse(self.live._is_auto_delivery_trigger(''))

    def test_no_trigger_partial_keyword(self):
        """部分关键词不触发"""
        self.assertFalse(self.live._is_auto_delivery_trigger('已付款'))

    def test_no_trigger_trade_close(self):
        """交易关闭不触发"""
        self.assertFalse(self.live._is_auto_delivery_trigger('[交易关闭]'))


# ==================== 测试2: 订单ID提取 ====================

class TestExtractOrderId(unittest.TestCase):
    """测试 _extract_order_id 方法"""

    def setUp(self):
        self.live = MockXianyuLive()

    def test_extract_from_button_target_url(self):
        """从button的targetUrl提取orderId"""
        message = {
            '1': {
                '6': {
                    '3': {
                        '5': json.dumps({
                            'dxCard': {
                                'item': {
                                    'main': {
                                        'exContent': {
                                            'button': {
                                                'targetUrl': 'https://example.com?orderId=4502061577026003543'
                                            }
                                        }
                                    }
                                }
                            }
                        })
                    }
                }
            }
        }
        self.assertEqual(self.live._extract_order_id(message), '4502061577026003543')

    def test_extract_from_order_detail_url(self):
        """从order_detail URL提取订单ID"""
        message = {
            '1': '3978931333384.PNM',
            '4': {
                '_CONTENT_MAP_UPDATE_PRE_dxCard.item.main.exContent.button':
                    '{"targetUrl":"fleamarket://order_detail?id=4502048797028021907&role=seller","text":"已付款"}'
            }
        }
        result = self.live._extract_order_id(message)
        self.assertEqual(result, '4502048797028021907')

    def test_extract_from_string_message_orderId(self):
        """从消息字符串中提取orderId=格式"""
        message = {
            '1': 'some_string',
            '4': {'reminderUrl': 'https://example.com?orderId=1234567890123'}
        }
        result = self.live._extract_order_id(message)
        self.assertEqual(result, '1234567890123')

    def test_no_order_id_in_simple_message(self):
        """简单消息无订单ID"""
        message = {
            '1': '58497186568@goofish',
            '2': 1,
            '3': {'redReminder': '等待卖家发货'},
            '4': 1771239570155
        }
        result = self.live._extract_order_id(message)
        self.assertIsNone(result)

    def test_no_order_id_in_list_message(self):
        """列表格式消息无订单ID"""
        message = {
            '1': [{'1': '58497186568@goofish', '2': 1, '3': 1, '4': '2209087798339@goofish'}]
        }
        result = self.live._extract_order_id(message)
        self.assertIsNone(result)

    def test_extract_from_card_update_message(self):
        """从卡片更新消息(_CONTENT_MAP_UPDATE_PRE_)中提取订单ID"""
        message = {
            '1': '3978931333384.PNM',
            '2': '58497186568@goofish',
            '3': 1,
            '4': {
                '_CONTENT_MAP_UPDATE_PRE_dxCard.item.main.exContent.button':
                    '{"bgColor":"#FAFAFA","targetUrl":"fleamarket://order_detail?id=4502061577026003543&role=seller","text":"已付款"}',
                'updateKey': '58497186568:4502061577026003543:63:TRADE_PAID_DONE_SELLER:26',
                'reminderContent': '[我已付款，等待你发货]',
                'reminderUrl': 'fleamarket://message_chat?itemId=1021323735276&peerUserId=2219139921839',
            }
        }
        result = self.live._extract_order_id(message)
        self.assertEqual(result, '4502061577026003543')

    def test_empty_message(self):
        """空消息"""
        self.assertIsNone(self.live._extract_order_id({}))


# ==================== 测试3: 非聊天消息付款检测 ====================

class TestNonChatMessagePaymentDetection(unittest.TestCase):
    """测试非聊天消息中的付款关键词检测逻辑"""

    def setUp(self):
        self.payment_indicators = ['我已付款', '已付款，待发货', '等待你发货', 'TRADE_PAID_DONE_SELLER']

    def _detect_payment(self, message: dict) -> bool:
        """模拟非聊天消息付款检测逻辑"""
        msg_str = str(message)
        return any(kw in msg_str for kw in self.payment_indicators)

    def test_detect_card_update_with_paid_button(self):
        """卡片更新消息包含"已付款"按钮"""
        message = {
            '1': '3978931333384.PNM',
            '4': {
                '_CONTENT_MAP_UPDATE_PRE_dxCard.item.main.exContent.button':
                    '{"targetUrl":"fleamarket://order_detail?id=123","text":"已付款"}',
                'reminderContent': '[我已付款，等待你发货]',
            }
        }
        self.assertTrue(self._detect_payment(message))

    def test_detect_paid_pending_delivery(self):
        """检测"已付款，待发货"关键词"""
        message = {
            '1': 'some_id',
            '4': {'reminderContent': '[已付款，待发货]'}
        }
        self.assertTrue(self._detect_payment(message))

    def test_detect_trade_paid_done_seller(self):
        """检测TRADE_PAID_DONE_SELLER状态"""
        message = {
            '1': 'some_id',
            '4': {'updateKey': '58497186568:4502061577026003543:63:TRADE_PAID_DONE_SELLER:26'}
        }
        self.assertTrue(self._detect_payment(message))

    def test_detect_waiting_for_delivery(self):
        """检测"等待你发货"关键词"""
        message = {
            '1': 'some_id',
            '3': {'redReminder': '等待卖家发货'},
            '4': {'reminderTitle': '等待你发货'}
        }
        self.assertTrue(self._detect_payment(message))

    def test_no_detect_unpaid_message(self):
        """未付款消息不检测为付款"""
        message = {
            '1': 'some_id',
            '4': {
                'reminderContent': '[我已拍下，待付款]',
                'reminderTitle': '买家已拍下，待付款',
                'updateKey': '58497186568:123:1_not_pay_seller'
            }
        }
        self.assertFalse(self._detect_payment(message))

    def test_no_detect_refund_message(self):
        """退款消息不检测为付款"""
        message = {
            '1': {'1': {'1': '2219139921839@goofish'}},
            '4': {'reminderContent': '[我发起了退款申请]'}
        }
        self.assertFalse(self._detect_payment(message))

    def test_no_detect_trade_close(self):
        """交易关闭消息不检测为付款"""
        message = {
            '1': 'some_id',
            '3': {'redReminder': '交易关闭'}
        }
        self.assertFalse(self._detect_payment(message))

    def test_no_detect_empty_message(self):
        """空消息不检测为付款"""
        self.assertFalse(self._detect_payment({}))


# ==================== 测试4: PostgreSQL Boolean SQL 兼容性 ====================

class TestPostgreSQLBooleanCompatibility(unittest.TestCase):
    """测试 db_manager.py 中 SQL 语句的 PostgreSQL boolean 兼容性"""

    @classmethod
    def setUpClass(cls):
        """读取 db_manager.py 文件内容"""
        db_manager_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'db_manager.py'
        )
        with open(db_manager_path, 'r', encoding='utf-8') as f:
            cls.db_content = f.read()

    def _find_raw_sql_blocks(self):
        """提取所有 text('''...''') 中的 raw SQL"""
        pattern = r"text\(\s*['\"]{{3}}(.*?)['\"]{{3}}\s*\)"
        # 使用更宽松的匹配
        blocks = re.finditer(r"text\(\s*'{3}(.*?)'{3}\s*\)", self.db_content, re.DOTALL)
        return list(blocks)

    def test_no_boolean_equals_integer_in_enabled(self):
        """enabled 列不使用 = 1 / = 0 比较"""
        # 匹配 raw SQL 中的 .enabled = 0 或 .enabled = 1
        matches = re.findall(r'\.enabled\s*=\s*[01]\b', self.db_content)
        self.assertEqual(len(matches), 0,
                         f"发现 boolean = integer 模式: {matches}")

    def test_no_boolean_equals_integer_in_is_multi_spec(self):
        """is_multi_spec 列不使用 = 1 / = 0 比较"""
        matches = re.findall(r'is_multi_spec\s*=\s*[01]\b', self.db_content)
        self.assertEqual(len(matches), 0,
                         f"发现 boolean = integer 模式: {matches}")

    def test_uses_boolean_true_false(self):
        """SQL 使用 = true / = false"""
        has_true = 'enabled = true' in self.db_content
        has_false = 'is_multi_spec = false' in self.db_content
        self.assertTrue(has_true, "未找到 'enabled = true'")
        self.assertTrue(has_false, "未找到 'is_multi_spec = false'")

    def test_is_multi_spec_true(self):
        """is_multi_spec = true 存在"""
        self.assertIn('is_multi_spec = true', self.db_content)


# ==================== 测试5: 完整流程模拟 ====================

class TestAutoDeliveryFlowIntegration(unittest.TestCase):
    """集成测试：模拟完整的付款消息处理流程"""

    def setUp(self):
        self.live = MockXianyuLive()

    def test_card_update_payment_flow(self):
        """模拟卡片更新付款消息的完整检测流程"""
        # 真实场景中的卡片更新消息
        message = {
            '1': '3978931333384.PNM',
            '2': '58497186568@goofish',
            '3': 1,
            '4': {
                '_CONTENT_MAP_UPDATE_PRE_dxCard.item.main.exContent.button':
                    '{"bgColor":"#FAFAFA","borderColor":"#FAFAFA","fontColor":"#C2C2C2",'
                    '"targetUrl":"fleamarket://order_detail?id=4502061577026003543&role=seller",'
                    '"text":"已付款"}',
                'updateKey': '58497186568:4502061577026003543:63:TRADE_PAID_DONE_SELLER:26',
                'reminderContent': '[我已付款，等待你发货]',
                'reminderUrl': 'fleamarket://message_chat?itemId=1021323735276&peerUserId=2219139921839',
            },
            '5': 1771232218322
        }

        # 步骤1: 提取订单ID
        order_id = self.live._extract_order_id(message)
        self.assertEqual(order_id, '4502061577026003543')

        # 步骤2: 非聊天消息付款检测
        msg_str = str(message)
        payment_indicators = ['我已付款', '已付款，待发货', '等待你发货', 'TRADE_PAID_DONE_SELLER']
        is_payment = any(kw in msg_str for kw in payment_indicators)
        self.assertTrue(is_payment)

        # 步骤3: 两个条件同时满足 → 应触发自动发货
        should_trigger = is_payment and order_id is not None
        self.assertTrue(should_trigger)

    def test_unpaid_card_no_trigger(self):
        """未付款卡片消息不应触发自动发货"""
        message = {
            '1': '3978931333384.PNM',
            '2': '58497186568@goofish',
            '4': {
                'updateKey': '58497186568:4502048797028021907:1_not_pay_seller',
                'reminderContent': '[我已拍下，待付款]',
                'reminderTitle': '买家已拍下，待付款',
            }
        }
        msg_str = str(message)
        payment_indicators = ['我已付款', '已付款，待发货', '等待你发货', 'TRADE_PAID_DONE_SELLER']
        is_payment = any(kw in msg_str for kw in payment_indicators)
        self.assertFalse(is_payment)

    def test_summary_message_no_order_id(self):
        """简化摘要消息无法提取订单ID"""
        message = {
            '1': '58497186568@goofish',
            '2': 1,
            '3': {'redReminder': '等待卖家发货', 'redReminderStyle': '1'},
            '4': 1771239570155
        }
        order_id = self.live._extract_order_id(message)
        self.assertIsNone(order_id)

        # 即使检测到付款关键词，没有order_id也不应触发
        msg_str = str(message)
        payment_indicators = ['我已付款', '已付款，待发货', '等待你发货', 'TRADE_PAID_DONE_SELLER']
        is_payment = any(kw in msg_str for kw in payment_indicators)
        should_trigger = is_payment and order_id is not None
        self.assertFalse(should_trigger)


if __name__ == '__main__':
    unittest.main(verbosity=2)
