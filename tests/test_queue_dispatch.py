"""取任务不能丢任务。

`pop_next_queue_item` 埋在 `start()` 的多层闭包里，没法 import，所以这里用
`tests/test_link_handler.py` 那套 AST 摘函数的办法：把源码里真实的函数体抠出来，
在一个自建的命名空间里编译执行，闭包变量用替身填进去。

守的性质：**入队多少个，就必须能取出多少个**。
老实现给三条队列各起一个 `queue.get()` 再 `asyncio.wait(FIRST_COMPLETED)`，
只要同时有两条以上队列非空，`done` 里就会有多个已完成的 getter；它只取一个、
把其余的结果丢掉，而那些元素已经从 asyncio.Queue 里移走了——于是任务凭空消失，
数据库里永远停在 queued。
"""
import ast
import asyncio
import contextlib
import logging
import os
import unittest

SOURCE = os.path.join(os.path.dirname(__file__), '..', 'telegram-download-daemon.py')


def _extract_functions(names):
    """从主程序里按名字摘出函数源码（含嵌套定义的）。"""
    with open(SOURCE, encoding='utf-8') as handle:
        tree = ast.parse(handle.read())

    found = {}

    class Visitor(ast.NodeVisitor):
        def visit_AsyncFunctionDef(self, node):
            if node.name in names and node.name not in found:
                found[node.name] = node
            self.generic_visit(node)

        def visit_FunctionDef(self, node):
            if node.name in names and node.name not in found:
                found[node.name] = node
            self.generic_visit(node)

    Visitor().visit(tree)
    missing = set(names) - set(found)
    if missing:
        raise AssertionError(f'未能在主程序里找到这些函数: {sorted(missing)}')
    return found


def build_namespace():
    """把 push_queue_item / pop_next_queue_item 装配到一个可运行的命名空间里。"""
    nodes = _extract_functions({'push_queue_item', 'pop_next_queue_item', 'get_queue_target'})

    ns = {
        'asyncio': asyncio,
        'contextlib': contextlib,
        'logger': logging.getLogger('test'),
        'getFilename': lambda message_obj: str(message_obj),
        'active_queue_items_by_id': {},
        'photo_queue': asyncio.Queue(),
        'video_queue': asyncio.Queue(),
        'other_queue': asyncio.Queue(),
        'photo_queue_items': [],
        'video_queue_items': [],
        'other_queue_items': [],
        'queue_items': [],
        'queue_lock': asyncio.Lock(),
        'queue_slots': asyncio.Semaphore(0),
    }

    def rebuild_web_queue_items():
        ns['queue_items'][:] = (ns['photo_queue_items'] + ns['video_queue_items']
                                + ns['other_queue_items'])

    ns['rebuild_web_queue_items'] = rebuild_web_queue_items

    # get_queue_target 依赖 message 对象的属性，这里用消息里带的 kind 字段直接分流
    def get_queue_target(message_obj):
        kind = message_obj['kind']
        if kind == 'photo':
            return ns['photo_queue'], ns['photo_queue_items'], 'photo'
        if kind == 'video':
            return ns['video_queue'], ns['video_queue_items'], 'video'
        return ns['other_queue'], ns['other_queue_items'], 'other'

    ns['get_queue_target'] = get_queue_target

    for name in ('push_queue_item', 'pop_next_queue_item'):
        module = ast.Module(body=[nodes[name]], type_ignores=[])
        ast.fix_missing_locations(module)
        exec(compile(module, SOURCE, 'exec'), ns)  # noqa: S102

    return ns


def make_item(kind, index):
    return [{'kind': kind, 'name': f'{kind}-{index}'}, None, None, f'{kind}-{index}', 0.0]


class QueueDispatchTest(unittest.TestCase):

    def test_mixed_queues_do_not_lose_tasks(self):
        """三条队列同时有货时，入队几个就得能取出几个。

        这是回归测试的核心：老实现在这个场景下 6 进 2 出，另外 4 个凭空消失。
        """
        async def scenario():
            ns = build_namespace()
            push = ns['push_queue_item']
            pop = ns['pop_next_queue_item']

            expected = []
            for i in range(2):
                for kind in ('photo', 'video', 'other'):
                    item = make_item(kind, i)
                    expected.append(item[3])
                    await push(item)

            popped = []
            for _ in range(len(expected)):
                element, _queue, _items, _kind = await asyncio.wait_for(pop(), timeout=1)
                popped.append(element[3])
            return expected, popped, ns

        expected, popped, ns = asyncio.run(scenario())

        self.assertCountEqual(
            expected, popped,
            f'入队 {len(expected)} 个，只取回 {len(popped)} 个：有任务在出队时被丢掉了')
        self.assertEqual(ns['queue_items'], [], '跟踪列表应该被清空')
        self.assertEqual(ns['active_queue_items_by_id'], {}, '待取出索引应该被清空')

    def test_many_workers_never_lose_or_duplicate(self):
        """多个 worker 并发取任务：既不能丢，也不能有两个 worker 拿到同一个。"""
        async def scenario():
            ns = build_namespace()
            push = ns['push_queue_item']
            pop = ns['pop_next_queue_item']

            total = 30
            for i in range(total):
                await push(make_item(('photo', 'video', 'other')[i % 3], i))

            taken = []

            async def worker():
                while True:
                    try:
                        element, _q, _items, _kind = await asyncio.wait_for(pop(), timeout=0.5)
                    except asyncio.TimeoutError:
                        return
                    taken.append(element[3])

            await asyncio.gather(*[worker() for _ in range(4)])
            return total, taken

        total, taken = asyncio.run(scenario())

        self.assertEqual(len(taken), total, f'入队 {total} 个，实际取回 {len(taken)} 个')
        self.assertEqual(len(set(taken)), total, '同一个任务被取走了多次')

    def test_pop_blocks_until_something_is_pushed(self):
        """队列空的时候要阻塞等待，而不是空转或抛错。"""
        async def scenario():
            ns = build_namespace()
            push = ns['push_queue_item']
            pop = ns['pop_next_queue_item']

            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(pop(), timeout=0.15)

            await push(make_item('video', 1))
            element, _q, _items, kind = await asyncio.wait_for(pop(), timeout=1)
            return element, kind

        element, kind = asyncio.run(scenario())
        self.assertEqual(element[3], 'video-1')
        self.assertEqual(kind, 'video')

    def test_cancelled_pop_returns_its_slot(self):
        """等待中的取任务被取消后，名额要还回去，不能永久漏掉一个任务。"""
        async def scenario():
            ns = build_namespace()
            push = ns['push_queue_item']
            pop = ns['pop_next_queue_item']

            waiter = asyncio.ensure_future(pop())
            await asyncio.sleep(0.05)          # 让它停在 acquire 上
            waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await waiter

            await push(make_item('other', 7))
            element, _q, _items, _kind = await asyncio.wait_for(pop(), timeout=1)
            return element

        element = asyncio.run(scenario())
        self.assertEqual(element[3], 'other-7')


if __name__ == '__main__':
    unittest.main()
