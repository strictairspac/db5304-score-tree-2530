"""Unit tests for the streaming multi-branch merge engine (pure Python)."""

import copy
import json
import unittest

from src.services.score_tree import BPlusTree, Score
from src.services.touring import (AttemptLost, advance, build_report, new_state,
                                  normalize, validate)


def branch(name, scores, drop_after=None):
    rows = [{'number': number, 'title': title, 'available_copies': copies}
            for number, title, copies in scores]
    spec = {'branch': name, 'scores': rows}
    if drop_after is not None:
        spec['drop_after'] = drop_after
    return spec


PAYLOAD = {
    'page_capacity': 3,
    'range_start': 1204,
    'range_end': 1212,
    'page_size': 4,
    'branches': [
        branch('榆荫分馆', [
            (1209, '江湾晨曲', 2), (1205, '松影三重奏', 3), (1214, '旧街进行曲', 2),
            (1201, '纸鸢圆舞曲', 1), (1212, '灯塔小步舞曲', 1), (1204, '榆荫弦乐合奏', 2),
            (1210, '竹桥间奏', 0), (1207, '溪畔变奏曲', 0), (1216, '雾中五重奏', 4),
        ]),
        branch('河岸分馆', [
            (1206, '沙洲小调', 2), (1211, '稻穗随想曲', 3), (1204, '榆荫弦乐合奏', 1),
            (1215, '涟漪重奏', 2), (1208, '北窗练习曲', 1), (1219, '远山终曲', 2),
            (1202, '长廊二重奏', 2),
        ], drop_after=2),
        branch('北窗分馆', [
            (1205, '松影三重奏', 1), (1213, '晴堤短歌', 2), (1209, '江湾晨曲', 1),
            (1203, '石阶小夜曲', 1), (1212, '灯塔小步舞曲', 2), (1217, '晚汐重奏', 1),
            (1208, '北窗练习曲', 2),
        ]),
    ],
}

EXPECTED_ROWS = [
    (1204, '榆荫弦乐合奏', 3, [('榆荫分馆', 2), ('河岸分馆', 1)]),
    (1205, '松影三重奏', 4, [('榆荫分馆', 3), ('北窗分馆', 1)]),
    (1206, '沙洲小调', 2, [('河岸分馆', 2)]),
    (1207, '溪畔变奏曲', 0, [('榆荫分馆', 0)]),
    (1208, '北窗练习曲', 3, [('河岸分馆', 1), ('北窗分馆', 2)]),
    (1209, '江湾晨曲', 3, [('榆荫分馆', 2), ('北窗分馆', 1)]),
    (1210, '竹桥间奏', 0, [('榆荫分馆', 0)]),
    (1211, '稻穗随想曲', 3, [('河岸分馆', 3)]),
    (1212, '灯塔小步舞曲', 3, [('榆荫分馆', 1), ('北窗分馆', 2)]),
]


def reliable_payload():
    payload = copy.deepcopy(PAYLOAD)
    for spec in payload['branches']:
        spec.pop('drop_after', None)
    return payload


class MemoryStore:
    """Checkpoint store double: keeps the last committed state and all pages."""

    def __init__(self, vault=None):
        self.vault = vault if vault is not None else {'state': None, 'pages': {}}
        self.status = None
        self.lost = False

    @property
    def pages(self):
        return self.vault['pages']

    @property
    def state(self):
        return self.vault['state']

    def checkpoint(self, state, pages, status):
        if self.lost:
            return False
        json.dumps(state)  # the checkpointed state must stay JSON-serializable
        self.vault['state'] = copy.deepcopy(state)
        for page_no, rows in pages:
            if page_no in self.vault['pages']:
                raise AssertionError(f'第 {page_no} 页被重复出页')
            self.vault['pages'][page_no] = copy.deepcopy(rows)
        self.status = status
        return True


def run_merge(payload, store):
    state = store.state or new_state(payload)
    return advance(payload, state, store)


def page_rows(store, page_no):
    return store.pages[page_no]


class ValidationTest(unittest.TestCase):
    def test_sample_payload_is_valid(self):
        validate(PAYLOAD)

    def test_field_errors(self):
        bad = [
            ({'page_capacity': 1}, '页容量'),
            ({'range_start': 1212, 'range_end': 1204}, '起始曲谱编号'),
            ({'page_size': 0}, '出页行数'),
            ({'branches': []}, '分馆目录批'),
        ]
        for patch, label in bad:
            payload = reliable_payload()
            payload.update(patch)
            with self.assertRaises(ValueError, msg=label):
                validate(payload)

    def test_duplicate_number_and_branch(self):
        payload = reliable_payload()
        payload['branches'][0]['scores'].append({'number': 1209, 'title': '重复', 'available_copies': 1})
        with self.assertRaises(ValueError):
            validate(payload)
        payload = reliable_payload()
        payload['branches'][1]['branch'] = '榆荫分馆'
        with self.assertRaises(ValueError):
            validate(payload)

    def test_bad_drop_after(self):
        payload = reliable_payload()
        payload['branches'][0]['drop_after'] = -1
        with self.assertRaises(ValueError):
            validate(payload)


class MergeTest(unittest.TestCase):
    def test_reliable_merge_completes_in_one_attempt(self):
        store = MemoryStore()
        outcome = run_merge(reliable_payload(), store)
        self.assertEqual(outcome, 'complete')
        self.assertEqual(store.state['attempts'], 1)
        self.assertEqual(sorted(store.pages), [1, 2, 3])
        rows = [row for page in (1, 2, 3) for row in page_rows(store, page)]
        self.assertEqual([row['曲谱编号'] for row in rows],
                         [number for number, *_ in EXPECTED_ROWS])
        for row, (number, title, copies, sources) in zip(rows, EXPECTED_ROWS):
            self.assertEqual(row['谱名'], title)
            self.assertEqual(row['合计份数'], copies)
            self.assertEqual([(s['分馆'], s['份数']) for s in row['来源分馆']], sources)
        self.assertEqual([len(page_rows(store, page)) for page in (1, 2, 3)], [4, 4, 1])

    def test_pages_are_deterministic(self):
        first, second = MemoryStore(), MemoryStore()
        run_merge(reliable_payload(), first)
        run_merge(reliable_payload(), second)
        self.assertEqual(json.dumps(first.pages, sort_keys=True, ensure_ascii=False),
                         json.dumps(second.pages, sort_keys=True, ensure_ascii=False))

    def test_empty_range_completes_without_pages(self):
        payload = reliable_payload()
        payload['range_start'], payload['range_end'] = 1300, 1399
        store = MemoryStore()
        self.assertEqual(run_merge(payload, store), 'complete')
        self.assertEqual(store.pages, {})
        report = build_report(payload, store.state, [])
        self.assertEqual(report['metrics']['合单命中曲谱'], 0)

    def test_visits_follow_the_leaf_chain(self):
        payload = reliable_payload()
        store = MemoryStore()
        run_merge(payload, store)
        cfg = normalize(payload)
        for index, spec in enumerate(cfg['branches']):
            tree = BPlusTree(cfg['capacity'])
            for score in spec['scores']:
                tree.insert(score)
            expected = [visit.page_id for visit in tree.lookup(cfg['first'], cfg['last']).visits]
            actual = [visit['page_id'] for visit in store.state['branches'][index]['visits']]
            self.assertEqual(actual, expected, spec['name'])


class DisconnectionTest(unittest.TestCase):
    def test_resolved_prefix_pages_before_block(self):
        store = MemoryStore()
        outcome = run_merge(PAYLOAD, store)
        self.assertEqual(outcome, 'blocked')
        state = store.state
        self.assertEqual(state['frontier'], 1208)
        self.assertEqual(sorted(store.pages), [1])
        self.assertEqual([row['曲谱编号'] for row in page_rows(store, 1)],
                         [1204, 1205, 1206, 1207])
        waiting = [b['name'] for b in state['branches'] if not b['done']]
        self.assertEqual(waiting, ['河岸分馆'])
        down = [b['name'] for b in state['branches'] if b['down']]
        self.assertEqual(down, ['河岸分馆'])
        self.assertEqual([row['曲谱编号'] for row in state['partial']], [1208])

    def test_resume_completes_without_changing_emitted_pages(self):
        vault = {'state': None, 'pages': {}}
        first = MemoryStore(vault)
        self.assertEqual(run_merge(PAYLOAD, first), 'blocked')
        emitted_before = copy.deepcopy(first.pages)

        second = MemoryStore(vault)
        self.assertEqual(run_merge(PAYLOAD, second), 'complete')
        self.assertEqual(second.state['attempts'], 2)
        for page_no, rows in emitted_before.items():
            self.assertEqual(second.pages[page_no], rows)  # 已出的页保持原内容
        self.assertEqual(sorted(second.pages), [1, 2, 3])

        reliable = MemoryStore()
        run_merge(reliable_payload(), reliable)
        self.assertEqual(json.dumps(second.pages, sort_keys=True, ensure_ascii=False),
                         json.dumps(reliable.pages, sort_keys=True, ensure_ascii=False))

    def test_crash_mid_run_resumes_from_last_checkpoint(self):
        vault = {'state': None, 'pages': {}}
        attempt = MemoryStore(vault)
        payload = reliable_payload()
        state = new_state(payload)
        # Lose the attempt after the second committed step, as if the worker died.
        commits = 0
        original = attempt.checkpoint

        def flaky(state, pages, status):
            nonlocal commits
            commits += 1
            if commits > 2:
                return False
            return original(state, pages, status)

        attempt.checkpoint = flaky
        with self.assertRaises(AttemptLost):
            advance(payload, state, attempt)

        resumed = MemoryStore(vault)
        self.assertEqual(run_merge(payload, resumed), 'complete')
        self.assertEqual(sorted(resumed.pages), [1, 2, 3])

    def test_attempt_lost_when_checkpoint_rejected(self):
        store = MemoryStore()
        store.lost = True
        with self.assertRaises(AttemptLost):
            run_merge(reliable_payload(), store)


class ReportTest(unittest.TestCase):
    def test_report_metrics_and_sections(self):
        store = MemoryStore()
        run_merge(PAYLOAD, store)      # blocked
        run_merge(PAYLOAD, store)      # resumed to completion
        pages = [(page_no, store.pages[page_no]) for page_no in sorted(store.pages)]
        report = build_report(PAYLOAD, store.state, pages)
        metrics = report['metrics']
        self.assertEqual(metrics['合单命中曲谱'], 9)
        self.assertEqual(metrics['合计可借份数'], 21)
        self.assertEqual(metrics['候补曲谱'], 2)
        self.assertEqual(metrics['出页数'], 3)
        self.assertEqual(metrics['分馆数'], 3)
        self.assertEqual(metrics['断连分馆'], 1)
        self.assertEqual(metrics['执行轮次'], 2)
        self.assertEqual(metrics['扫描叶页'], 9)
        titles = [section['title'] for section in report['sections']]
        self.assertEqual(titles, ['巡演借阅单', '出页目录', '分馆索引巡检', '叶链读取记录', '断连与恢复'])
        lending = report['sections'][0]['rows']
        self.assertEqual(len(lending), 9)
        self.assertEqual(lending[0]['来源明细'], '榆荫分馆 2 份 · 河岸分馆 1 份')
        json.dumps(report)  # evidence must stay JSON-serializable


if __name__ == '__main__':
    unittest.main()
