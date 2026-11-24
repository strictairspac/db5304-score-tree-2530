"""Streaming merge of inspected branch catalogs into one paged touring order.

Each branch library's catalog batch is rebuilt as a B+ tree (the batch was
already inspected when registered).  The merge walks every branch's leaf chain
on demand — one leaf page per step — and emits fixed-size borrow pages in
score-number order, summing copies whenever several branches hold the same
number.

A number is resolved once every branch has either yielded it or scanned past
it, so the safe frontier is the minimum of the per-branch frontiers.  Pages
are emitted as soon as they fill up with resolved rows: emission never waits
for the whole range scan, and a disconnected branch only stalls the tail that
is still beyond its frozen frontier.  All progress lives in a JSON state
committed through the store after every step, so an interrupted run resumes
from the last checkpoint and pages already emitted stay immutable — retrying
a page fetch returns the stored content without creating copies.
"""

from bisect import bisect_left, bisect_right

from .score_tree import BPlusTree, Score, score_number

MAX_BRANCHES = 8
MAX_PAGE_SIZE = 50
STEP_BUDGET = 10_000


class AttemptLost(Exception):
    """Another attempt took over the run; stop touching the store."""


def _integer(value, minimum: int, maximum: int, label: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f'{label}必须为 {minimum} 至 {maximum} 之间的整数')
    return value


def _read_scores(rows, label: str) -> list[Score]:
    if not isinstance(rows, list) or len(rows) > 5000:
        raise ValueError(f'{label}曲谱目录须为数组，每馆最多 5000 条')
    scores, numbers = [], set()
    for index, row in enumerate(rows, 1):
        prefix = f'{label}第 {index} 条曲谱的'
        if not isinstance(row, dict):
            raise ValueError(f'{label}第 {index} 条曲谱须为对象')
        number = _integer(row.get('number'), 1, 999_999_999, f'{prefix}编号')
        if number in numbers:
            raise ValueError(f'{prefix}编号 {number} 重复')
        title = row.get('title')
        if not isinstance(title, str) or not 1 <= len(title.strip()) <= 120:
            raise ValueError(f'{prefix}谱名去除首尾空白后须为 1 至 120 字符')
        copies = _integer(row.get('available_copies'), 0, 9999, f'{prefix}可借份数')
        scores.append(Score(number, title.strip(), copies))
        numbers.add(number)
    return scores


def normalize(payload) -> dict:
    """Validate the merge request and return a plain config dict."""
    if not isinstance(payload, dict):
        raise ValueError('请提交巡演借谱合单对象')
    capacity = _integer(payload.get('page_capacity'), 2, 128, '页容量')
    first = _integer(payload.get('range_start'), 1, 999_999_999, '起始曲谱编号')
    last = _integer(payload.get('range_end'), 1, 999_999_999, '结束曲谱编号')
    if first > last:
        raise ValueError('起始曲谱编号须小于或等于结束曲谱编号')
    page_size = _integer(payload.get('page_size'), 1, MAX_PAGE_SIZE, '出页行数')
    rows = payload.get('branches')
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_BRANCHES:
        raise ValueError(f'分馆目录批须为 1 至 {MAX_BRANCHES} 家分馆的数组')
    branches, names = [], set()
    for index, row in enumerate(rows, 1):
        label = f'第 {index} 家分馆的'
        if not isinstance(row, dict):
            raise ValueError(f'第 {index} 家分馆须为对象')
        name = row.get('branch')
        if not isinstance(name, str) or not 1 <= len(name.strip()) <= 40:
            raise ValueError(f'{label}馆名去除首尾空白后须为 1 至 40 字符')
        name = name.strip()
        if name in names:
            raise ValueError(f'{label}馆名 {name} 重复')
        drop_after = row.get('drop_after')
        if drop_after is not None:
            drop_after = _integer(drop_after, 0, 5000, f'{label}断连点')
        branches.append({'name': name, 'drop_after': drop_after,
                         'scores': _read_scores(row.get('scores'), label)})
        names.add(name)
    return {'capacity': capacity, 'first': first, 'last': last,
            'page_size': page_size, 'branches': branches}


def validate(payload) -> None:
    normalize(payload)


def new_state(payload) -> dict:
    """Initial checkpointable merge state for a freshly opened session."""
    cfg = normalize(payload)
    return {
        'frontier': cfg['first'] - 1,
        'partial': [],
        'emitted_pages': 0,
        'resolved_rows': 0,
        'attempts': 0,
        'drops': [],
        'branches': [{
            'name': branch['name'], 'frontier': cfg['first'] - 1,
            'done': False, 'down': False, 'buffer': [], 'visits': [], 'pages_read': 0,
        } for branch in cfg['branches']],
    }


def _build_trees(cfg: dict) -> dict:
    """Rebuild every branch index; identical batches always yield identical pages."""
    trees = {}
    for branch in cfg['branches']:
        tree = BPlusTree(cfg['capacity'])
        for score in branch['scores']:
            tree.insert(score)
        trees[branch['name']] = tree
    return trees


class BranchReader:
    """Incremental leaf-chain cursor, repositioned from the branch frontier.

    Only the frontier is checkpointed: reads are whole leaf pages, so the scan
    always resumes by descending to frontier + 1 and following the leaf links.
    """

    def __init__(self, tree: BPlusTree, first: int, last: int, frontier: int):
        self.tree = tree
        self.last = last
        self.next_key = max(first, frontier + 1)
        self.leaf = None

    def read_page(self):
        """Read the next leaf page: (scores, frontier, done, visit)."""
        if self.leaf is None:
            self.leaf, _ = self.tree.descend(self.next_key)
            # Repositioning after a resume can land on a page that was already
            # fully consumed; skip ahead without counting it as read again.
            while self.leaf is not None and self.leaf.entries \
                    and self.leaf.entries[-1].number < self.next_key:
                self.leaf = self.leaf.next_leaf
        page = self.leaf
        if page is None:
            return [], self.last, True, None
        offset = bisect_left(page.entries, self.next_key, key=score_number)
        stop = bisect_right(page.entries, self.last, lo=offset, key=score_number)
        batch = list(page.entries[offset:stop])
        page_last = page.entries[-1].number if page.entries else None
        if page_last is None or page_last >= self.last or page.next_leaf is None:
            frontier, done = self.last, True
        else:
            frontier, done = page_last, False
            self.leaf = page.next_leaf
            self.next_key = page_last + 1
        visit = {'page_id': page.page_id,
                 'first': page.entries[0].number if page.entries else None,
                 'last': page_last, 'hits': len(batch)}
        return batch, frontier, done, visit


def _drain(cfg: dict, state: dict) -> list:
    """Resolve buffered entries up to the safe frontier; return pages to emit."""
    frontier = min(branch['frontier'] for branch in state['branches'])
    state['frontier'] = frontier
    pending = []
    for branch in state['branches']:
        keep = []
        for item in branch['buffer']:
            (pending if item['number'] <= frontier else keep).append(item)
        branch['buffer'] = keep
    pages = []
    if pending:
        by_number = {}
        for item in pending:
            by_number.setdefault(item['number'], []).append(item)
        for number in sorted(by_number):
            sources = sorted(by_number[number], key=lambda item: item['branch_index'])
            state['partial'].append({
                '曲谱编号': number,
                '谱名': sources[0]['title'],
                '合计份数': sum(item['copies'] for item in sources),
                '来源分馆': [{'分馆': item['branch'], '份数': item['copies']} for item in sources],
            })
            state['resolved_rows'] += 1
        while len(state['partial']) >= cfg['page_size']:
            chunk = state['partial'][:cfg['page_size']]
            state['partial'] = state['partial'][cfg['page_size']:]
            state['emitted_pages'] += 1
            pages.append((state['emitted_pages'], chunk))
    return pages


def advance(payload, state: dict, store, step_budget: int = STEP_BUDGET) -> str:
    """Drive the merge until it completes, blocks, or spends its step budget.

    store.checkpoint(state, pages, status) must durably persist the state and
    any pages emitted this step; it returns False when this attempt has been
    superseded, in which case AttemptLost is raised.  Branch disconnections
    are simulated per branch with `drop_after` (pages read before the link
    drops); each drop fires at most once per session, so a later attempt
    reconnects and finishes the scan.
    """
    cfg = normalize(payload)
    trees = _build_trees(cfg)
    state['attempts'] += 1
    readers = {}
    for branch, spec in zip(state['branches'], cfg['branches']):
        branch['down'] = False  # a new attempt reconnects every branch
        readers[spec['name']] = BranchReader(
            trees[spec['name']], cfg['first'], cfg['last'], branch['frontier'])
    if not store.checkpoint(state, [], 'merging'):
        raise AttemptLost

    steps = 0
    while steps < step_budget:
        candidates = [(branch['frontier'], index)
                      for index, branch in enumerate(state['branches'])
                      if not branch['done'] and not branch['down']]
        if not candidates:
            if all(branch['done'] for branch in state['branches']):
                pages = []
                if state['partial']:
                    state['emitted_pages'] += 1
                    pages.append((state['emitted_pages'], state['partial']))
                    state['partial'] = []
                status = 'complete'
            else:
                pages, status = [], 'blocked'
            if not store.checkpoint(state, pages, status):
                raise AttemptLost
            return status
        _, index = min(candidates)
        branch = state['branches'][index]
        spec = cfg['branches'][index]
        dropped = any(drop['branch'] == spec['name'] for drop in state['drops'])
        if (spec['drop_after'] is not None and not dropped
                and branch['pages_read'] >= spec['drop_after']):
            branch['down'] = True
            state['drops'].append({'branch': spec['name'], 'pages_read': branch['pages_read'],
                                   'attempt': state['attempts']})
            steps += 1
            if not store.checkpoint(state, [], 'merging'):
                raise AttemptLost
            continue
        batch, frontier, done, visit = readers[spec['name']].read_page()
        if visit is not None:
            branch['pages_read'] += 1
            branch['visits'].append(visit)
        branch['frontier'] = frontier
        branch['done'] = done
        branch['buffer'].extend({
            'number': score.number, 'title': score.title,
            'copies': score.available_copies,
            'branch': spec['name'], 'branch_index': index,
        } for score in batch)
        pages = _drain(cfg, state)
        steps += 1
        if not store.checkpoint(state, pages, 'merging'):
            raise AttemptLost
    return 'paused'


def build_report(payload, state: dict, pages: list) -> dict:
    """Assemble the run evidence from the final state and emitted pages."""
    cfg = normalize(payload)
    trees = _build_trees(cfg)
    rows = []
    for page_no, page_rows in pages:
        for row in page_rows:
            rows.append({
                '曲谱编号': row['曲谱编号'], '谱名': row['谱名'], '合计份数': row['合计份数'],
                '来源明细': ' · '.join(f"{source['分馆']} {source['份数']} 份"
                                       for source in row['来源分馆']),
                '借阅判定': '可拣配' if row['合计份数'] else '候补', '页号': page_no,
            })
    ready = sum(row['合计份数'] > 0 for row in rows)
    copies = sum(row['合计份数'] for row in rows)
    inspections = []
    visits = []
    for index, spec in enumerate(cfg['branches']):
        tree = trees[spec['name']]
        directory, _, height = tree.inspect(len(spec['scores']))
        leaf_splits = sum(split.page_type == '叶页' for split in tree.splits)
        inspections.append({
            '分馆': spec['name'], '记录数': len(spec['scores']), '树高': height,
            '页数': len(directory), '叶页分裂': leaf_splits,
            '内部页分裂': len(tree.splits) - leaf_splits, '巡检结论': '通过',
        })
        branch = state['branches'][index]
        visits.extend({
            '分馆': spec['name'], '顺序': order, '叶页号': visit['page_id'],
            '最小编号': visit['first'], '最大编号': visit['last'], '本页命中': visit['hits'],
        } for order, visit in enumerate(branch['visits'], 1))
    interval = f"编号 {cfg['first']} 至 {cfg['last']} 的巡演合单"
    summary = f'{interval}未找到登记曲谱。'
    if rows:
        summary = (f'{interval}命中 {len(rows)} 种曲谱，合计可借 {copies} 份，'
                   f'出页 {len(pages)} 页，{len(rows) - ready} 种候补。')
    if state['drops']:
        names = '、'.join(drop['branch'] for drop in state['drops'])
        summary += f'执行中 {names} 断连，重连后续扫完成。'
    return {
        'summary': summary,
        'metrics': {
            '合单命中曲谱': len(rows), '可拣配曲谱': ready, '候补曲谱': len(rows) - ready,
            '合计可借份数': copies, '出页数': len(pages), '分馆数': len(cfg['branches']),
            '断连分馆': len(state['drops']), '执行轮次': state['attempts'],
            '扫描叶页': sum(branch['pages_read'] for branch in state['branches']),
        },
        'sections': [
            {'title': '巡演借阅单', 'rows': rows},
            {'title': '出页目录', 'rows': [
                {'页号': page_no, '起始编号': page_rows[0]['曲谱编号'],
                 '结束编号': page_rows[-1]['曲谱编号'], '行数': len(page_rows)}
                for page_no, page_rows in pages]},
            {'title': '分馆索引巡检', 'rows': inspections},
            {'title': '叶链读取记录', 'rows': visits},
            {'title': '断连与恢复', 'rows': [
                {'分馆': drop['branch'], '断连时已读叶页': drop['pages_read'],
                 '发生轮次': drop['attempt']} for drop in state['drops']]},
        ],
    }
