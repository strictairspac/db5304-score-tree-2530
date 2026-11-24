"""Touring borrow order: merge inspected branch catalogs into paged output."""

from sqlalchemy import select

from src.config.settings import engine
from src.models.tables import tour_sessions
from src.services.touring import (STEP_BUDGET, advance, build_report, new_state,
                                  normalize)
from src.services.tour_store import SessionStore, open_session, session_pages

SPEC = {
    'name': '榆弦谱库·巡演借谱合单',
    'summary': '把几家分馆已巡检的目录批次合开一张巡演借谱单，按编号出页，同号库存相加。',
    'customer': '榆弦社区音乐中心 · 曲谱流通组',
    'entity_labels': ['分馆目录批', '合并执行', '巡演借阅单'],
    'field_labels': {
        'page_capacity': '页容量（最大键数）',
        'range_start': '起始曲谱编号（含）',
        'range_end': '结束曲谱编号（含）',
        'page_size': '出页行数（每页曲谱种数）',
        'branches': '分馆目录批',
        'branch': '分馆名称',
        'drop_after': '断连点（读取叶页数，可选）',
        'scores': '曲谱目录',
        'number': '曲谱编号',
        'title': '谱名',
        'available_copies': '可借份数',
    },
    'guidance': '按到达顺序录入各分馆已巡检的曲谱批次，设置页容量、编号区间和每页行数。合单沿各馆叶链按需扫描，满页即出；断连点用于演示分馆断连，重连后从断点续扫，已出的页保持不变。',
    'sample': {
        'page_capacity': 3,
        'range_start': 1204,
        'range_end': 1212,
        'page_size': 4,
        'branches': [
            {'branch': '榆荫分馆', 'scores': [
                {'number': 1209, 'title': '江湾晨曲', 'available_copies': 2},
                {'number': 1205, 'title': '松影三重奏', 'available_copies': 3},
                {'number': 1214, 'title': '旧街进行曲', 'available_copies': 2},
                {'number': 1201, 'title': '纸鸢圆舞曲', 'available_copies': 1},
                {'number': 1212, 'title': '灯塔小步舞曲', 'available_copies': 1},
                {'number': 1204, 'title': '榆荫弦乐合奏', 'available_copies': 2},
                {'number': 1210, 'title': '竹桥间奏', 'available_copies': 0},
                {'number': 1207, 'title': '溪畔变奏曲', 'available_copies': 0},
                {'number': 1216, 'title': '雾中五重奏', 'available_copies': 4},
            ]},
            {'branch': '河岸分馆', 'drop_after': 2, 'scores': [
                {'number': 1206, 'title': '沙洲小调', 'available_copies': 2},
                {'number': 1211, 'title': '稻穗随想曲', 'available_copies': 3},
                {'number': 1204, 'title': '榆荫弦乐合奏', 'available_copies': 1},
                {'number': 1215, 'title': '涟漪重奏', 'available_copies': 2},
                {'number': 1208, 'title': '北窗练习曲', 'available_copies': 1},
                {'number': 1219, 'title': '远山终曲', 'available_copies': 2},
                {'number': 1202, 'title': '长廊二重奏', 'available_copies': 2},
            ]},
            {'branch': '北窗分馆', 'scores': [
                {'number': 1205, 'title': '松影三重奏', 'available_copies': 1},
                {'number': 1213, 'title': '晴堤短歌', 'available_copies': 2},
                {'number': 1209, 'title': '江湾晨曲', 'available_copies': 1},
                {'number': 1203, 'title': '石阶小夜曲', 'available_copies': 1},
                {'number': 1212, 'title': '灯塔小步舞曲', 'available_copies': 2},
                {'number': 1217, 'title': '晚汐重奏', 'available_copies': 1},
                {'number': 1208, 'title': '北窗练习曲', 'available_copies': 2},
            ]},
        ],
    },
    'expect': {
        '合单命中曲谱': 9,
        '合计可借份数': 21,
        '候补曲谱': 2,
        '出页数': 3,
        '断连分馆': 1,
        '扫描叶页': 9,
    },
}


def validate(payload) -> None:
    normalize(payload)


def stream(run_id, token, payload) -> str:
    """Resume the merge for one run from its last checkpoint."""
    store = SessionStore(run_id, token)
    state = store.load()
    return advance(payload, state, store, STEP_BUDGET)


def report(run_id, payload) -> dict:
    """Build the run evidence from the finished session and emitted pages."""
    with engine.connect() as conn:
        session = conn.execute(select(tour_sessions)
                               .where(tour_sessions.c.run_id == run_id)).mappings().first()
        pages = session_pages(conn, session['id'])
    return build_report(payload, session['state'], pages)


__all__ = ['SPEC', 'validate', 'stream', 'report', 'open_session', 'new_state']
