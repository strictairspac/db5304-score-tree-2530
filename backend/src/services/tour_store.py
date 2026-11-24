"""Durable storage for touring merge sessions and their emitted pages."""

from sqlalchemy import select
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert

from src.config.settings import IS_MYSQL, engine
from src.constants.workflow import Status, TourStatus
from src.models.tables import cases, runs, tour_pages, tour_sessions
from src.services.touring import new_state, normalize
from src.utils.time import utc_time


def open_session(conn, run_id, case_id, payload):
    """Create the merge session for a freshly enqueued run (same transaction)."""
    cfg = normalize(payload)
    conn.execute(tour_sessions.insert().values(
        run_id=run_id, case_id=case_id, status=TourStatus.MERGING,
        page_size=cfg['page_size'], range_start=cfg['first'], range_end=cfg['last'],
        frontier=cfg['first'] - 1, emitted_pages=0, resolved_rows=0,
        state=new_state(payload), created_at=utc_time(), updated_at=utc_time()))


def _insert_page(conn, session_id, page_no, rows):
    """Idempotent page emission: the unique (session, page) key keeps retries
    from creating copies, so an already emitted page keeps its content."""
    values = {'session_id': session_id, 'page_no': page_no, 'rows': rows,
              'first_number': rows[0]['曲谱编号'], 'last_number': rows[-1]['曲谱编号'],
              'created_at': utc_time()}
    if IS_MYSQL:
        conn.execute(mysql_insert(tour_pages).values(**values)
                     .on_duplicate_key_update(id=tour_pages.c.id))
    else:
        conn.execute(postgres_insert(tour_pages).values(**values)
                     .on_conflict_do_nothing(index_elements=['session_id', 'page_no']))


class SessionStore:
    """Checkpoint store bound to one run attempt.

    Every checkpoint commits the merge state together with the pages emitted
    during that step, guarded by the run's attempt token: if another attempt
    has taken over (manual resume or stale re-claim), the commit is rejected
    and the caller raises AttemptLost instead of writing stale progress.
    """

    def __init__(self, run_id, token):
        self.run_id = run_id
        self.token = token
        self.session_id = None

    def load(self):
        with engine.connect() as conn:
            row = conn.execute(select(tour_sessions.c.id, tour_sessions.c.state)
                               .where(tour_sessions.c.run_id == self.run_id)).mappings().first()
        if row is None:
            raise ValueError('找不到该执行对应的合单会话')
        self.session_id = row['id']
        return dict(row['state'])

    def checkpoint(self, state, pages, status):
        with engine.begin() as conn:
            owner = conn.execute(runs.update().where(
                runs.c.id == self.run_id, runs.c.attempt_token == self.token,
                runs.c.status == Status.RUNNING).values(started_at=utc_time())).rowcount
            if not owner:
                return False
            for page_no, rows in pages:
                _insert_page(conn, self.session_id, page_no, rows)
            conn.execute(tour_sessions.update()
                         .where(tour_sessions.c.id == self.session_id)
                         .values(status=status, frontier=state['frontier'], state=state,
                                 emitted_pages=state['emitted_pages'],
                                 resolved_rows=state['resolved_rows'],
                                 updated_at=utc_time()))
            return True


def _branch_summary(branch):
    status = 'done' if branch['done'] else 'down' if branch['down'] else 'reading'
    return {'name': branch['name'], 'status': status,
            'frontier': branch['frontier'], 'pages_read': branch['pages_read']}


def list_sessions():
    with engine.connect() as conn:
        rows = conn.execute(
            select(tour_sessions, runs.c.status.label('run_status'), cases.c.title)
            .join(runs, tour_sessions.c.run_id == runs.c.id)
            .join(cases, tour_sessions.c.case_id == cases.c.id)
            .order_by(tour_sessions.c.id.desc()).limit(100)).mappings()
    result = []
    for row in rows:
        item = dict(row)
        state = item.pop('state')
        item['branches'] = [_branch_summary(branch) for branch in state['branches']]
        result.append(item)
    return result


def session_pages(conn, session_id):
    rows = conn.execute(select(tour_pages.c.page_no, tour_pages.c.rows)
                        .where(tour_pages.c.session_id == session_id)
                        .order_by(tour_pages.c.page_no)).mappings()
    return [(row['page_no'], row['rows']) for row in rows]


def get_session(run_id):
    with engine.connect() as conn:
        row = conn.execute(select(tour_sessions).where(tour_sessions.c.run_id == run_id)
                           ).mappings().first()
    return dict(row) if row else None


def get_page(session_id, page_no):
    with engine.connect() as conn:
        row = conn.execute(select(tour_pages)
                           .where(tour_pages.c.session_id == session_id,
                                  tour_pages.c.page_no == page_no)).mappings().first()
    return dict(row) if row else None
