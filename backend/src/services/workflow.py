import hashlib
import uuid

from fastapi import HTTPException
from sqlalchemy import and_, or_, select

from src.config.settings import cache, engine
from src.constants.workflow import Status, TourStatus
from src.models.tables import audit, cases, evidence, runs, tour_sessions
from src.services import domain
from src.utils.encoding import canonical
from src.utils.time import utc_time


def log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action, reference=str(reference), created_at=utc_time()))


def recent(table):
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(table).order_by(table.c.id.desc()).limit(100)).mappings()]


def found(conn, table, identity):
    row = conn.execute(select(table).where(table.c.id == identity)).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该记录')
    return dict(row)


def make_case(title, payload, actor):
    title = title.strip()
    if not title:
        raise HTTPException(422, '档案名称不能为空')
    try:
        if len(canonical(payload)) > 512 * 1024:
            raise ValueError('业务输入超过 512 KiB')
        if callable(getattr(domain, 'validate', None)):
            domain.validate(payload)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from None
    with engine.begin() as conn:
        case_id = conn.execute(cases.insert().values(slug=uuid.uuid4().hex, title=title, payload=payload,
                               author=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'create_case', case_id)
        return found(conn, cases, case_id)


def wake():
    try:
        cache.lpush('worker:wake', 'queued')
        cache.ltrim('worker:wake', 0, 99)
    except Exception:
        # The durable database queue is polled even if Redis is temporarily unavailable.
        pass


def enqueue(case_id, actor):
    with engine.begin() as conn:
        case = found(conn, cases, case_id)
        run_id = conn.execute(runs.insert().values(case_id=case_id, status=Status.QUEUED,
                              owner=actor, created_at=utc_time())).inserted_primary_key[0]
        try:
            domain.open_session(conn, run_id, case_id, case['payload'])
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from None
        log(conn, actor, 'enqueue', run_id)
        result = found(conn, runs, run_id)
    wake()
    return result


def claim():
    ready = or_(runs.c.status == Status.QUEUED,
                and_(runs.c.status == Status.RUNNING, runs.c.started_at < utc_time(-600)))
    with engine.begin() as conn:
        run = conn.execute(select(runs.c.id, runs.c.case_id).where(ready).order_by(runs.c.id)
                           .limit(1).with_for_update(skip_locked=True)).first()
        if not run:
            return None
        token = str(uuid.uuid4())
        conn.execute(runs.update().where(runs.c.id == run.id).values(
            started_at=utc_time(), status=Status.RUNNING, attempt_token=token))
        return run.id, token, found(conn, cases, run.case_id)['payload']


def finish(run_id, token, result=None, error=None):
    digest = hashlib.sha256(canonical(result)).hexdigest() if result is not None else None
    status = Status.FAILED if error else Status.COMPLETE
    with engine.begin() as conn:
        count = conn.execute(runs.update().where(runs.c.id == run_id,
                        runs.c.attempt_token == token, runs.c.status == Status.RUNNING)
                        .values(status=status, finished_at=utc_time(), error=error)).rowcount
        if not count:
            return
        if result is not None:
            conn.execute(evidence.insert().values(run_id=run_id, report=result, digest=digest))
        log(conn, 'worker', status.value, run_id)


def requeue(run_id, token):
    """Hand a paused run back to the queue so its next claim keeps merging."""
    with engine.begin() as conn:
        conn.execute(runs.update().where(runs.c.id == run_id, runs.c.attempt_token == token,
                                         runs.c.status == Status.RUNNING)
                     .values(status=Status.QUEUED))
    wake()


def resume(run_id, actor):
    """Reconnect a blocked or stalled merge: rotate the attempt token so any
    lingering attempt stops at its next checkpoint, then re-queue the run."""
    with engine.begin() as conn:
        run = found(conn, runs, run_id)
        if run['status'] != Status.RUNNING:
            raise HTTPException(409, '该执行不在可恢复状态')
        session = conn.execute(select(tour_sessions.c.status)
                               .where(tour_sessions.c.run_id == run_id)).mappings().first()
        blocked = session and session['status'] == TourStatus.BLOCKED
        stalled = run['started_at'] and run['started_at'] < utc_time(-30)
        if not (blocked or stalled):
            raise HTTPException(409, '合单正在执行，请稍后再试')
        conn.execute(runs.update().where(runs.c.id == run_id)
                     .values(status=Status.QUEUED, attempt_token=str(uuid.uuid4())))
        log(conn, actor, 'resume', run_id)
        result = found(conn, runs, run_id)
    wake()
    return result


def review(identity, note, actor):
    note = note.strip()
    if not note:
        raise HTTPException(422, '复核意见不能为空')
    with engine.begin() as conn:
        found(conn, evidence, identity)
        count = conn.execute(evidence.update().where(evidence.c.id == identity, evidence.c.reviewed_at.is_(None))
                            .values(review_note=note, reviewer=actor, reviewed_at=utc_time())).rowcount
        if count != 1:
            raise HTTPException(409, '该证据已经复核')
        log(conn, actor, 'review', identity)
        return found(conn, evidence, identity)
