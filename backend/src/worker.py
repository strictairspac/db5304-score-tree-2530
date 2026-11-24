import logging
from pathlib import Path
from threading import Event, Thread
from time import sleep

from src.config.settings import ARTIFACTS, cache
from src.services import domain
from src.services.touring import AttemptLost
from src.services.workflow import claim, finish, requeue

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'


def heartbeat(stop):
    while not stop.is_set():
        HEARTBEAT.touch()
        stop.wait(2)


def execute(run_id, token, payload):
    """Drive one run's merge; every step checkpoints so a crash resumes cleanly."""
    outcome = domain.stream(run_id, token, payload)
    if outcome == 'complete':
        finish(run_id, token, result=domain.report(run_id, payload))
    elif outcome == 'paused':
        requeue(run_id, token)
    # A blocked merge keeps the run active: emitted pages stay available and a
    # manual resume (or the stale-run re-claim) reconnects the branches later.


def work():
    HEARTBEAT.unlink(missing_ok=True)
    while True:
        try:
            job = claim()
            HEARTBEAT.touch()
            if not job:
                cache.brpop('worker:wake', timeout=1)
                continue
            run_id, token, payload = job
            stop = Event()
            thread = Thread(target=heartbeat, args=(stop,), daemon=True)
            thread.start()
            try:
                execute(run_id, token, payload)
            except AttemptLost:
                pass  # another attempt owns the run now
            except (ValueError, KeyError, TypeError) as exc:
                finish(run_id, token, error=str(exc)[:500])
            except Exception:
                logging.exception('Domain run failed: %s', run_id)
                finish(run_id, token, error='后台执行失败，请检查输入或查看 worker 日志。')
            finally:
                stop.set()
                thread.join()
        except Exception:
            logging.exception('Worker loop failure')
            sleep(2)


if __name__ == '__main__':
    work()
