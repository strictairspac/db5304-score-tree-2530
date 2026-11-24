import {heading, panel} from '../components/common/heading.js';
import {records} from '../components/common/records.js';
import {topic} from '../constants/project.js';
import {branchStatusLabels, tourStatusLabels} from '../constants/workflow.js';
import {date, esc} from '../utils/dom.js';

const pager = {};      // session id -> current page number
const pageCache = {};  // emitted pages are immutable, so cache by session:page

const pill = (state, label)=>`<span class="status" data-state="${esc(state)}">${esc(label)}</span>`;

function pageTable(page) {
  const rows = page.rows.map(row=>[
    esc(row['曲谱编号']), esc(row['谱名']), esc(row['合计份数']),
    esc(row['来源分馆'].map(source=>`${source['分馆']} ${source['份数']} 份`).join(' · ')),
    row['合计份数'] ? '' : pill('blocked', '候补'),
  ]);
  return records(['曲谱编号', '谱名', '合计份数', '各馆明细', '借阅判定'], rows);
}

export function tourPage(state, identity) {
  const current = state.tours.find(row=>row.id===identity) || state.tours[0];
  if (!current) return heading(topic.modules[3], '各分馆目录合并出的巡演借阅单。')
    + '<p class="empty">提交合单执行后，可在此翻页查看已出的借阅页，并核对还在等待的分馆。</p>';
  const sessions = state.tours.map(row=>[
    `<a href="#tour/${row.id}" ${row.id===current.id?'aria-current="true"':''}>${esc(row.title)}</a>`,
    pill(row.status, tourStatusLabels[row.status] || row.status),
    `${row.emitted_pages} 页`, esc(date(row.updated_at)),
  ]);
  const waiting = current.branches.filter(branch=>branch.status!=='done').map(branch=>branch.name);
  const failed = current.run_status==='failed' && current.status!=='complete';
  const branches = current.branches.map(branch=>[
    esc(branch.name), pill(branch.status, branchStatusLabels[branch.status] || branch.status),
    esc(branch.frontier), esc(branch.pages_read),
  ]);
  const total = current.emitted_pages;
  const pageNo = Math.max(1, Math.min(pager[current.id] || total || 1, Math.max(total, 1)));
  pager[current.id] = pageNo;
  const blocked = current.status==='blocked';
  const tools = blocked && state.session.role==='operator'
    ? `<button id="resume" data-run="${current.run_id}">重连分馆继续合单</button>` : '';
  const pagerBar = total ? `<div class="pager">
      <button type="button" class="secondary" id="prev-page" ${pageNo<=1?'disabled':''}>‹ 上一页</button>
      <span>第 ${pageNo} / ${total} 页</span>
      <button type="button" class="secondary" id="next-page" ${pageNo>=total?'disabled':''}>下一页 ›</button>
    </div>` : '<p class="empty">尚未出页。</p>';
  const note = failed ? '<p class="error">本次执行已失败，可在合并执行记录中查看原因。</p>'
    : waiting.length && current.status!=='complete'
    ? `<p class="note">还在等：${waiting.map(esc).join('、')}${blocked?'（分馆断连，可重连续扫）':''}</p>` : '';
  return `${heading(topic.modules[3], `编号 ${current.range_start} 至 ${current.range_end} · 每页 ${current.page_size} 种 · 各馆已共同确认至编号 ${current.frontier}`, tools)}
    <div class="spread"><section aria-label="合单目录">${records(['合单', '状态', '已出页', '更新时间'], sessions, '暂无合单。')}</section>
    <aside class="brief"><h2>${esc(current.title)}</h2><p>${pill(current.status, tourStatusLabels[current.status] || current.status)}</p>${note}
    <dl><dt>已出页</dt><dd>${total} 页</dd><dt>已查明曲谱</dt><dd>${current.resolved_rows} 种</dd></dl></aside></div>
    ${panel('分馆进度', records(['分馆', '状态', '已确认至编号', '已读叶页'], branches))}
    ${panel(`借阅页 · 第 ${pageNo} 页`, `${pagerBar}<div id="page-view" data-session="${current.id}" data-page="${pageNo}" data-total="${total}"><p class="note">页面载入中…</p></div>`)}`;
}

export function bindTour(api, update, fail) {
  const view = document.getElementById('page-view');
  if (!view) return;
  const sessionId = Number(view.dataset.session);
  const pageNo = Number(view.dataset.page);
  const total = Number(view.dataset.total);
  async function loadPage() {
    if (!total) { view.innerHTML=''; return; }
    const key = `${sessionId}:${pageNo}`;
    try {
      if (!pageCache[key]) pageCache[key] = await api(`/tours/${sessionId}/pages/${pageNo}`);
      const page = pageCache[key];
      view.innerHTML = `<p class="note">本页编号 ${page.first_number} 至 ${page.last_number}</p>${pageTable(page)}`;
    } catch (error) { view.innerHTML=''; fail(error); }
  }
  void loadPage();
  document.getElementById('prev-page')?.addEventListener('click', ()=>{ pager[sessionId]=pageNo-1; void update(); });
  document.getElementById('next-page')?.addEventListener('click', ()=>{ pager[sessionId]=pageNo+1; void update(); });
  document.getElementById('resume')?.addEventListener('click', async event=>{
    const button = event.currentTarget;
    button.disabled = true;
    try { await api(`/runs/${button.dataset.run}/resume`, {method:'POST'}); await update(); }
    catch (error) { fail(error); }
    finally { button.disabled = false; }
  });
}
