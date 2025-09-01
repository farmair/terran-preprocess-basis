'use strict';

/**
 * 임주원 프로젝트 - Lambda 단일 파일 (비동기/동시성 제어 포함)
 * - SQS Records 이벤트/직접 이벤트 모두 처리
 * - 비거나 누락된 필드는 동적으로 기본값 채움
 * - (datadate × item) 조합을 비동기 처리, 동시성 제한 가능
 */

const axios = require('axios');
const http = require('http');
const https = require('https');

// Axios Keep-Alive (대량 호출 안정성/성능 향상)
const httpAgent = new http.Agent({ keepAlive: true });
const httpsAgent = new https.Agent({ keepAlive: true });
const ax = axios.create({ httpAgent, httpsAgent });

// 동시성 제한 (환경변수로 조절 가능, 기본 10)
const CONCURRENCY_LIMIT = Number(process.env.CONCURRENCY_LIMIT || 10);

/* =========================
 *  코드 리스트 fetch 유틸
 * ========================= */
async function fetchCodeList(codeName) {
  try {
    const url = `https://refs.terran.kr/${codeName}.json`;
    const res = await ax.get(url);
    if (res.data && Array.isArray(res.data.item_list)) {
      return res.data.item_list;
    } else {
      throw new Error('item_list not found in response');
    }
  } catch (err) {
    throw new Error(`Failed to fetch ${codeName}: ${err.message}`);
  }
}

/* =========================
 *  이벤트 → payload 배열 정규화
 * ========================= */
function toPayloads(event) {
  let e = event;
  if (typeof e === 'string') {
    try { e = JSON.parse(e); } catch { /* 문자열 그대로 두되 아래 분기에서 처리 */ }
  }
  if (e && Array.isArray(e.Records)) {
    return e.Records.map((r) => {
      if (!r) return {};
      const body = r.body;
      if (typeof body === 'string') {
        try { return JSON.parse(body); } catch { return {}; }
      }
      if (typeof body === 'object' && body !== null) return body;
      return {};
    });
  }
  return [e ?? {}];
}

/* ==========================================
 *  기본값 채우기: datadate_arr, market_arr, item_arr, db_arr
 *  - 입력 필드명: datadate / market / item (배열 가정)
 *  - 누락/빈배열이면 규칙에 따라 생성
 * ========================================== */
async function fillWithDynamicDefaults(p) {
  // 날짜: 제공 없으면 오늘 기준 과거 3일(YYYYMMDD) 생성
  let datadate_arr;
  if (Array.isArray(p.datadate) && p.datadate.length > 0) {
    datadate_arr = p.datadate;
  } else {
    const today = new Date();
    datadate_arr = [];
    for (let i = 1; i <= 3; i++) {
      const d = new Date(today);
      d.setDate(today.getDate() - i);
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, '0');
      const day = String(d.getDate()).padStart(2, '0');
      datadate_arr.unshift(`${y}${m}${day}`); // 오래된 날짜가 앞에 오도록
    }
  }

  // 마켓: 제공 없으면 레퍼런스에서 전체 목록
  let market_arr;
  if (Array.isArray(p.market) && p.market.length > 0) {
    market_arr = p.market;
  } else {
    market_arr = await fetchCodeList('market_code_list');
  }

  // 아이템(채소): 제공 없으면 레퍼런스에서 전체 목록
  let item_arr;
  if (Array.isArray(p.item) && p.item.length > 0) {
    item_arr = p.item;
  } else {
    item_arr = await fetchCodeList('vegetable_code_list');
  }

  let db_arr;
  if (Array.isArray(p.db) && p.db.length > 0) {   
    db_arr = p.db;
  } else {
    db_arr = ['test']; // 기본 DB 테이블
  }

  return { datadate_arr, market_arr, item_arr };
}

/* =========================
 *  동시성 제한 실행 유틸
 *  - tasks: () => Promise 를 원소로 갖는 배열
 * ========================= */
async function runTasksWithConcurrency(tasks, limit) {
  const results = new Array(tasks.length);
  let idx = 0;

  async function worker() {
    while (true) {
      const current = idx++;
      if (current >= tasks.length) break;
      try {
        results[current] = await tasks[current]();
      } catch (err) {
        results[current] = { ok: false, error: err?.message || 'unknown' };
      }
    }
  }

  const workers = Array.from({ length: Math.min(limit, tasks.length) }, worker);
  await Promise.all(workers);
  return results;
}

/* =========================
 *  업무 1건 처리 (비동기)
 * ========================= */
async function handleOne({ datadate, item, market_arr }, meta) {
  // 여기서 실제 처리 (예: 외부 API 호출/DB 저장 등)
  console.log(`[payload:${meta.payloadIdx} job:${meta.jobIdx}] datadate=${datadate}, item=${item}, markets=${market_arr?.length ?? 0}`);

  // 예시 외부 호출 (필요 시 활성화)
  // await ax.post('http://your-endpoint/ingest', { datadate, item, market_arr });

  return { ok: true, datadate, item };
}

/* =======================================================
 *  각 payload 처리
 *  - 이중 for문 내에서 handleOne을 "비동기"로 호출
 *  - 동시에 너무 많이 안 나가게 동시성 제한 적용
 * ======================================================= */
async function handleEachPayload(p, payloadIdx) {
  const { datadate_arr, item_arr, market_arr } = p;

  console.log(`[payload:${payloadIdx}] datadate_arr: ${datadate_arr.length}, item_arr: ${item_arr.length}, market_arr: ${market_arr.length}`);

  // 이중 for문에서 작업(task) 생성
  const tasks = [];
  let jobIdx = 0;

  for (const datadate of datadate_arr) {
    for (const item of item_arr) {
      // 태식아 여기: 이 자리에서 handleOne 비동기 호출되도록 태스크 등록
      tasks.push(() => handleOne({ datadate, item, market_arr }, { payloadIdx, jobIdx: jobIdx++ }));
    }
  }

  // 동시성 제한으로 병렬 실행 (끝까지 기다림)
  const results = await runTasksWithConcurrency(tasks, CONCURRENCY_LIMIT);

  const failed = results.filter(r => !r || !r.ok);
  return { index: payloadIdx, ok: failed.length === 0, total: results.length, failed: failed.length };
}

/* =========================
 *  Lambda 핸들러
 * ========================= */
exports.handler = async (event/*, context*/) => {
  /**
   * (테스트/디버깅용) 슬랙으로 이벤트 객체 전송 후 die (조기 종료)
   * - SLACK_WEBHOOK_URL 환경변수 필요
   */
  if (process.env.SLACK_WEBHOOK_URL) {
    try {
      const slackPayload = {
        text: `[Lambda Debug] Received event:\n\`\`\`${typeof event === 'string' ? event : JSON.stringify(event, null, 2)}\`\`\``
      };
      await ax.post(process.env.SLACK_WEBHOOK_URL, slackPayload);
    } catch (err) {
      console.error('Failed to send event to Slack:', err?.message || err);
    }
    // die: 슬랙 전송 후 즉시 종료
    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Hi, Event sent to Slack for debug. Lambda exited early.' }),
    };
  }
  console.log('Received event:', typeof event === 'string' ? event : JSON.stringify(event));

  // 1) 이벤트 정규화 → raw payloads
  const rawPayloads = toPayloads(event);

  // 2) 기본값 채워 처리 가능한 payload로 변환
  const payloads = [];
  for (let i = 0; i < rawPayloads.length; i++) {
    payloads.push(await fillWithDynamicDefaults(rawPayloads[i]));
  }

  // 3) payload 단위 처리: Promise.all로 병렬 실행
  const results = await Promise.all(
    payloads.map((p, i) =>
      handleEachPayload(p, i).catch(err => {
        console.error(`[payload ${i}] Error:`, err?.message || err);
        return { index: i, ok: false, error: err?.message || 'unknown' };
      })
    )
  );

  const success = results.filter(r => r.ok).length;
  const fail = results.length - success;

  return {
    statusCode: fail === 0 ? 200 : 207, // 일부 실패면 207
    body: JSON.stringify({
      message: `processed payloads: ok=${success}, fail=${fail}`,
      results,
    }),
  };
};
