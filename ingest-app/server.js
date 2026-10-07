#!/usr/bin/env node
/*
 * INDODANA ingest control panel — local API server.
 *
 * Runs the Snowflake work by shelling out to the `snow` CLI with the
 * existing named connection. That is deliberate: the account uses OAuth
 * authorization-code auth, which the Node driver cannot complete headless.
 * Delegating to the CLI means no credential ever lives in this process,
 * in the browser, or on disk here.
 *
 * SQL is written to a temp file and passed with -f rather than -q, which
 * sidesteps shell quoting entirely. All client-supplied values are either
 * integer-validated or resolved against a server-side whitelist, so no
 * user input is ever concatenated into SQL.
 */

'use strict';

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFile } = require('child_process');

const PORT = Number(process.env.PORT || 5190);
// No hardcoded default: the connection name is account-specific, so it has
// to be supplied. Failing loudly at startup beats failing on every request.
const CONNECTION = process.env.SNOW_CONNECTION;
const WAREHOUSE = process.env.SNOW_WAREHOUSE || 'DEFAULT_WH';
const DB = 'DEMO_INDODANA';
const DBT_PROJECT = `${DB}.DBT.INDODANA_LENDING`;
const PUBLIC_DIR = path.join(__dirname, 'public');

if (!CONNECTION) {
  console.error(
    '\n  SNOW_CONNECTION is not set.\n\n' +
    '  Pass the name of a Snowflake CLI connection, e.g.\n' +
    '    SNOW_CONNECTION=my_connection node server.js\n\n' +
    '  List available connections with:  snow connection list\n'
  );
  process.exit(1);
}

// dbt commands are never taken from the client verbatim — the UI sends a
// key, the server owns the argument string.
const DBT_PRESETS = {
  build:         { args: 'build',                   label: 'Full build + tests (bronze → gold)' },
  run:           { args: 'run',                     label: 'Run models only (no tests)' },
  snapshot:      { args: 'snapshot',                label: 'Snapshots only (SCD2 history)' },
  test:          { args: 'test',                    label: 'Tests only' },
  silver:        { args: 'run --select silver',     label: 'Silver layer only (CDC resolve)' },
  gold:          { args: 'run --select gold',       label: 'Gold layer only (dims/facts/marts)' },
  incremental:   { args: 'build --select silver+',  label: 'Silver + everything downstream' },
  full_refresh:  { args: 'build --full-refresh',    label: 'FULL REFRESH (rebuild from scratch)' }
};

// ---------------------------------------------------------------------
// Snowflake execution
// ---------------------------------------------------------------------
function runSql(sql, timeoutMs) {
  return new Promise((resolve, reject) => {
    const file = path.join(
      os.tmpdir(),
      `indodana-${Date.now()}-${Math.random().toString(36).slice(2)}.sql`
    );
    fs.writeFileSync(file, sql, 'utf8');

    execFile(
      'snow',
      ['sql', '-c', CONNECTION, '-f', file, '--format', 'json',
       '--warehouse', WAREHOUSE, '--database', DB],
      { maxBuffer: 128 * 1024 * 1024, timeout: timeoutMs || 15 * 60 * 1000 },
      (err, stdout, stderr) => {
        fs.unlink(file, () => {});
        if (err) {
          return reject(new Error(
            (stderr || '').trim() || (stdout || '').trim() || err.message
          ));
        }
        try {
          resolve(JSON.parse(stdout));
        } catch (e) {
          reject(new Error('Could not parse CLI output: ' + String(stdout).slice(0, 1500)));
        }
      }
    );
  });
}

// `snow sql --format json` returns an array of rows for a single statement,
// or an array of row-arrays when several statements ran. Normalise to rows.
function rows(result) {
  if (!Array.isArray(result)) return [];
  if (result.length && Array.isArray(result[0])) return result[0];
  return result;
}

function stripAnsi(s) {
  // eslint-disable-next-line no-control-regex
  return String(s || '').replace(/\u001b\[[0-9;]*m/g, '');
}

function intParam(value, min, max, fallback) {
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

// ---------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------
const SQL_LAYER_COUNTS = `
select 'BRONZE' as layer, 'raw_customers'             as object_name, count(*) as row_count from ${DB}.BRONZE.RAW_CUSTOMERS
union all select 'BRONZE', 'raw_leads',                count(*) from ${DB}.BRONZE.RAW_LEADS
union all select 'BRONZE', 'raw_loan_applications',    count(*) from ${DB}.BRONZE.RAW_LOAN_APPLICATIONS
union all select 'BRONZE', 'raw_loans',                count(*) from ${DB}.BRONZE.RAW_LOANS
union all select 'BRONZE', 'raw_installments',         count(*) from ${DB}.BRONZE.RAW_INSTALLMENTS
union all select 'BRONZE', 'raw_payments',             count(*) from ${DB}.BRONZE.RAW_PAYMENTS
union all select 'BRONZE', 'raw_collection_activities',count(*) from ${DB}.BRONZE.RAW_COLLECTION_ACTIVITIES
union all select 'SILVER', 'slv_customers',            count(*) from ${DB}.SILVER.SLV_CUSTOMERS
union all select 'SILVER', 'slv_loans',                count(*) from ${DB}.SILVER.SLV_LOANS
union all select 'SILVER', 'slv_installments',         count(*) from ${DB}.SILVER.SLV_INSTALLMENTS
union all select 'SILVER', 'slv_payments',             count(*) from ${DB}.SILVER.SLV_PAYMENTS
union all select 'SILVER', 'snap_customer_credit',     count(*) from ${DB}.SILVER.SNAP_CUSTOMER_CREDIT
union all select 'SILVER', 'snap_loan_status',         count(*) from ${DB}.SILVER.SNAP_LOAN_STATUS
union all select 'GOLD',   'dim_customer',             count(*) from ${DB}.GOLD.DIM_CUSTOMER
union all select 'GOLD',   'fct_loan',                 count(*) from ${DB}.GOLD.FCT_LOAN
union all select 'GOLD',   'fct_payment',              count(*) from ${DB}.GOLD.FCT_PAYMENT
union all select 'GOLD',   'mart_customer_360',        count(*) from ${DB}.GOLD.MART_CUSTOMER_360
union all select 'GOLD',   'mart_portfolio_quality',   count(*) from ${DB}.GOLD.MART_PORTFOLIO_QUALITY
union all select 'GOLD',   'mart_origination_funnel',  count(*) from ${DB}.GOLD.MART_ORIGINATION_FUNNEL
order by layer desc, object_name
`;

// Bronze version counts prove CDC is real: more raw rows than business keys
// means updates have genuinely been captured, not just inserts.
const SQL_CDC_PRESSURE = `
with b as (
  select 'customers' as entity, count(*) as raw_rows, count(distinct customer_id) as keys,
         count_if(_op = 'U') as updates, count_if(_op = 'D') as deletes
  from ${DB}.BRONZE.RAW_CUSTOMERS
  union all
  select 'loans', count(*), count(distinct loan_id), count_if(_op='U'), count_if(_op='D')
  from ${DB}.BRONZE.RAW_LOANS
  union all
  select 'installments', count(*), count(distinct installment_id), count_if(_op='U'), count_if(_op='D')
  from ${DB}.BRONZE.RAW_INSTALLMENTS
  union all
  select 'leads', count(*), count(distinct lead_id), count_if(_op='U'), count_if(_op='D')
  from ${DB}.BRONZE.RAW_LEADS
  union all
  select 'applications', count(*), count(distinct application_id), count_if(_op='U'), count_if(_op='D')
  from ${DB}.BRONZE.RAW_LOAN_APPLICATIONS
)
select entity, raw_rows, keys, raw_rows - keys as extra_versions, updates, deletes
from b order by entity
`;

const SQL_HEADLINE = `
select
  (select count(*) from ${DB}.GOLD.FCT_LOAN)                                  as loans_total,
  (select count(*) from ${DB}.GOLD.FCT_LOAN where loan_status = 'ACTIVE')     as loans_active,
  (select sum(principal_amount) from ${DB}.GOLD.FCT_LOAN)                     as principal_disbursed,
  (select sum(outstanding_principal) from ${DB}.GOLD.FCT_LOAN
     where loan_status <> 'PAID_OFF')                                         as outstanding_principal,
  (select round(div0(sum(iff(is_npl, outstanding_principal, 0)),
                     sum(outstanding_principal)) * 100, 2)
     from ${DB}.GOLD.FCT_LOAN where loan_status <> 'PAID_OFF')                as npl_ratio_pct,
  (select round(div0(count_if(is_approved), count(*)) * 100, 2)
     from ${DB}.GOLD.FCT_LOAN_APPLICATION)                                    as approval_pct,
  (select count(*) from ${DB}.GOLD.MART_CUSTOMER_360)                         as customers_total,
  (select sum(amount) from ${DB}.GOLD.FCT_PAYMENT)                            as total_collected,
  (select max(_ingested_at) from ${DB}.BRONZE.RAW_CUSTOMERS)                  as bronze_latest_ingest,
  (select max(cdc_commit_ts) from ${DB}.SILVER.SLV_LOANS)                     as silver_latest_commit
`;

const SQL_BATCHES = `
select batch_id, batch_type, started_at, finished_at, rows_landed, detail
from ${DB}.BRONZE.BATCH_LOG
order by started_at desc
limit 12
`;

// Freshness gap: rows sitting in Bronze that Silver has not merged yet.
// This is what makes the "ingest then run dbt" story visible rather than
// something the user has to take on trust.
const SQL_PENDING = `
select
  (select count(*) from ${DB}.BRONZE.RAW_CUSTOMERS b
     where b._ingested_at > coalesce(
       (select max(cdc_ingested_at) from ${DB}.SILVER.SLV_CUSTOMERS), '1900-01-01'::timestamp_ntz))
                                                               as customers_pending,
  (select count(*) from ${DB}.BRONZE.RAW_LOANS b
     where b._ingested_at > coalesce(
       (select max(cdc_ingested_at) from ${DB}.SILVER.SLV_LOANS), '1900-01-01'::timestamp_ntz))
                                                               as loans_pending,
  (select count(*) from ${DB}.BRONZE.RAW_PAYMENTS b
     where b._ingested_at > coalesce(
       (select max(cdc_ingested_at) from ${DB}.SILVER.SLV_PAYMENTS), '1900-01-01'::timestamp_ntz))
                                                               as payments_pending
`;

// ---------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------
const handlers = {
  async status() {
    const [counts, pressure, headline, batches, pending] = await Promise.all([
      runSql(SQL_LAYER_COUNTS, 120000).then(rows),
      runSql(SQL_CDC_PRESSURE, 120000).then(rows),
      runSql(SQL_HEADLINE, 120000).then(rows),
      runSql(SQL_BATCHES, 120000).then(rows),
      runSql(SQL_PENDING, 120000).then(rows)
    ]);
    return {
      counts,
      cdcPressure: pressure,
      headline: headline[0] || {},
      batches,
      pending: pending[0] || {},
      connection: CONNECTION,
      warehouse: WAREHOUSE
    };
  },

  async ingest(body) {
    const n = intParam(body.customers, 1, 5000, 150);
    const monthsBack = intParam(body.monthsBack, 0, 60, 0);
    const sql = `call ${DB}.GEN.SP_INGEST_BATCH(${n}, ` +
      `date_trunc('month', dateadd(month, -${monthsBack}, current_date()))::date)`;
    const out = rows(await runSql(sql, 20 * 60 * 1000));
    return { message: Object.values(out[0] || {})[0] || 'no output', sql };
  },

  async cdcUpdate(body) {
    const c = intParam(body.customers, 0, 20000, 120);
    const l = intParam(body.loans, 0, 20000, 200);
    const sql = `call ${DB}.GEN.SP_CDC_UPDATE(${c}, ${l})`;
    const out = rows(await runSql(sql, 20 * 60 * 1000));
    return { message: Object.values(out[0] || {})[0] || 'no output', sql };
  },

  async cdcDelete(body) {
    const le = intParam(body.leads, 0, 5000, 15);
    const ap = intParam(body.applications, 0, 5000, 10);
    const sql = `call ${DB}.GEN.SP_CDC_DELETE(${le}, ${ap})`;
    const out = rows(await runSql(sql, 20 * 60 * 1000));
    return { message: Object.values(out[0] || {})[0] || 'no output', sql };
  },

  async dbt(body) {
    const preset = DBT_PRESETS[body.preset];
    if (!preset) {
      throw new Error(`Unknown dbt preset "${body.preset}". ` +
        `Allowed: ${Object.keys(DBT_PRESETS).join(', ')}`);
    }
    const sql = `execute dbt project ${DBT_PROJECT} args='${preset.args}'`;
    let out;
    try {
      out = rows(await runSql(sql, 45 * 60 * 1000));
    } catch (e) {
      // A failing dbt run surfaces as a SQL error; the useful detail is in
      // the message, so pass it through instead of a bare 500.
      return { success: false, args: preset.args, log: stripAnsi(e.message) };
    }
    const r = out[0] || {};
    return {
      success: r.SUCCESS === true || r.SUCCESS === 'true',
      args: preset.args,
      exception: r.EXCEPTION && r.EXCEPTION !== 'None' ? r.EXCEPTION : null,
      log: stripAnsi(r.STDOUT)
    };
  },

  presets() {
    // Wrapped in an object, not returned as a bare array: the response is
    // spread into the envelope, and spreading an array would yield
    // {0: ..., 1: ...} on the client.
    return {
      presets: Object.entries(DBT_PRESETS)
        .map(([k, v]) => ({ key: k, label: v.label, args: v.args }))
    };
  }
};

// ---------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml'
};

function sendJson(res, code, payload) {
  const data = JSON.stringify(payload);
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(data),
    'Cache-Control': 'no-store'
  });
  res.end(data);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let raw = '';
    req.on('data', (c) => {
      raw += c;
      if (raw.length > 1e6) {
        reject(new Error('Request body too large'));
        req.destroy();
      }
    });
    req.on('end', () => {
      if (!raw) return resolve({});
      try { resolve(JSON.parse(raw)); } catch (e) { reject(new Error('Invalid JSON body')); }
    });
    req.on('error', reject);
  });
}

function serveStatic(req, res, urlPath) {
  const rel = urlPath === '/' ? 'index.html' : urlPath.replace(/^\/+/, '');
  const full = path.join(PUBLIC_DIR, rel);
  // keep traversal inside public/
  if (!full.startsWith(PUBLIC_DIR)) {
    res.writeHead(403).end('Forbidden');
    return;
  }
  fs.readFile(full, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' }).end('Not found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(full)] || 'application/octet-stream',
      'Cache-Control': 'no-store'
    });
    res.end(data);
  });
}

const ROUTES = {
  'GET /api/status':     () => handlers.status(),
  'GET /api/presets':    () => handlers.presets(),
  'POST /api/ingest':     (b) => handlers.ingest(b),
  'POST /api/cdc-update': (b) => handlers.cdcUpdate(b),
  'POST /api/cdc-delete': (b) => handlers.cdcDelete(b),
  'POST /api/dbt':        (b) => handlers.dbt(b)
};

const server = http.createServer(async (req, res) => {
  const urlPath = (req.url || '/').split('?')[0];
  const key = `${req.method} ${urlPath}`;

  if (!urlPath.startsWith('/api/')) return serveStatic(req, res, urlPath);

  const route = ROUTES[key];
  if (!route) return sendJson(res, 404, { error: `No route for ${key}` });

  const started = Date.now();
  try {
    const body = req.method === 'POST' ? await readBody(req) : {};
    const result = await route(body);
    sendJson(res, 200, { ok: true, elapsedMs: Date.now() - started, ...result });
  } catch (err) {
    sendJson(res, 500, {
      ok: false,
      elapsedMs: Date.now() - started,
      error: stripAnsi(err && err.message ? err.message : String(err))
    });
  }
});

server.listen(PORT, '127.0.0.1', () => {
  console.log('');
  console.log('  INDODANA ingest control panel');
  console.log('  ------------------------------------------------');
  console.log(`  URL         : http://127.0.0.1:${PORT}`);
  console.log(`  Connection  : ${CONNECTION}`);
  console.log(`  Warehouse   : ${WAREHOUSE}`);
  console.log(`  dbt project : ${DBT_PROJECT}`);
  console.log('');
});
