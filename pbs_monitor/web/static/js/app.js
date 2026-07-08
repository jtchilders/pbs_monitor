/**
 * PBS Monitor Dashboard — Vue 3 frontend
 */

// ── constants ────────────────────────────────────────────────────────────────

const STATE_CHAR_LABELS = {
    A:'free', B:'offline', C:'down', D:'busy', E:'job-exclusive',
    F:'job-sharing', G:'reserve', H:'resv-exclusive', I:'down,offline',
    J:'state-unknown,down', K:'state-unknown,down,offline',
    L:'job-exclusive,resv-exclusive', M:'offline,resv-exclusive', N:'unknown',
};
const FALLBACK_COLOR = '#1f2937';

// Down color (gray — distinct from job blues, visible against dark background)
const DOWN_COLOR  = '#6b7280';
const FREE_COLOR  = '#4ade80';
const JOB_COLOR   = '#3b82f6'; // base; individual jobs use JOB_PALETTE

// Per-char render color — all down/offline variants share gray
const STATE_COLORS = {
    A: FREE_COLOR,
    B: DOWN_COLOR, C: DOWN_COLOR, I: DOWN_COLOR,
    J: DOWN_COLOR, K: DOWN_COLOR, M: DOWN_COLOR,
    E: '#3b82f6', F: '#06b6d4', L: '#2563eb',
    G: '#8b5cf6', H: '#a855f7',
    D: '#f59e0b', N: '#374151',
};

// Three mutually exclusive legend states (priority: job > down > free)
// Resv is a separate overlay filter
const LEGEND_STATE_CHARS = {
    free: new Set(['A']),
    job:  new Set(['E','F','L']),
    down: new Set(['B','C','I','J','K','M']),
};
// State chars that include a reservation component
const RESV_CHARS = new Set(['G','H','L','M']);

// Brighten a hex color for the legend-hover highlight
function brightenColor(hex) {
    const n = parseInt(hex.slice(1), 16);
    const r = Math.min(255, ((n >> 16) & 0xff) + 80);
    const g = Math.min(255, ((n >>  8) & 0xff) + 80);
    const b = Math.min(255, ( n        & 0xff) + 80);
    return '#' + [r, g, b].map(v => v.toString(16).padStart(2,'0')).join('');
}

const JOB_PALETTE = [
    '#3b82f6','#2563eb','#1d4ed8','#1e40af','#1e3a8a',
    '#60a5fa','#0ea5e9','#0284c7','#0369a1','#075985',
    '#6366f1','#4f46e5','#4338ca','#3730a3','#312e81',
    '#8b5cf6','#7c3aed','#6d28d9','#5b21b6','#4c1d95',
    '#818cf8','#a78bfa','#c084fc','#38bdf8',
];

const QUEUE_COLORS = [
    '#3b82f6','#f59e0b','#ef4444','#8b5cf6','#06b6d4',
    '#10b981','#f43f5e','#eab308','#14b8a6','#ec4899',
];

// Wait-time bin definitions — MUST stay identical to server BINS in
// pbs_monitor/web/server.py api_analytics_wait_current(). Update both together.
// Each entry: { label, lo (hours, inclusive), hi (hours, exclusive) }.
const WAIT_BINS = [
    { label: '<1hr',   lo:   0, hi:    1 },
    { label: '1-6hr',  lo:   1, hi:    6 },
    { label: '6-12hr', lo:   6, hi:   12 },
    { label: '12-24hr',lo:  12, hi:   24 },
    { label: '1-2d',   lo:  24, hi:   48 },
    { label: '2-7d',   lo:  48, hi:  168 },
    { label: '7-14d',  lo: 168, hi:  336 },
    { label: '2-3wk',  lo: 336, hi:  504 },
    { label: '3-5wk',  lo: 504, hi:  840 },
    { label: '>1mo',   lo: 840, hi: Infinity },
];

// ── helpers ──────────────────────────────────────────────────────────────────

function hashStr(s) {
    let h = 0;
    for (let i = 0; i < s.length; i++) { h = ((h << 5) - h) + s.charCodeAt(i); h |= 0; }
    return Math.abs(h);
}
function jobColor(id) { return JOB_PALETTE[hashStr(String(id)) % JOB_PALETTE.length]; }
function queueColor(name) { return QUEUE_COLORS[hashStr(String(name)) % QUEUE_COLORS.length]; }

function fmtDuration(totalSec) {
    if (totalSec == null) return '--';
    const s = Math.max(0, Math.floor(totalSec));
    const d = Math.floor(s / 86400);
    const h = Math.floor((s % 86400) / 3600);
    const m = Math.floor((s % 3600) / 60);
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m`;
    return `${m}m`;
}

function timeSince(isoStr) {
    if (!isoStr) return '';
    const diff = Math.floor((Date.now() - new Date(isoStr).getTime()) / 1000);
    if (diff < 0) return 'just now';
    if (diff < 60) return `${diff}s ago`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
    return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m ago`;
}

// ── Vue app ──────────────────────────────────────────────────────────────────

const { createApp, ref, reactive, computed, onMounted, onUnmounted, watch, nextTick } = Vue;

createApp({
    setup() {
        const systemInfo   = ref(null);
        const snapshot     = ref(null);
        const loading      = ref(true);
        const error        = ref(null);

        // ── job classification ──
        const hideBlocked    = ref(false);

        // Classify a queued job based on PBS comment and node availability.
        // Only flags jobs blocked by hardware limits, not queue contention.
        // Returns: 'never' | 'misconfigured' | null
        function classifyJob(job, onlineNodes) {
            const comment = (job.comment || '').toLowerCase();

            // PBS explicit "Can Never Run" — trust PBS completely
            if (comment.startsWith('can never run')) return 'never';

            // Requested more nodes than are currently online (offline nodes excluded).
            // This is a hardware limit — job cannot run regardless of queue state.
            if (onlineNodes != null && job.nodes > onlineNodes) return 'misconfigured';

            return null;  // normal — waiting its turn
        }

        const activeTab    = ref('running');
        const sortKey      = ref('nodes');
        const sortDesc     = ref(true);
        const queuedSortKey  = ref('score');
        const queuedSortDesc = ref(true);
        const selectedJobId  = ref(null);
        const hoveredJobId   = ref(null);
        const jobDetail      = ref(null);   // detailed job data for modal
        const jobDetailLoading = ref(false);
        const hoveredLegend  = ref(null);  // 'free'|'job'|'down'|null — mouse hover (no lock)
        const lockedLegend  = ref(null);  // 'free'|'job'|'down'|null — click-to-lock
        const resvFilter    = ref(false); // reservation overlay toggle
        const filterText    = ref('');
        const jobsSection    = ref(null);   // scroll target for drill-down
        const depthGroupBy   = ref('queue');   // 'queue' | 'allocation' | 'project'
        const depthShowHeld  = ref(false);
        const waitShowHeld   = ref(false);

        // ── reservations ──
        const reservations  = ref([]);
        const resvLoading   = ref(false);
        const resvOpen      = ref(true);
        const resvSortKey   = ref('start_time');
        const resvSortDesc  = ref(true);

        const nodeCanvas   = ref(null);
        const mapContainer = ref(null);
        const tooltip = reactive({ visible: false, x: 0, y: 0, nodeName: '', state: '', jobId: '', owner: '', project: '', queue: '' });

        let layout = [];
        let rackLayout = [];  // {x, y, name} for rack labels
        let cellSize = 10;
        let nodeToJobMap = new Map();
        let jobToIndices = new Map();

        // ── derived ──

        const systemName  = computed(() => systemInfo.value?.system_name || 'PBS Monitor');
        const serverHost  = computed(() => systemInfo.value?.server_host  || '');
        const busyNodes = computed(() => {
            const counts = snapshot.value?.state_counts || {};
            return (counts['job-exclusive'] || 0) + (counts['job-sharing'] || 0) + (counts['job-exclusive,resv-exclusive'] || 0);
        });
        const utilization = computed(() => {
            const total = totalComputeNodes.value;
            if (!total) return 0;
            return Math.round(busyNodes.value / total * 100);
        });
        const totalComputeNodes = computed(() => (systemInfo.value?.node_index || []).length);
        const stateCounts = computed(() => snapshot.value?.state_counts || {});

        // Counts for legend labels — exclusive priority: job > down > free
        // Each compute node counted exactly once.
        const legendCounts = computed(() => {
            const stateStr  = snapshot.value?.state_string || '';
            const snapIdxs  = systemInfo.value?.snapshot_indices || [];
            const counts = { free: 0, job: 0, down: 0, resv: 0 };
            for (const si of snapIdxs) {
                const ch = (si != null && si < stateStr.length) ? stateStr[si] : '0';
                if (LEGEND_STATE_CHARS.job.has(ch))       counts.job++;
                else if (LEGEND_STATE_CHARS.down.has(ch)) counts.down++;
                else if (LEGEND_STATE_CHARS.free.has(ch)) counts.free++;
                if (RESV_CHARS.has(ch)) counts.resv++;
            }
            return counts;
        });

        const jobCounts = computed(() => {
            const s = snapshot.value;
            if (!s) return { running: 0, queued: 0, held: 0 };
            return {
                running: s.jobs?.running?.length ?? s.system?.running_jobs ?? 0,
                queued:  s.jobs?.queued?.length ?? s.system?.queued_jobs ?? 0,
                held:    s.system?.held_jobs ?? 0,
            };
        });

        const freshnessClass = computed(() => {
            if (!snapshot.value?.timestamp) return '';
            const age = (Date.now() - new Date(snapshot.value.timestamp).getTime()) / 1000;
            if (age < 300)  return 'green';
            if (age < 900)  return 'yellow';
            return 'red';
        });
        const timeSinceLastUpdate = computed(() => timeSince(snapshot.value?.timestamp));

        function sortedList(list) {
            const key = sortKey.value;
            return [...list].sort((a, b) => {
                let va = a[key] ?? '';
                let vb = b[key] ?? '';
                if (typeof va === 'string') va = va.toLowerCase();
                if (typeof vb === 'string') vb = vb.toLowerCase();
                if (va < vb) return sortDesc.value ? 1 : -1;
                if (va > vb) return sortDesc.value ? -1 : 1;
                return 0;
            });
        }

        const sortedRunningJobs = computed(() => sortedList(snapshot.value?.jobs?.running || []));

        // Parse filter text: supports "field:value" syntax or plain free-text.
        // field aliases: queue, allocation (→allocation_type), project, owner, id
        function parseFilter(raw) {
            const text = raw.trim().toLowerCase();
            if (!text) return null;
            const colonIdx = text.indexOf(':');
            if (colonIdx > 0) {
                const field = text.slice(0, colonIdx).trim();
                const value = text.slice(colonIdx + 1).trim();
                if (value) return { field, value };
            }
            return { field: null, value: text };
        }

        function jobMatchesFilter(j, parsed) {
            if (!parsed) return true;
            const { field, value } = parsed;
            if (!field) {
                // free-text: match any column
                return (
                    (j.owner || '').toLowerCase().includes(value) ||
                    (j.project || '').toLowerCase().includes(value) ||
                    String(j.job_id).includes(value) ||
                    (j.queue || '').toLowerCase().includes(value) ||
                    (j.allocation_type || '').toLowerCase().includes(value)
                );
            }
            // field-specific match
            switch (field) {
                case 'queue':      return (j.queue || '').toLowerCase() === value;
                case 'allocation': return (j.allocation_type || '').toLowerCase() === value;
                case 'project':    return (j.project || '').toLowerCase() === value;
                case 'owner':      return (j.owner || '').toLowerCase() === value;
                case 'id':         return String(j.job_id) === value;
                case 'wait': {
                    // Match job's wait-time bin by recomputing from queue_time_seconds.
                    // Uses WAIT_BINS (same edges as server BINS in server.py).
                    const waitHours = (j.queue_time_seconds || 0) / 3600;
                    const bin = WAIT_BINS.find(b => waitHours >= b.lo && waitHours < b.hi);
                    return bin ? bin.label === value : false;
                }
                default:           return (
                    (j.owner || '').toLowerCase().includes(value) ||
                    (j.project || '').toLowerCase().includes(value) ||
                    String(j.job_id).includes(value) ||
                    (j.queue || '').toLowerCase().includes(value) ||
                    (j.allocation_type || '').toLowerCase().includes(value)
                );
            }
        }

        const filteredRunningJobs = computed(() => {
            const parsed = parseFilter(filterText.value);
            if (!parsed) return sortedRunningJobs.value;
            return sortedRunningJobs.value.filter(j => jobMatchesFilter(j, parsed));
        });

        const sortedQueuedJobs  = computed(() => {
            const list = snapshot.value?.jobs?.queued || [];
            const stateCounts = snapshot.value?.state_counts || {};
            const offlineNodes = (stateCounts['offline'] || 0) + (stateCounts['down'] || 0) +
                                 (stateCounts['down,offline'] || 0) + (stateCounts['state-unknown,down'] || 0) +
                                 (stateCounts['state-unknown,down,offline'] || 0);
            const totalNodes = snapshot.value?.system?.total_nodes ?? null;
            const onlineNodes = totalNodes != null ? totalNodes - offlineNodes : null;
            const key = queuedSortKey.value;
            return [...list]
                .map(j => ({ ...j, _classification: classifyJob(j, onlineNodes), _onlineNodes: onlineNodes }))
                .sort((a, b) => {
                    let va = a[key] ?? '';
                    let vb = b[key] ?? '';
                    // nulls last
                    if (va == null && vb == null) return 0;
                    if (va == null) return 1;
                    if (vb == null) return -1;
                    if (typeof va === 'string') va = va.toLowerCase();
                    if (typeof vb === 'string') vb = vb.toLowerCase();
                    if (va < vb) return queuedSortDesc.value ? 1 : -1;
                    if (va > vb) return queuedSortDesc.value ? -1 : 1;
                    return 0;
                });
        });

        const blockedQueuedCount = computed(() =>
            sortedQueuedJobs.value.filter(j => j._classification != null).length
        );

        const filteredQueuedJobs = computed(() => {
            const parsed = parseFilter(filterText.value);
            let list = sortedQueuedJobs.value;
            if (hideBlocked.value) list = list.filter(j => j._classification == null);
            if (!parsed) return list;
            return list.filter(j => jobMatchesFilter(j, parsed));
        });

        const sortedHeldJobs = computed(() => {
            const list = snapshot.value?.jobs?.held || [];
            return [...list].sort((a, b) => (b.queue_time_seconds ?? 0) - (a.queue_time_seconds ?? 0));
        });

        const filteredHeldJobs = computed(() => {
            const parsed = parseFilter(filterText.value);
            if (!parsed) return sortedHeldJobs.value;
            return sortedHeldJobs.value.filter(j => jobMatchesFilter(j, parsed));
        });

        // ── bar drill-down ──
        // groupBy: 'queue' | 'allocation' | 'project'
        // state:   'running' | 'queued' | 'held'
        // name:    bucket name (e.g. 'large', 'insight')
        function drillDownBar(groupBy, state, name) {
            if (name === 'other') return; // 'other' is an aggregate — can't filter meaningfully
            const fieldMap = { queue: 'queue', allocation: 'allocation', project: 'project' };
            const field = fieldMap[groupBy] || 'queue';
            filterText.value = `${field}:${name}`;
            activeTab.value = state;  // 'running' | 'queued' | 'held'
            // Scroll jobs panel into view
            nextTick(() => {
                if (jobsSection.value) {
                    jobsSection.value.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            });
        }

        // Drill down into a wait-time bin: set wait:<binLabel> filter and switch to
        // the queued tab (or queued, with held tab also filtered via filteredHeldJobs
        // when waitShowHeld is on).
        function drillDownWaitBin(binLabel) {
            filterText.value = `wait:${binLabel}`;
            activeTab.value = 'queued';
            nextTick(() => {
                if (jobsSection.value) {
                    jobsSection.value.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            });
        }

        const DEPTH_THRESHOLD = 100; // node-hours (internal) below which a bucket is collapsed into "other"

        // Compute node-hours from a job object.
        function jobNodeHours(job) {
            const nodes = job.nodes || 1;
            // walltime is "HH:MM:SS" or "H:MM:SS"
            const wt = job.walltime || '';
            const parts = wt.split(':').map(Number);
            let wtSec = 3600;
            if (parts.length === 3) wtSec = parts[0] * 3600 + parts[1] * 60 + parts[2];
            else if (parts.length === 2) wtSec = parts[0] * 3600 + parts[1] * 60;
            return nodes * wtSec / 3600;
        }

        function scaleBuckets(buckets, showHeld) {
            const nNodes = totalComputeNodes.value || 1;
            // Convert node-hours → system-hours for display
            const toSysHours = nh => nh / nNodes;
            const maxTotal = buckets.length > 0
                ? Math.max(...buckets.map(b => b.running + b.queued + (showHeld ? b.held : 0)))
                : 1;
            const denom = maxTotal || 1;
            const maxSysHours = toSysHours(denom);
            const scaled = buckets.map(b => ({
                ...b,
                running: toSysHours(b.running),
                queued:  toSysHours(b.queued),
                held:    toSysHours(b.held),
                runningPctScaled: (b.running / denom * 100).toFixed(1),
                queuedPctScaled:  (b.queued  / denom * 100).toFixed(1),
                heldPctScaled:    (b.held    / denom * 100).toFixed(1),
            }));
            scaled._maxSysHours = maxSysHours;
            return scaled;
        }

        // Generate nice axis tick values for the queue depth bars.
        // Returns [{label, pct}] where pct is the left % position (0–100).
        const depthAxisTicks = computed(() => {
            const buckets = sortedDepthBuckets.value;
            const maxSH = buckets._maxSysHours ?? 0;
            if (!maxSH) return [];
            // Pick a round step that gives 4–5 ticks
            const rawStep = maxSH / 4;
            const magnitude = Math.pow(10, Math.floor(Math.log10(rawStep)));
            const candidates = [1, 2, 2.5, 5, 10].map(f => f * magnitude);
            const step = candidates.find(s => maxSH / s <= 5) || rawStep;
            const ticks = [];
            for (let v = step; v <= maxSH * 1.001; v += step) {
                const rounded = Math.round(v * 1000) / 1000;
                ticks.push({ label: fmtSysHours(rounded), pct: (rounded / maxSH * 100).toFixed(2) });
            }
            return ticks;
        });

        // Build bucket map from a jobs array keyed by a getter fn.
        function accumulateBuckets(map, jobs, keyFn, state) {
            for (const job of jobs) {
                const key = keyFn(job) || '(unknown)';
                if (!map[key]) map[key] = { name: key, running: 0, queued: 0, held: 0 };
                map[key][state] += jobNodeHours(job);
            }
        }

        const sortedDepthBuckets = computed(() => {
            const groupBy   = depthGroupBy.value;
            const showHeld  = depthShowHeld.value;
            let buckets;

            if (groupBy === 'queue') {
                // Use pre-aggregated queue data from server (most accurate — includes all job states)
                const all = [...(snapshot.value?.queues || [])].filter(q => q.total > 0);
                // Compute effective total, hiding held when toggle is off
                const effectiveTotal = q => q.running + q.queued + (showHeld ? q.held : 0);
                const shown = all.filter(q => effectiveTotal(q) >= DEPTH_THRESHOLD || q.name.includes('debug'));
                const other = all.filter(q => effectiveTotal(q) <  DEPTH_THRESHOLD && !q.name.includes('debug'));
                buckets = shown.sort((a, b) => effectiveTotal(b) - effectiveTotal(a));
                if (other.length > 0) {
                    buckets = [...buckets, other.reduce((acc, q) => ({
                        name: 'other',
                        running: acc.running + q.running,
                        queued:  acc.queued  + q.queued,
                        held:    acc.held    + q.held,
                        total:   acc.total   + q.total,
                    }), { name: 'other', running: 0, queued: 0, held: 0, total: 0 })];
                }
            } else {
                // Compute from per-job lists (allocation_type or project)
                const keyFn = groupBy === 'allocation'
                    ? j => j.allocation_type
                    : j => j.project;
                const map = {};
                accumulateBuckets(map, snapshot.value?.jobs?.running || [], keyFn, 'running');
                accumulateBuckets(map, snapshot.value?.jobs?.queued  || [], keyFn, 'queued');
                if (showHeld) {
                    accumulateBuckets(map, snapshot.value?.jobs?.held || [], keyFn, 'held');
                }
                const all = Object.values(map).filter(b => (b.running + b.queued + b.held) > 0);
                const effectiveTotal = b => b.running + b.queued + (showHeld ? b.held : 0);
                const shown = all.filter(b => effectiveTotal(b) >= DEPTH_THRESHOLD);
                const other = all.filter(b => effectiveTotal(b) <  DEPTH_THRESHOLD);
                buckets = shown.sort((a, b) => effectiveTotal(b) - effectiveTotal(a));
                if (other.length > 0) {
                    buckets = [...buckets, other.reduce((acc, b) => ({
                        name: 'other',
                        running: acc.running + b.running,
                        queued:  acc.queued  + b.queued,
                        held:    acc.held    + b.held,
                    }), { name: 'other', running: 0, queued: 0, held: 0 })];
                }
            }

            return scaleBuckets(buckets, showHeld);
        });

        // ── data fetching ──

        async function fetchSystem() {
            const res = await fetch('/api/system');
            if (!res.ok) throw new Error(`System API ${res.status}`);
            systemInfo.value = await res.json();
        }

        async function fetchSnapshot() {
            const res = await fetch('/api/snapshot');
            if (!res.ok) throw new Error(`Snapshot API ${res.status}`);
            snapshot.value = await res.json();
            rebuildIndexes();
        }

        async function fetchData() {
            try {
                if (!systemInfo.value) await fetchSystem();
                await fetchSnapshot();
                loading.value = false;
                error.value = null;
            } catch (e) {
                console.error(e);
                error.value = e.message;
                if (!snapshot.value) loading.value = false;
            }
        }

        function rebuildIndexes() {
            nodeToJobMap = new Map();
            jobToIndices = new Map();
            for (const job of (snapshot.value?.jobs?.running || [])) {
                const indices = job.node_indices || [];
                // Store as Set of positions in our layout (0-based into node_index)
                const layoutPositions = new Set();
                const snapIndices = systemInfo.value?.snapshot_indices || [];
                for (const snapIdx of indices) {
                    // Find which layout position has this snapshot_index
                    const pos = snapIndices.indexOf(snapIdx);
                    if (pos !== -1) {
                        layoutPositions.add(pos);
                        nodeToJobMap.set(pos, job.job_id);
                    }
                }
                jobToIndices.set(job.job_id, layoutPositions);
            }
        }

        // ── canvas layout ──

        function computeLayout() {
            const canvas = nodeCanvas.value;
            const container = mapContainer.value;
            if (!canvas || !container || !systemInfo.value) return;

            const totalNodes = (systemInfo.value.node_index || []).length;
            if (totalNodes === 0) return;

            const containerW = container.clientWidth - 24; // padding
            const MAX_CANVAS_HEIGHT = 500;                  // px — keeps the map compact on large systems
            const topo = systemInfo.value.topology;
            const rackNames = topo?.rack_names || [];
            const nodesPerRack = topo?.nodes_per_rack || [];

            if (rackNames.length > 0) {
                const nRacks = rackNames.length;
                const maxInRack = Math.max(...nodesPerRack);
                const gap = 2;
                const labelHeight = 14;  // space for rack labels below each group
                const rowOverhead = labelHeight + gap * 4; // non-node vertical cost per rack row

                // Find the largest cellSize (1–14) where the map fits within both
                // containerW (width) and MAX_CANVAS_HEIGHT (height) simultaneously.
                // For each candidate, compute the minimum racksPerRow needed to keep
                // height ≤ MAX_CANVAS_HEIGHT, then check the resulting width fits too.
                let bestCell = 1;
                let bestRacksPerRow = nRacks;
                for (let cs = 14; cs >= 1; cs--) {
                    const rackH = maxInRack * (cs + 1) + rowOverhead; // height of one rack row
                    const maxRows = Math.max(1, Math.floor(MAX_CANVAS_HEIGHT / rackH));
                    const rpr = Math.ceil(nRacks / maxRows); // racks per row needed
                    const totalW = rpr * (cs + gap) - gap;   // canvas width that results
                    if (totalW <= containerW) {
                        bestCell = cs;
                        bestRacksPerRow = rpr;
                        break;
                    }
                }
                cellSize = bestCell;
                const racksPerRow = bestRacksPerRow;

                layout = [];
                rackLayout = [];
                let globalIdx = 0;

                for (let ri = 0; ri < nRacks; ri++) {
                    const col = ri % racksPerRow;
                    const row = Math.floor(ri / racksPerRow);
                    const xOff = col * (cellSize + gap);
                    const yOff = row * (maxInRack * (cellSize + 1) + rowOverhead);

                    for (let ni = 0; ni < nodesPerRack[ri]; ni++) {
                        layout.push({ x: xOff, y: yOff + ni * (cellSize + 1), idx: globalIdx });
                        globalIdx++;
                    }

                    // Store rack label position (centered below nodes)
                    const labelY = yOff + nodesPerRack[ri] * (cellSize + 1) + 2;
                    rackLayout.push({ x: xOff + cellSize / 2, y: labelY, name: rackNames[ri] });
                }

                const maxX = Math.max(...layout.map(l => l.x)) + cellSize;
                const maxY = Math.max(...rackLayout.map(r => r.y)) + labelHeight;
                canvas.width = maxX;
                canvas.height = maxY;
                canvas.style.width = maxX + 'px';
                canvas.style.height = maxY + 'px';
            } else {
                const cols = Math.ceil(Math.sqrt(totalNodes * 1.5));
                const rows = Math.ceil(totalNodes / cols);
                // Clamp cellSize to fit both width and MAX_CANVAS_HEIGHT
                const cellFromWidth = Math.floor(containerW / cols);
                const cellFromHeight = Math.floor(MAX_CANVAS_HEIGHT / rows);
                cellSize = Math.max(2, Math.min(12, Math.min(cellFromWidth, cellFromHeight)));
                layout = [];
                for (let i = 0; i < totalNodes; i++) {
                    layout.push({
                        x: (i % cols) * (cellSize + 1),
                        y: Math.floor(i / cols) * (cellSize + 1),
                        idx: i,
                    });
                }
                const w = cols * (cellSize + 1);
                const h = rows * (cellSize + 1);
                canvas.width = w;
                canvas.height = h;
                canvas.style.width = w + 'px';
                canvas.style.height = h + 'px';
            }
        }

        // ── canvas draw ──

        function drawMap() {
            const canvas = nodeCanvas.value;
            if (!canvas || layout.length === 0) return;
            const ctx = canvas.getContext('2d');
            const stateStr = snapshot.value?.state_string || '';
            const snapIndices = systemInfo.value?.snapshot_indices || [];

            ctx.clearRect(0, 0, canvas.width, canvas.height);

            const highlightSet = hoveredJobId.value
                ? jobToIndices.get(hoveredJobId.value)
                : selectedJobId.value
                    ? jobToIndices.get(selectedJobId.value)
                    : null;
            // Active legend: locked takes priority over hovered
            const activeLegend = lockedLegend.value || hoveredLegend.value;
            const legendChars  = activeLegend ? LEGEND_STATE_CHARS[activeLegend] : null;
            const resvOn       = resvFilter.value;

            for (const cell of layout) {
                // Map layout position → snapshot_data index
                const snapIdx = snapIndices[cell.idx];
                const ch = (snapIdx != null) ? (stateStr[snapIdx] || '0') : '0';
                const isResv  = RESV_CHARS.has(ch);

                // Base color
                let color = STATE_COLORS[ch] || FALLBACK_COLOR;
                if ('EFL'.includes(ch)) {
                    const jid = nodeToJobMap.get(cell.idx);
                    if (jid) color = jobColor(jid);
                }

                // Determine visibility under active filters
                const stateMatch = !legendChars || legendChars.has(ch);
                const resvMatch  = !resvOn || isResv;
                const visible    = stateMatch && resvMatch;

                if (legendChars || resvOn) {
                    // Something is active: brighten visible, dim hidden
                    color = visible ? brightenColor(STATE_COLORS[ch] || FALLBACK_COLOR) : '#111827';
                    // Re-apply job palette for brightened job nodes
                    if (visible && 'EFL'.includes(ch)) {
                        const jid = nodeToJobMap.get(cell.idx);
                        if (jid) color = brightenColor(jobColor(jid));
                    }
                }

                ctx.fillStyle = color;
                ctx.fillRect(cell.x, cell.y, cellSize, cellSize);

                if (highlightSet && highlightSet.has(cell.idx)) {
                    ctx.strokeStyle = '#ffffff';
                    ctx.lineWidth = 1.5;
                    ctx.strokeRect(cell.x - 0.5, cell.y - 0.5, cellSize + 1, cellSize + 1);
                }
            }

            // Rack labels intentionally omitted — too small to read at scale
        }

        // ── canvas interaction ──

        function cellAtMouse(e) {
            const canvas = nodeCanvas.value;
            if (!canvas) return null;
            const rect = canvas.getBoundingClientRect();
            const scaleX = canvas.width / rect.width;
            const scaleY = canvas.height / rect.height;
            const mx = (e.clientX - rect.left) * scaleX;
            const my = (e.clientY - rect.top) * scaleY;
            for (const cell of layout) {
                if (mx >= cell.x && mx < cell.x + cellSize &&
                    my >= cell.y && my < cell.y + cellSize) return cell;
            }
            return null;
        }

        function onCanvasMove(e) {
            const cell = cellAtMouse(e);
            if (!cell) { tooltip.visible = false; return; }

            const nodeIndex = systemInfo.value?.node_index || [];
            const snapIndices = systemInfo.value?.snapshot_indices || [];
            const stateStr  = snapshot.value?.state_string || '';
            const snapIdx = snapIndices[cell.idx];
            const ch = (snapIdx != null) ? (stateStr[snapIdx] || '?') : '?';

            tooltip.nodeName = nodeIndex[cell.idx] || `node-${cell.idx}`;
            tooltip.state    = STATE_CHAR_LABELS[ch] || ch;
            tooltip.jobId = ''; tooltip.owner = ''; tooltip.project = ''; tooltip.queue = '';

            const jid = nodeToJobMap.get(cell.idx);
            if (jid) {
                const job = (snapshot.value?.jobs?.running || []).find(j => j.job_id === jid);
                if (job) {
                    tooltip.jobId   = job.job_id;
                    tooltip.owner   = job.owner;
                    tooltip.project = job.project;
                    tooltip.queue   = job.queue;
                }
            }

            const container = mapContainer.value;
            const rect = container.getBoundingClientRect();
            tooltip.x = e.clientX - rect.left + 12;
            tooltip.y = e.clientY - rect.top + 12;
            tooltip.visible = true;
        }

        function onCanvasLeave() { tooltip.visible = false; }

        function onCanvasClick(e) {
            const cell = cellAtMouse(e);
            if (!cell) return;
            const jid = nodeToJobMap.get(cell.idx);
            if (jid) {
                selectedJobId.value = (selectedJobId.value === jid) ? null : jid;
                activeTab.value = 'running';
                requestAnimationFrame(drawMap);
                nextTick(() => {
                    const row = document.getElementById(`job-row-${jid}`);
                    if (row) row.scrollIntoView({ behavior: 'smooth', block: 'center' });
                });
            }
        }

        // ── jobs table ──

        function sortJobs(key) {
            if (sortKey.value === key) sortDesc.value = !sortDesc.value;
            else { sortKey.value = key; sortDesc.value = true; }
        }
        function sortQueuedJobs(key) {
            if (queuedSortKey.value === key) queuedSortDesc.value = !queuedSortDesc.value;
            else { queuedSortKey.value = key; queuedSortDesc.value = true; }
        }
        function fmtScore(score) {
            if (score == null) return '--';
            // Score is a dimensionless WFP priority value; display as plain number
            if (score >= 1e6) return (score / 1e6).toFixed(2) + 'M';
            if (score >= 1e3) return (score / 1e3).toFixed(1) + 'k';
            return score.toFixed(1);
        }
        function fmtSysHours(sh) {
            if (sh == null || sh === 0) return '0';
            if (sh >= 1000) return (sh / 1000).toFixed(1) + 'k';
            if (sh >= 10) return Math.round(sh).toString();
            return sh.toFixed(2);
        }
        function highlightJob(jid) { hoveredJobId.value = jid; requestAnimationFrame(drawMap); }
        function clearHighlight()   { hoveredJobId.value = null; requestAnimationFrame(drawMap); }
        function selectJob(jid)     { selectedJobId.value = (selectedJobId.value === jid) ? null : jid; requestAnimationFrame(drawMap); }

        async function openJobDetail(jid) {
            // Highlight on node map
            selectedJobId.value = jid;
            requestAnimationFrame(drawMap);

            // Show modal immediately with a loading stub
            jobDetailLoading.value = true;
            jobDetail.value = { job_id: jid, _loading: true };

            try {
                const res = await fetch(`/api/jobs/${encodeURIComponent(jid)}`);
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                jobDetail.value = await res.json();
            } catch (e) {
                console.error('Job detail fetch failed:', e);
                jobDetail.value = { job_id: jid, _error: e.message };
            } finally {
                jobDetailLoading.value = false;
            }
        }

        function closeJobDetail() {
            jobDetail.value = null;
        }

        function fmtIso(isoStr) {
            if (!isoStr) return '—';
            const d = new Date(isoStr);
            if (isNaN(d)) return isoStr;
            return d.toLocaleString(undefined, {
                year: 'numeric', month: 'short', day: 'numeric',
                hour: '2-digit', minute: '2-digit', second: '2-digit',
                hour12: false,
            });
        }

        // Close modal on Escape key
        function onKeyDown(e) {
            if (e.key === 'Escape' && jobDetail.value) closeJobDetail();
        }
        function hoverLegend(key) {
            // Only apply hover effect when that state isn't already locked
            if (lockedLegend.value !== key) hoveredLegend.value = key;
            requestAnimationFrame(drawMap);
        }
        function clearLegend() {
            hoveredLegend.value = null;
            requestAnimationFrame(drawMap);
        }
        function clickLegend(key) {
            // Toggle lock: same key → unlock; different key → switch lock
            lockedLegend.value = (lockedLegend.value === key) ? null : key;
            hoveredLegend.value = null;
            requestAnimationFrame(drawMap);
        }
        function toggleResv() {
            resvFilter.value = !resvFilter.value;
            requestAnimationFrame(drawMap);
        }
        function isOverdue(job)     { return job.remaining_seconds <= 0 && job.elapsed_seconds > 0; }

        const tooltipStyle = computed(() => ({ left: tooltip.x + 'px', top: tooltip.y + 'px' }));

        // ── lifecycle ──

        let pollTimer = null;

        function onResize() { computeLayout(); requestAnimationFrame(drawMap); }

        watch(snapshot, () => {
            nextTick(() => { computeLayout(); requestAnimationFrame(drawMap); });
        });

                // ── reservations fetch + sort ──
        async function fetchReservations() {
            resvLoading.value = true;
            try {
                const res = await fetch('/api/reservations');
                if (res.ok) reservations.value = (await res.json()).reservations || [];
            } finally {
                resvLoading.value = false;
            }
        }

        const sortedReservations = computed(() => {
            const list = [...reservations.value];
            return list.sort((a, b) => {
                let va = a[resvSortKey.value] ?? '', vb = b[resvSortKey.value] ?? '';
                if (typeof va === 'string') { va = va.toLowerCase(); vb = vb.toLowerCase(); }
                if (va < vb) return resvSortDesc.value ? 1 : -1;
                if (va > vb) return resvSortDesc.value ? -1 : 1;
                return 0;
            });
        });

        function resvSortBy(key) {
            if (resvSortKey.value === key) resvSortDesc.value = !resvSortDesc.value;
            else { resvSortKey.value = key; resvSortDesc.value = true; }
        }
        function resvSortArrow(key) {
            if (resvSortKey.value !== key) return '';
            return resvSortDesc.value ? ' ▼' : ' ▲';
        }

        // ── wait distribution chart ──

        const waitOpen        = ref(true);

        watch(waitOpen, async (open) => {
            if (open && _waitChart) {
                await nextTick();
                _waitChart.resize();
            }
        });
        const waitDistLoading = ref(false);
        const waitDistEmpty   = ref(false);
        const waitCanvas      = ref(null);
        let _waitChart        = null;

        function _initWaitChart(data) {
            if (!waitCanvas.value) return;
            const ctx = waitCanvas.value.getContext('2d');
            _waitChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: data.bins,
                    datasets: [
                        {
                            label: 'Queued',
                            data: data.queued_counts,
                            backgroundColor: '#f59e0b',  // --accent-busy / .bar-segment.queued
                            borderRadius: 3,
                            stack: 'wait',
                        },
                        {
                            label: 'Held',
                            // Held jobs only appear as orange segments when waitShowHeld is on.
                            // NOTE: held_counts will be all-zeros (from the server) when
                            // include_held was not requested — orange bars only show when present.
                            data: data.held_counts,
                            backgroundColor: '#f97316',  // .bar-segment.held (orange)
                            borderRadius: 3,
                            stack: 'wait',
                        },
                    ]
                },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: true,
                    onClick: (evt, elements) => {
                        if (!elements.length) return;
                        // elements[0].index is the bin index regardless of which
                        // stacked dataset segment was clicked.
                        const idx = elements[0].index;
                        const label = _waitChart.data.labels[idx];
                        drillDownWaitBin(label);
                    },
                    onHover: (evt, elements) => {
                        evt.native.target.style.cursor = elements.length ? 'pointer' : 'default';
                    },
                    plugins: {
                        legend: { display: true },
                        tooltip: {
                            callbacks: {
                                label: ctx => ` ${ctx.dataset.label}: ${ctx.parsed.x} jobs`,
                            },
                        },
                    },
                    scales: {
                        x: { stacked: true, ticks: { color: '#94a3b8' }, grid: { color: '#2d3748' } },
                        y: { stacked: true, ticks: { color: '#94a3b8' }, grid: { color: '#2d3748' } },
                    },
                },
            });
        }

        function _updateWaitChart(data) {
            if (!_waitChart) { _initWaitChart(data); return; }
            _waitChart.data.labels = data.bins;
            _waitChart.data.datasets[0].data = data.queued_counts;
            _waitChart.data.datasets[1].data = data.held_counts;
            _waitChart.update();
        }

        async function fetchWaitDist() {
            if (!_waitChart) waitDistLoading.value = true;
            try {
                const url = '/api/analytics/wait-current' + (waitShowHeld.value ? '?include_held=true' : '');
                const r = await fetch(url);
                if (!r.ok) return;
                const data = await r.json();
                const total = (data.counts || []).reduce((a, b) => a + b, 0);
                waitDistEmpty.value = total === 0;
                if (total > 0) {
                    await nextTick();
                    _updateWaitChart(data);
                }
            } finally {
                waitDistLoading.value = false;
            }
        }

        // Re-fetch immediately when the held toggle changes.
        watch(waitShowHeld, () => fetchWaitDist());

        onMounted(async () => {
            await fetchData();
            fetchReservations();
            fetchWaitDist();
            pollTimer = setInterval(() => { fetchData(); fetchWaitDist(); fetchReservations(); }, 30000);
            window.addEventListener('resize', onResize);
            window.addEventListener('keydown', onKeyDown);
        });

        onUnmounted(() => {
            if (pollTimer) clearInterval(pollTimer);
            window.removeEventListener('resize', onResize);
            window.removeEventListener('keydown', onKeyDown);
        });

        return {
            systemInfo, snapshot, loading, error,
            activeTab, sortKey, sortDesc, queuedSortKey, queuedSortDesc, selectedJobId, hoveredJobId, filterText,
            hideBlocked, blockedQueuedCount,
            depthGroupBy, depthShowHeld, depthAxisTicks,
            nodeCanvas, mapContainer, jobsSection, tooltip, tooltipStyle,
            jobDetail, jobDetailLoading,
            systemName, serverHost, utilization, busyNodes, totalComputeNodes, stateCounts, legendCounts, jobCounts, freshnessClass, timeSinceLastUpdate,
            lockedLegend, resvFilter,
            sortedRunningJobs, sortedQueuedJobs, sortedHeldJobs, filteredRunningJobs, filteredQueuedJobs, filteredHeldJobs, sortedDepthBuckets,
            fetchData, sortJobs, sortQueuedJobs, selectJob, highlightJob, clearHighlight, hoverLegend, clearLegend, clickLegend, toggleResv, isOverdue,
            openJobDetail, closeJobDetail, drillDownBar, drillDownWaitBin,
            onCanvasMove, onCanvasLeave, onCanvasClick,
            fmtDuration, fmtScore, fmtSysHours, fmtIso, queueColor,
            reservations, resvLoading, resvOpen, sortedReservations, resvSortBy, resvSortArrow,
            waitOpen, waitDistLoading, waitDistEmpty, waitCanvas, fetchWaitDist, waitShowHeld,
        };
    }
}).mount('#app');
