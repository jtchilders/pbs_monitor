/**
 * PBS Monitor — Analytics page (Vue 3 + Chart.js)
 *
 * Shared color utilities come from js/colors.js (loaded before this file).
 * Access via window.PBSColors.colorFor() and window.PBSColors.OUTCOME_COLORS.
 */

// ── Pull shared colors from the registry loaded by colors.js ──────────────
// These are module-level aliases so the rest of the file reads cleanly.
// Do NOT re-declare ANALYTICS_PALETTE or colorFor here — use the shared ones.
const ANALYTICS_PALETTE = window.PBSColors.ANALYTICS_PALETTE;
const colorFor          = (name) => window.PBSColors.colorFor(name);
const OUTCOME_COLORS    = window.PBSColors.OUTCOME_COLORS;

// ── Bin label formatter (unchanged from prior version) ────────────────────
function fmtBin(iso, freq) {
    // 'h' → 'MM-DD HH:00'  'd' → 'YYYY-MM-DD'  'w' → 'YYYY-MM-DD' (week of)
    const d = new Date(iso);
    if (isNaN(d)) return iso;
    const pad = n => String(n).padStart(2, '0');
    if (freq === 'h') return `${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:00`;
    return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`;
}

// ── Reservation-queue collapse (unchanged) ────────────────────────────────
function collapseResvGroups(series, groups) {
    const resvRe = /^[RMrm]\d+$/;
    const collapsed = {};
    const newGroups = [];
    let hasResv = false;
    for (const g of groups) {
        if (resvRe.test(g)) {
            hasResv = true;
            if (!collapsed['Reservations']) collapsed['Reservations'] = null;
        } else {
            collapsed[g] = series[g];
            newGroups.push(g);
        }
    }
    if (hasResv) {
        const resvGroups = groups.filter(g => resvRe.test(g));
        const len = (series[resvGroups[0]] || []).length;
        const summed = new Array(len).fill(0);
        for (const g of resvGroups) {
            const vals = series[g] || [];
            for (let i = 0; i < len; i++) summed[i] += (vals[i] || 0);
        }
        collapsed['Reservations'] = summed.map(v => Math.round(v * 100) / 100);
        newGroups.push('Reservations');
    }
    return { series: collapsed, groups: newGroups.sort() };
}

// ── Small-group filter (unchanged) ────────────────────────────────────────
const DEPTH_MIN_NODE_HOURS = 0;  // show all queues (no minimum threshold)

function filterSmallGroups(series, groups, minTotal) {
    const kept = groups.filter(g => {
        const vals = series[g] || [];
        return vals.reduce((a, b) => a + b, 0) >= minTotal;
    });
    const filtered = {};
    for (const g of kept) filtered[g] = series[g];
    return { series: filtered, groups: kept };
}

// ── Common Chart.js line options (unchanged) ──────────────────────────────
function _commonLineOpts(yLabel, stacked) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: 'index', intersect: false },
        plugins: {
            legend: { labels: { color: '#e0e0e0', boxWidth: 12 }, position: 'bottom' },
            tooltip: {
                mode: 'index',
                intersect: false,
                itemSort: (a, b) => b.parsed.y - a.parsed.y,
                callbacks: {
                    beforeBody: () => [],
                    afterBody: (items) => {
                        const total = items.reduce((s, it) => s + (it.parsed.y || 0), 0);
                        const hidden = Math.max(0, items.length - 5);
                        return [
                            `Total: ${total.toFixed(2)}`,
                            ...(hidden > 0 ? [`(+${hidden} more not shown)`] : []),
                        ];
                    },
                    label: (item) => {
                        const sorted = item.chart.tooltip.dataPoints
                            .slice()
                            .sort((a, b) => b.parsed.y - a.parsed.y);
                        const rank = sorted.findIndex(d => d.datasetIndex === item.datasetIndex);
                        if (rank >= 5) return null;
                        return ` ${item.dataset.label}: ${item.parsed.y.toFixed(2)}`;
                    },
                },
            },
        },
        scales: {
            x: { ticks: { color: '#94a3b8', maxRotation: 45, autoSkip: true, maxTicksLimit: 20 },
                 grid: { color: '#2d3748' },
                 stacked: stacked || false },
            y: { ticks: { color: '#94a3b8' }, grid: { color: '#2d3748' },
                 title: { display: true, text: yLabel, color: '#94a3b8' },
                 stacked: stacked || false },
        },
    };
}

// ── Vue app ───────────────────────────────────────────────────────────────

const { createApp, ref, reactive, computed, onMounted, nextTick } = Vue;

createApp({
    setup() {

        // ── Tab definitions (order = display order) ───────────────────────
        // id values match the v-if conditions in analytics.html.
        const tabDefs = [
            { id: 'trends',           label: 'Trends' },
            { id: 'job-outcomes',     label: 'Job Outcomes' },
            { id: 'walltime-accuracy',label: 'Walltime Accuracy' },
            { id: 'wait-times',       label: 'Wait Times' },
            { id: 'reservations',     label: 'Reservations' },
            { id: 'collector-health', label: 'Collector Health' },
        ];

        // ── Global state ──────────────────────────────────────────────────
        const systemName   = ref('PBS Monitor');
        const days         = ref(30);
        const freqOverride = ref('auto');   // 'auto' | 'h' | 'd' | 'w'
        const groupBy      = ref('queue');  // 'queue' | 'allocation_type' | 'project'
        const loading      = ref(false);
        const error        = ref(null);
        const lastRefresh  = ref(null);
        const collectBanner = ref(null);   // completeness banner text (null = no banner)

        const utilMeta     = ref('');
        const depthMeta    = ref('');

        const activeTab    = ref('trends'); // starts on Trends

        const freqChoices = [
            { k: 'auto', l: 'Auto' },
            { k: 'h',    l: 'Hour' },
            { k: 'd',    l: 'Day' },
            { k: 'w',    l: 'Week' },
        ];

        const effectiveFreq = computed(() => {
            if (freqOverride.value !== 'auto') return freqOverride.value;
            if (days.value <= 7)  return 'h';
            if (days.value < 90)  return 'd';
            return 'w';
        });
        const effectiveFreqLabel = computed(() =>
            ({ h: 'Hour', d: 'Day', w: 'Week' })[effectiveFreq.value]);

        // ── Lazy-loading cache (plan §4.2) ────────────────────────────────
        // Keyed by `tabId + '|' + buildParams()`.  On tab switch: if key is in
        // cache, re-render from memory; else fetch.  Any global control change
        // clears the whole cache and reloads the current tab.
        const tabData = reactive({});

        // ── Filter state ──────────────────────────────────────────────────
        const filterDims = [
            { key: 'queue',           label: 'Queue' },
            { key: 'owner',           label: 'Owner' },
            { key: 'project',         label: 'Project' },
            { key: 'allocation_type', label: 'Alloc Type' },
        ];
        const filterOptions    = reactive({ queue: [], owner: [], project: [], allocation_type: [] });
        const filterSelections = reactive({ queue: [], owner: [], project: [], allocation_type: [] });
        const filterMode       = reactive({ queue: 'include', owner: 'include', project: 'include', allocation_type: 'include' });
        const filterSearch     = reactive({ queue: '', owner: '', project: '', allocation_type: '' });
        const openFilterPanel  = ref(null);

        function toggleFilterPanel(key) {
            openFilterPanel.value = (openFilterPanel.value === key) ? null : key;
        }
        function activeFilterCount(key) { return filterSelections[key].length; }
        function filteredOptions(key) {
            const s = (filterSearch[key] || '').toLowerCase();
            const opts = filterOptions[key] || [];
            if (!s) return opts;
            return opts.filter(v => v.toLowerCase().includes(s));
        }
        function clearFilter(key) {
            filterSelections[key] = [];
            filterMode[key] = 'include';
        }
        function applyFilters() {
            openFilterPanel.value = null;
            invalidateAndReload();
        }

        function _outsideClick(e) {
            if (!e.target.closest('.filter-dropdown')) openFilterPanel.value = null;
        }

        // ── Loading indicators ────────────────────────────────────────────
        const loadingUtil  = ref(false);
        const loadingDepth = ref(false);
        const loadingAny   = computed(() => loadingUtil.value || loadingDepth.value);

        // ── Chart canvas refs & instances ─────────────────────────────────
        const utilCanvas  = ref(null);
        const depthCanvas = ref(null);
        let _utilChart    = null;
        let _depthChart   = null;

        // Destroy both Trends chart instances (called on tab switch away from Trends
        // to avoid canvas-leak warnings from Chart.js).
        function _destroyTrendsCharts() {
            if (_utilChart)  { _utilChart.destroy();  _utilChart  = null; }
            if (_depthChart) { _depthChart.destroy(); _depthChart = null; }
        }

        // ── Query-string builder ──────────────────────────────────────────
        function buildParams(extra = {}) {
            const p = new URLSearchParams();
            p.set('days', days.value);
            if (freqOverride.value !== 'auto') p.set('freq', freqOverride.value);
            p.set('group_by', groupBy.value);
            for (const dim of filterDims) {
                const sel = filterSelections[dim.key];
                if (!sel || sel.length === 0) continue;
                const paramKey = filterMode[dim.key] === 'exclude'
                    ? `${dim.key}_exclude`
                    : dim.key;
                for (const v of sel) p.append(paramKey, v);
            }
            for (const [k, v] of Object.entries(extra)) p.set(k, v);
            return p.toString();
        }

        // Cache key = tabId + '|' + current params string
        function cacheKey(tabId) {
            return `${tabId}|${buildParams()}`;
        }

        // ── Fetchers ──────────────────────────────────────────────────────
        async function fetchFilters() {
            try {
                const r = await fetch(`/api/analytics/filters?days=${days.value}`);
                if (!r.ok) return;
                const data = await r.json();
                filterOptions.queue           = data.queues           || [];
                filterOptions.owner           = data.owners           || [];
                filterOptions.project         = data.projects         || [];
                filterOptions.allocation_type = data.allocation_types || [];
            } catch (e) { console.warn('filters fetch:', e); }
        }

        async function fetchSystemName() {
            try {
                const r = await fetch('/api/system');
                if (r.ok) {
                    const d = await r.json();
                    systemName.value = d.system_name || 'PBS Monitor';
                }
            } catch {}
        }

        // Completeness banner: lightweight summary call (plan §5.6 / §4.2).
        // The endpoint does not exist yet (task E adds it); 404 is silently
        // swallowed so the banner simply stays hidden.
        async function fetchCollectorBanner() {
            collectBanner.value = null;
            try {
                const r = await fetch(`/api/analytics/collector-health?days=${days.value}&summary=1`);
                if (!r.ok) return; // 404 expected until task E — silently skip
                const d = await r.json();
                if (d.gap_count > 0 || d.failed_count > 0) {
                    collectBanner.value =
                        `${d.gap_count} collection gap(s) detected in this range` +
                        (d.max_gap_min ? ` (max gap: ${d.max_gap_min} min)` : '') +
                        '. Collector Health tab has details.';
                }
            } catch {
                // collector-health endpoint not yet deployed — no banner
            }
        }

        // ── Trends-tab data fetch & render ────────────────────────────────
        async function fetchTrends() {
            const key = cacheKey('trends');

            // Cache hit: re-render from stored data (no network needed)
            if (tabData[key]) {
                await nextTick();
                _renderTrendsFromCache(tabData[key]);
                return;
            }

            loadingUtil.value  = true;
            loadingDepth.value = true;
            loading.value      = true;
            error.value        = null;

            try {
                const qs = buildParams();
                const [uRes, dRes] = await Promise.all([
                    fetch(`/api/analytics/utilization?${qs}`),
                    fetch(`/api/analytics/queue-depth?${qs}`),
                ]);
                if (!uRes.ok || !dRes.ok) throw new Error('API error');
                const uData = await uRes.json();
                const dData = await dRes.json();

                // Collapse reservation groups
                const uCollapsed = collapseResvGroups(uData.series, uData.groups);
                uData.series = uCollapsed.series; uData.groups = uCollapsed.groups;
                const dCollapsed = collapseResvGroups(dData.series, dData.groups);
                dData.series = dCollapsed.series; dData.groups = dCollapsed.groups;

                // Filter depth: drop negligible queues
                const dFiltered = filterSmallGroups(dData.series, dData.groups, DEPTH_MIN_NODE_HOURS);
                dData.series = dFiltered.series; dData.groups = dFiltered.groups;

                // Pre-register all groups in a single sorted pass so colors
                // are deterministic regardless of response order.
                const allGroups = [...new Set([...uData.groups, ...dData.groups])].sort();
                allGroups.forEach(g => colorFor(g));

                // Store in cache before rendering (cache survives tab switches)
                tabData[key] = { uData, dData };

                await nextTick();
                _renderTrendsFromCache(tabData[key]);
                lastRefresh.value = new Date().toLocaleTimeString();
            } catch (e) {
                console.error(e);
                error.value = `Failed to load Trends: ${e.message}`;
            } finally {
                loading.value      = false;
                loadingUtil.value  = false;
                loadingDepth.value = false;
            }
        }

        function _renderTrendsFromCache({ uData, dData }) {
            renderLineChart('util',  uData, '%',            '/ capacity',    true);
            renderLineChart('depth', dData, 'system-hours', 'queued backlog', true);
            utilMeta.value  = `${uData.groups.length} group(s) · ${uData.bins.length} bins · ${uData.total_nodes} compute nodes`;
            depthMeta.value = `${dData.groups.length} group(s) · ${dData.bins.length} bins · normalized to ${dData.total_nodes} nodes`;
            lastRefresh.value = new Date().toLocaleTimeString();
        }

        // ── Task A: Job Outcomes tab ──────────────────────────────────────────
        // State refs
        const loadingOutcomes    = ref(false);
        const loadingTaxonomy    = ref(false);
        const outcomesMeta       = ref('');
        const taxonomyMeta       = ref('');
        const outcomesLegendNote = ref('');
        const outcomeMode        = ref('count');   // 'count' | 'rate'

        // Canvas refs
        const jobOutcomesCanvas  = ref(null);
        const exitTaxonomyCanvas = ref(null);

        // Chart instances (module-level so we can destroy them)
        let _jobOutcomesChart  = null;
        let _exitTaxonomyChart = null;

        // Ordered display list: system/requeue classes first (purple/grey),
        // then user-fault classes (red, amber, rose), then success (green), unknown last.
        // This visual ordering separates "system/requeue" from "user-fault" per plan §9.
        const OUTCOME_ORDER = [
            'requeued',       // purple  — often benign: preemption/maintenance
            'could_not_run',  // grey    — PBS special (e.g. -29): often benign
            'walltime_killed',// amber   — user may have underestimated
            'signal_killed',  // red     — killed by signal
            'error',          // rose    — non-zero user errors
            'success',        // green   — good
            'unknown',        // muted
        ];
        const LEGEND_NOTE = '📋 Left group (purple/grey): system/requeue events — often benign (preemption, maintenance). ' +
                            'Right group (amber/red/rose): user-fault outcomes.';

        function _destroyJobOutcomesCharts() {
            if (_jobOutcomesChart)  { _jobOutcomesChart.destroy();  _jobOutcomesChart  = null; }
            if (_exitTaxonomyChart) { _exitTaxonomyChart.destroy(); _exitTaxonomyChart = null; }
        }

        async function fetchJobOutcomes() {
            const key = cacheKey('job-outcomes');

            // Cache hit: re-render from stored data (no network needed)
            if (tabData[key]) {
                await nextTick();
                renderOutcomesFromCache();
                return;
            }

            loadingOutcomes.value = true;
            loadingTaxonomy.value = true;
            error.value           = null;

            try {
                const qs = buildParams();
                const [oRes, tRes] = await Promise.all([
                    fetch(`/api/analytics/job-outcomes?${qs}`),
                    fetch(`/api/analytics/exit-taxonomy?${qs}`),
                ]);
                if (!oRes.ok || !tRes.ok) throw new Error('API error fetching Job Outcomes');
                const oData = await oRes.json();
                const tData = await tRes.json();

                // Store in cache
                tabData[key] = { oData, tData };

                await nextTick();
                _renderOutcomesCharts(oData, tData);
                lastRefresh.value = new Date().toLocaleTimeString();
            } catch (e) {
                console.error(e);
                error.value = `Failed to load Job Outcomes: ${e.message}`;
            } finally {
                loadingOutcomes.value = false;
                loadingTaxonomy.value = false;
            }
        }

        // Called by the Count/Rate toggle (no re-fetch needed — uses cached data)
        function renderOutcomesFromCache() {
            const key = cacheKey('job-outcomes');
            const cached = tabData[key];
            if (!cached) {
                fetchJobOutcomes();
                return;
            }
            _renderOutcomesCharts(cached.oData, cached.tData);
        }

        function _renderOutcomesCharts(oData, tData) {
            _renderOutcomesTimeSeries(oData);
            _renderExitTaxonomyBar(tData);
        }

        function _renderOutcomesTimeSeries(oData) {
            const canvas = jobOutcomesCanvas.value;
            if (!canvas) return;
            if (_jobOutcomesChart) { _jobOutcomesChart.destroy(); _jobOutcomesChart = null; }

            const OC = window.PBSColors.OUTCOME_COLORS;
            const freq   = oData.freq;
            const labels = (oData.bins || []).map(b => fmtBin(b, freq));
            const classes = oData.classes || [];
            const useRate = outcomeMode.value === 'rate';

            // Order classes per OUTCOME_ORDER; append any unknown extras at end
            const ordered = [
                ...OUTCOME_ORDER.filter(c => classes.includes(c)),
                ...classes.filter(c => !OUTCOME_ORDER.includes(c)).sort(),
            ];

            const seriesKey = useRate ? 'series_rate' : 'series';
            const yLabel    = useRate ? '% of bin total' : 'job count';

            const datasets = ordered.map(cls => {
                const color = OC[cls] || OC.unknown;
                return {
                    label:            cls.replace(/_/g, ' '),
                    data:             (oData[seriesKey] || {})[cls] || [],
                    borderColor:      color,
                    backgroundColor:  color + 'cc',
                    borderWidth:      1.5,
                    pointRadius:      0,
                    pointHoverRadius: 4,
                    tension:          0.2,
                    fill:             'origin',
                };
            });

            const total = oData.total || 0;
            const binCount = (oData.bins || []).length;
            outcomesMeta.value = `${classes.length} outcome class(es) · ${binCount} bins · ${total.toLocaleString()} jobs`;
            outcomesLegendNote.value = LEGEND_NOTE;

            _jobOutcomesChart = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: { labels, datasets },
                options: _commonLineOpts(yLabel, true),
            });
        }

        function _renderExitTaxonomyBar(tData) {
            const canvas = exitTaxonomyCanvas.value;
            if (!canvas) return;
            if (_exitTaxonomyChart) { _exitTaxonomyChart.destroy(); _exitTaxonomyChart = null; }

            const OC = window.PBSColors.OUTCOME_COLORS;
            const codes = tData.codes || [];

            const labels = codes.map(c => {
                // Truncate long labels for readability
                const lbl = c.label || String(c.code);
                return lbl.length > 35 ? lbl.slice(0, 33) + '…' : lbl;
            });
            const counts = codes.map(c => c.count);
            const colors = codes.map(c => (OC[c.outcome_class] || OC.unknown) + 'cc');
            const borders = codes.map(c => OC[c.outcome_class] || OC.unknown);

            const total = tData.total || 0;
            taxonomyMeta.value = `${Object.keys(tData.classes || {}).length} class(es) · ${total.toLocaleString()} finished jobs in window`;

            _exitTaxonomyChart = new Chart(canvas.getContext('2d'), {
                type: 'bar',
                data: {
                    labels,
                    datasets: [{
                        label: 'Jobs',
                        data: counts,
                        backgroundColor: colors,
                        borderColor: borders,
                        borderWidth: 1,
                    }],
                },
                options: {
                    indexAxis: 'y',   // horizontal bar for long labels
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label(item) {
                                    const pct = total > 0
                                        ? (item.parsed.x / total * 100).toFixed(1)
                                        : '0.0';
                                    return ` ${item.parsed.x.toLocaleString()} jobs (${pct}%)`;
                                },
                                afterLabel(item) {
                                    const code = codes[item.dataIndex];
                                    return code ? ` class: ${code.outcome_class}` : '';
                                },
                            },
                        },
                    },
                    scales: {
                        x: {
                            ticks:  { color: '#94a3b8' },
                            grid:   { color: '#2d3748' },
                            title:  { display: true, text: 'job count', color: '#94a3b8' },
                        },
                        y: {
                            ticks:  { color: '#94a3b8', autoSkip: false },
                            grid:   { color: '#2d3748' },
                        },
                    },
                },
            });
        }
        // ── End Task A ────────────────────────────────────────────────────────

        // ── Task E: Collector Health tab ─────────────────────────────────────
        let _collectorGapChart = null;

        const loadingCollectorHealth = ref(false);
        const collectorHealthError   = ref(null);
        const collectorHealthData    = ref(null);
        const collectorHealthMeta    = ref('');
        const collectorGapCanvas     = ref(null);

        async function fetchCollectorHealth() {
            const key = cacheKey('collector-health');

            // Cache hit — re-render from memory
            if (tabData[key]) {
                await nextTick();
                _renderCollectorHealth(tabData[key]);
                return;
            }

            loadingCollectorHealth.value = true;
            collectorHealthError.value   = null;

            try {
                const qs = `days=${days.value}`;
                const r  = await fetch(`/api/analytics/collector-health?${qs}`);
                if (!r.ok) throw new Error(`HTTP ${r.status}`);
                const data = await r.json();

                tabData[key] = data;
                await nextTick();
                _renderCollectorHealth(data);
            } catch (e) {
                console.error('collector-health fetch:', e);
                collectorHealthError.value = `Failed to load Collector Health: ${e.message}`;
            } finally {
                loadingCollectorHealth.value = false;
            }
        }

        async function _renderCollectorHealth(data) {
            collectorHealthData.value = data;

            const snapCount  = (data.cadence || []).length;
            const gapCount   = (data.gaps    || []).length;
            collectorHealthMeta.value =
                `${snapCount.toLocaleString()} intervals · ` +
                `${gapCount} significant gap(s) · ` +
                `median cadence ${data.median_gap_min ?? '—'} min`;

            // Ensure the data-gated section/canvas is laid out before drawing.
            await nextTick();
            const canvas = collectorGapCanvas.value;
            if (!canvas) return;

            // Destroy old chart instance to avoid canvas-reuse errors
            if (_collectorGapChart) { _collectorGapChart.destroy(); _collectorGapChart = null; }

            const cadence = data.cadence || [];
            if (cadence.length === 0) return;

            // Build labels and values; color each point red if gap > 60 min
            const NORMAL_COLOR = '#38bdf8';   // sky-400 — distinct from queue palette
            const GAP_COLOR    = '#ef4444';   // red-500
            const THRESHOLD    = 60;          // minutes — flagging threshold for UI

            const labels = cadence.map(c => {
                const d = new Date(c.t);
                if (isNaN(d)) return c.t;
                const pad = n => String(n).padStart(2, '0');
                return `${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
            });
            const values       = cadence.map(c => c.gap_min);
            const pointColors  = values.map(v => v > THRESHOLD ? GAP_COLOR : NORMAL_COLOR);
            const pointRadii   = values.map(v => v > THRESHOLD ? 5 : 2);

            _collectorGapChart = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: {
                    labels,
                    datasets: [{
                        label: 'Gap (min)',
                        data: values,
                        borderColor: NORMAL_COLOR,
                        backgroundColor: 'transparent',
                        borderWidth: 1,
                        pointBackgroundColor: pointColors,
                        pointBorderColor:     pointColors,
                        pointRadius:          pointRadii,
                        pointHoverRadius:     6,
                        stepped: 'before',   // step chart — shows cadence shifts clearly
                        tension: 0,
                        fill: false,
                    }],
                },
                options: {
                    responsive: true,
                    interaction: { mode: 'index', intersect: false },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label: ctx => `Gap: ${ctx.parsed.y.toFixed(1)} min` +
                                              (ctx.parsed.y > THRESHOLD ? ' ⚠ ABOVE THRESHOLD' : ''),
                            },
                        },
                    },
                    scales: {
                        x: {
                            ticks: { color: '#94a3b8', maxTicksLimit: 12, maxRotation: 30 },
                            grid:  { color: '#2d3748' },
                        },
                        y: {
                            title: { display: true, text: 'Gap (minutes)', color: '#94a3b8' },
                            ticks: { color: '#94a3b8' },
                            grid:  { color: '#2d3748' },
                            min: 0,
                        },
                    },
                },
            });
        }

        function _destroyCollectorHealthChart() {
            if (_collectorGapChart) { _collectorGapChart.destroy(); _collectorGapChart = null; }
            collectorHealthData.value  = null;
            collectorHealthMeta.value  = '';
            collectorHealthError.value = null;
        }
        // ── End Task E ───────────────────────────────────────────────────────

        // ── Task C: Wait Times ────────────────────────────────────────────
        const loadingWaitTimes    = ref(false);
        const waitTimesError      = ref('');
        const waitEcdfCanvas      = ref(null);
        const waitPercentilesCanvas = ref(null);
        // Which percentile to show on the percentiles chart ('p50'|'p90'|'p99')
        const waitPctView         = ref('p90');
        let _waitEcdfChart        = null;
        let _waitPercentilesChart = null;

        function _destroyWaitTimesCharts() {
            if (_waitEcdfChart)        { _waitEcdfChart.destroy();        _waitEcdfChart        = null; }
            if (_waitPercentilesChart) { _waitPercentilesChart.destroy(); _waitPercentilesChart = null; }
        }

        async function fetchWaitTimes() {
            const key = `wait-times|${buildParams()}`;
            if (tabData[key]) {
                await nextTick();
                _renderWaitTimesFromCache(tabData[key]);
                return;
            }
            loadingWaitTimes.value = true;
            waitTimesError.value   = '';
            _destroyWaitTimesCharts();

            try {
                const params = buildParams();
                const [ecdfResp, pctResp] = await Promise.all([
                    fetch(`/api/analytics/wait-ecdf?${params}`),
                    fetch(`/api/analytics/wait-percentiles?${params}`),
                ]);
                if (!ecdfResp.ok)  throw new Error(`ECDF fetch failed: ${ecdfResp.status}`);
                if (!pctResp.ok)   throw new Error(`Percentiles fetch failed: ${pctResp.status}`);
                const ecdfData = await ecdfResp.json();
                const pctData  = await pctResp.json();
                tabData[key] = { ecdfData, pctData };
                await nextTick();
                _renderWaitTimesFromCache({ ecdfData, pctData });
            } catch (err) {
                waitTimesError.value = String(err);
            } finally {
                loadingWaitTimes.value = false;
            }
        }

        function _renderWaitTimesFromCache({ ecdfData, pctData }) {
            _renderWaitEcdf(ecdfData);
            _renderWaitPercentiles(pctData, waitPctView.value);
        }

        function _renderWaitEcdf(data) {
            const canvas = waitEcdfCanvas.value;
            if (!canvas || !data) return;
            if (_waitEcdfChart) { _waitEcdfChart.destroy(); _waitEcdfChart = null; }

            const groups  = data.groups || [];
            const curves  = data.curves || {};
            const darkTick = '#94a3b8';
            const darkGrid = '#2d3748';

            const datasets = groups.map(grp => ({
                label:           grp,
                data:            (curves[grp] || []).map(([x, y]) => ({ x, y })),
                borderColor:     colorFor(grp),
                backgroundColor: 'transparent',
                borderWidth:     1.5,
                pointRadius:     0,
                pointHoverRadius: 3,
                tension:         0.2,
            }));

            _waitEcdfChart = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: { datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    parsing: false,
                    plugins: {
                        legend: { display: true, labels: { color: darkTick, boxWidth: 12 } },
                        tooltip: {
                            callbacks: {
                                label: ctx => {
                                    const d = ctx.parsed;
                                    return `${ctx.dataset.label}: wait=${d.x.toFixed(2)}h, cdf=${(d.y * 100).toFixed(1)}%`;
                                },
                            },
                        },
                    },
                    scales: {
                        x: {
                            type: 'linear',
                            title: { display: true, text: 'Wait time (hours)', color: darkTick },
                            ticks: { color: darkTick },
                            grid:  { color: darkGrid },
                        },
                        y: {
                            min: 0, max: 1,
                            title: { display: true, text: 'Cumulative fraction', color: darkTick },
                            ticks: { color: darkTick, callback: v => (v * 100).toFixed(0) + '%' },
                            grid:  { color: darkGrid },
                        },
                    },
                },
            });
        }

        function _renderWaitPercentiles(data, view) {
            const canvas = waitPercentilesCanvas.value;
            if (!canvas || !data) return;
            if (_waitPercentilesChart) { _waitPercentilesChart.destroy(); _waitPercentilesChart = null; }

            const freq    = data.freq;
            const labels  = (data.bins || []).map(b => fmtBin(b, freq));
            const groups  = data.groups || [];
            const series  = data.series || {};
            const darkTick = '#94a3b8';
            const darkGrid = '#2d3748';

            const datasets = groups.map(grp => ({
                label:           grp,
                data:            (series[grp] || {})[view] || [],
                borderColor:     colorFor(grp),
                backgroundColor: 'transparent',
                borderWidth:     1.5,
                pointRadius:     0,
                pointHoverRadius: 3,
                tension:         0.2,
            }));

            _waitPercentilesChart = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: true, labels: { color: darkTick, boxWidth: 12 } },
                        tooltip: {
                            mode: 'index',
                            intersect: false,
                            callbacks: {
                                label: ctx => `${ctx.dataset.label}: ${(ctx.parsed.y / 3600).toFixed(2)}h`,
                            },
                        },
                    },
                    scales: {
                        x: { ticks: { color: darkTick }, grid: { color: darkGrid } },
                        y: {
                            title: { display: true, text: 'Wait time (seconds)', color: darkTick },
                            ticks: { color: darkTick },
                            grid:  { color: darkGrid },
                        },
                    },
                },
            });
        }

        function switchWaitPctView(view) {
            waitPctView.value = view;
            // Re-render the percentiles chart from cached data only
            const key = `wait-times|${buildParams()}`;
            if (tabData[key]) {
                _renderWaitPercentiles(tabData[key].pctData, view);
            }
        }
        // ── End Task C ────────────────────────────────────────────────────

        // ── Task D: Reservations ──────────────────────────────────────────
        const loadingReservations = ref(false);
        const reservationsError   = ref('');
        const resvTimelineCanvas  = ref(null);
        const resvRankingTable    = ref(null);
        const reservationsSummary = ref(null);   // summary stats for header chips
        const reservationsData    = ref(null);   // full response for the table
        let _resvTimelineChart    = null;

        function _destroyReservationsCharts() {
            if (_resvTimelineChart) { _resvTimelineChart.destroy(); _resvTimelineChart = null; }
            reservationsSummary.value = null;
            reservationsData.value    = null;
            reservationsError.value   = '';
        }

        async function fetchReservations() {
            const key = `reservations|days=${days.value}`;
            if (tabData[key]) {
                await nextTick();
                _renderReservationsFromCache(tabData[key]);
                return;
            }
            loadingReservations.value = true;
            reservationsError.value   = '';
            _destroyReservationsCharts();

            try {
                const r = await fetch(`/api/analytics/reservation-utilization-timeline?days=${days.value}`);
                if (!r.ok) throw new Error(`Reservations fetch failed: ${r.status}`);
                const data = await r.json();
                tabData[key] = data;
                await nextTick();
                _renderReservationsFromCache(data);
            } catch (err) {
                reservationsError.value = String(err);
            } finally {
                loadingReservations.value = false;
            }
        }

        async function _renderReservationsFromCache(data) {
            reservationsSummary.value = data;
            reservationsData.value    = data;
            // Wait for the summary-gated section (and its properly-sized canvas)
            // to actually be in the DOM before building the chart — otherwise
            // Chart.js grabs a 300x150 default-size canvas and draws nothing.
            await nextTick();
            _renderResvTimeline(data);
        }

        function _renderResvTimeline(data) {
            const canvas = resvTimelineCanvas.value;
            if (!canvas || !data) return;
            if (_resvTimelineChart) { _resvTimelineChart.destroy(); _resvTimelineChart = null; }

            const reservations = data.reservations || [];
            if (reservations.length === 0) return;

            const darkTick = '#94a3b8';
            const darkGrid = '#2d3748';

            // Horizontal bar chart: reserved vs used node-hours per reservation
            const labels = reservations.map(r => r.name || r.reservation_id);
            const reserved = reservations.map(r => r.reserved_node_hours);
            const used     = reservations.map(r => r.used_node_hours);

            _resvTimelineChart = new Chart(canvas.getContext('2d'), {
                type: 'bar',
                data: {
                    labels,
                    datasets: [
                        {
                            label: 'Reserved node-hours',
                            data:  reserved,
                            backgroundColor: 'rgba(94,129,172,0.5)',
                            borderColor:     '#5e81ac',
                            borderWidth: 1,
                        },
                        {
                            label: 'Used node-hours',
                            data:  used,
                            backgroundColor: 'rgba(16,185,129,0.6)',
                            borderColor:     '#10b981',
                            borderWidth: 1,
                        },
                    ],
                },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: true, labels: { color: darkTick, boxWidth: 12 } },
                        tooltip: {
                            callbacks: {
                                afterBody: (ctxArr) => {
                                    const idx = ctxArr[0]?.dataIndex;
                                    if (idx == null) return '';
                                    const r = reservations[idx];
                                    return [`Utilization: ${r.utilization_pct}%`, `Owner: ${r.owner}`];
                                },
                            },
                        },
                    },
                    scales: {
                        x: {
                            title: { display: true, text: 'Node-hours', color: darkTick },
                            ticks: { color: darkTick },
                            grid:  { color: darkGrid },
                        },
                        y: { ticks: { color: darkTick }, grid: { color: darkGrid } },
                    },
                },
            });
        }
        // ── End Task D ────────────────────────────────────────────────────

        // ── Tab switching ─────────────────────────────────────────────────
        async function switchTab(tabId) {
            if (activeTab.value === tabId) return;

            // Destroy chart instances on the old tab to avoid canvas leaks
            if (activeTab.value === 'trends') _destroyTrendsCharts();
            if (activeTab.value === 'job-outcomes') _destroyJobOutcomesCharts();
            if (activeTab.value === 'walltime-accuracy') _destroyWalltimeCharts();
            if (activeTab.value === 'collector-health') _destroyCollectorHealthChart();
            if (activeTab.value === 'wait-times') _destroyWaitTimesCharts();
            if (activeTab.value === 'reservations') _destroyReservationsCharts();

            activeTab.value = tabId;

            // Fetch data for the newly active tab
            if (tabId === 'trends') {
                await nextTick();
                await fetchTrends();
            }
            if (tabId === 'job-outcomes') {
                await nextTick();
                await fetchJobOutcomes();
            }
            if (tabId === 'walltime-accuracy') {
                await nextTick();
                await fetchWalltimeAccuracy();
            }
            if (tabId === 'collector-health') {
                await nextTick();
                await fetchCollectorHealth();
            }
            if (tabId === 'wait-times') {
                await nextTick();
                await fetchWaitTimes();
            }
            if (tabId === 'reservations') {
                await nextTick();
                await fetchReservations();
            }
            // Tabs C, D: no fetch needed — placeholder renders immediately.
        }

        // ── Global control changes ────────────────────────────────────────
        // Invalidate ALL cached tab data, then reload the currently active tab.
        function invalidateAndReload() {
            // Clear entire tabData cache
            for (const k of Object.keys(tabData)) delete tabData[k];
            // Reload active tab
            if (activeTab.value === 'trends') fetchTrends();
            if (activeTab.value === 'job-outcomes') fetchJobOutcomes();
            if (activeTab.value === 'walltime-accuracy') fetchWalltimeAccuracy();
            if (activeTab.value === 'collector-health') fetchCollectorHealth();
            if (activeTab.value === 'wait-times') fetchWaitTimes();
            if (activeTab.value === 'reservations') fetchReservations();
            // Other tabs are placeholders — nothing to reload yet.
        }

        function setDays(d) {
            days.value = d;
            // Refresh filter options for the new window, then invalidate + reload
            fetchFilters().then(() => {
                invalidateAndReload();
                fetchCollectorBanner();
            });
        }

        // ── Convenience alias (used by Bin/Group buttons) ─────────────────
        // Note: analytics.html calls invalidateAndReload() directly via @click.
        function reload() { invalidateAndReload(); }

        // ── Chart renderers ───────────────────────────────────────────────
        // ── Task B: Walltime Accuracy ─────────────────────────────────────
        const loadingWalltime  = ref(false);
        const errorWalltime    = ref('');
        const walltimeStats    = ref(null);   // summary stats chips
        const walltimeScorecard = ref(null);  // scorecard table data
        const walltimeHistCanvas = ref(null); // canvas ref
        let _walltimeHistChart = null;

        function _destroyWalltimeCharts() {
            if (_walltimeHistChart) { _walltimeHistChart.destroy(); _walltimeHistChart = null; }
        }

        async function fetchWalltimeAccuracy() {
            const key = `walltime-accuracy|${buildParams()}`;
            if (tabData[key]) {
                await nextTick();
                _renderWalltimeFromCache(tabData[key]);
                return;
            }

            loadingWalltime.value = true;
            errorWalltime.value   = '';
            walltimeStats.value   = null;
            walltimeScorecard.value = null;
            _destroyWalltimeCharts();

            try {
                const params = buildParams();
                // Fetch histogram + scorecard in parallel
                const [histResp, scorecardResp] = await Promise.all([
                    fetch(`/api/analytics/walltime-histogram?${params}`),
                    fetch(`/api/analytics/walltime-efficiency?days=${days.value}&group_by=${groupBy.value === 'project' ? 'project' : 'user'}`),
                ]);
                if (!histResp.ok) throw new Error(`Histogram fetch failed: ${histResp.status}`);
                const histData = await histResp.json();
                const scorecardData = scorecardResp.ok ? await scorecardResp.json() : null;

                tabData[key] = { histData, scorecardData };
                await nextTick();
                _renderWalltimeFromCache({ histData, scorecardData });
            } catch (err) {
                errorWalltime.value = String(err);
            } finally {
                loadingWalltime.value = false;
            }
        }

        function _renderWalltimeFromCache({ histData, scorecardData }) {
            // Update reactive state for chips and scorecard table
            walltimeStats.value    = histData;
            walltimeScorecard.value = scorecardData;

            // Render the matrix heatmap
            const canvas = walltimeHistCanvas.value;
            if (!canvas) return;
            _destroyWalltimeCharts();

            const xLabels = histData.x_labels || [];
            const yLabels = histData.y_labels || [];
            const cells   = histData.cells   || [];

            // Max count for color scaling
            const maxCount = cells.reduce((m, c) => Math.max(m, c.count), 1);

            // Use PBSColors blue base (index 0) for single-hue sequential ramp
            // palette[0] is the first analytics color — derive rgba from it
            const baseColor = window.PBSColors.ANALYTICS_PALETTE[0]; // e.g. '#4e79a7'
            function hexToRGB(hex) {
                const r = parseInt(hex.slice(1,3), 16);
                const g = parseInt(hex.slice(3,5), 16);
                const b = parseInt(hex.slice(5,7), 16);
                return [r, g, b];
            }
            const [br, bg, bb] = hexToRGB(baseColor);

            // y-axis index 5 = "95-100%" row = walltime-kill risk → amber border
            const RISK_Y = 5;

            const datasets = [{
                label: 'Job count',
                data: cells.map(c => ({ x: c.x, y: c.y, v: c.count })),
                backgroundColor(ctx) {
                    const v = ctx.dataset.data[ctx.dataIndex]?.v ?? 0;
                    if (v <= 0) return 'rgba(0,0,0,0)';
                    // sqrt (perceptual) ramp with a visible floor so sparse
                    // cells don't vanish into the dark background — a linear
                    // ramp makes low-count cells (alpha≈0.08) invisible.
                    const alpha = 0.18 + 0.80 * Math.sqrt(v / maxCount);
                    return `rgba(${br},${bg},${bb},${alpha.toFixed(3)})`;
                },
                borderColor(ctx) {
                    const d = ctx.dataset.data[ctx.dataIndex];
                    if (!d) return 'transparent';
                    // Amber border for near-limit risk row
                    if (d.y === RISK_Y) return '#f59e0b';
                    // Light border for diagonal (xi === yi loosely)
                    if (d.x === d.y) return `rgba(${br},${bg},${bb},0.6)`;
                    return `rgba(${br},${bg},${bb},0.15)`;
                },
                borderWidth(ctx) {
                    const d = ctx.dataset.data[ctx.dataIndex];
                    if (!d) return 0;
                    return (d.y === RISK_Y || d.x === d.y) ? 2 : 0.5;
                },
                width(ctx) {
                    // Derive cell width from the x-scale's pixel spacing, not
                    // chart.chartArea (which is undefined on first paint and
                    // yields 0-size, invisible cells). One category = 1 unit on
                    // the linear axis.
                    const x = ctx.chart.scales.x;
                    if (!x) return 0;
                    return Math.max(0, (x.getPixelForValue(1) - x.getPixelForValue(0)) - 2);
                },
                height(ctx) {
                    const y = ctx.chart.scales.y;
                    if (!y) return 0;
                    return Math.max(0, Math.abs(y.getPixelForValue(1) - y.getPixelForValue(0)) - 2);
                },
            }];

            const darkTick  = '#94a3b8';
            const darkGrid  = '#2d3748';

            _walltimeHistChart = new Chart(canvas.getContext('2d'), {
                type: 'matrix',
                data: { datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                title()  { return ''; },
                                label(ctx) {
                                    const d = ctx.dataset.data[ctx.dataIndex];
                                    return [
                                        `Requested: ${xLabels[d.x] || d.x}`,
                                        `Used:      ${yLabels[d.y] || d.y}`,
                                        `Jobs:      ${d.v.toLocaleString()}`,
                                    ];
                                },
                            },
                        },
                    },
                    scales: {
                        x: {
                            type: 'linear',
                            min: -0.5,
                            max: xLabels.length - 0.5,
                            ticks: {
                                color: darkTick,
                                stepSize: 1,
                                callback: (v) => xLabels[Math.round(v)] ?? '',
                            },
                            grid: { color: darkGrid },
                            title: { display: true, text: 'Requested Walltime', color: darkTick },
                        },
                        y: {
                            type: 'linear',
                            min: -0.5,
                            max: yLabels.length - 0.5,
                            ticks: {
                                color: darkTick,
                                stepSize: 1,
                                callback: (v) => yLabels[Math.round(v)] ?? '',
                            },
                            grid: { color: darkGrid },
                            title: { display: true, text: 'Used Fraction', color: darkTick },
                        },
                    },
                },
            });
        }

        // ── Trends chart renderer ─────────────────────────────────────────
        function renderLineChart(which, data, yUnit, subtitle, stacked) {
            const freq    = data.freq;
            const labels  = (data.bins || []).map(b => fmtBin(b, freq));
            const sorted  = [...(data.groups || [])].sort();
            const datasets = sorted.map(grp => ({
                label:           grp,
                data:            data.series[grp] || [],
                borderColor:     colorFor(grp),
                backgroundColor: colorFor(grp) + '99',
                borderWidth:     1.5,
                pointRadius:     0,
                pointHoverRadius: 4,
                tension:         0.2,
                fill:            stacked ? 'origin' : false,
            }));

            const canvas = which === 'util' ? utilCanvas.value : depthCanvas.value;
            if (!canvas) return;

            // Destroy existing chart before creating a new one
            const existing = which === 'util' ? _utilChart : _depthChart;
            if (existing) existing.destroy();

            const chart = new Chart(canvas.getContext('2d'), {
                type: 'line',
                data: { labels, datasets },
                options: _commonLineOpts(yUnit, stacked),
            });
            if (which === 'util')  _utilChart  = chart;
            else                   _depthChart = chart;
        }

        // ── Lifecycle ─────────────────────────────────────────────────────
        onMounted(async () => {
            document.addEventListener('click', _outsideClick);
            await fetchSystemName();
            await fetchFilters();
            // Fetch Trends (default tab) + completeness banner in parallel
            await Promise.all([
                fetchTrends(),
                fetchCollectorBanner(),
            ]);
        });

        return {
            // state
            systemName, days, freqOverride, groupBy,
            loading, loadingAny, error, lastRefresh, collectBanner,
            utilMeta, depthMeta,
            freqChoices, effectiveFreq, effectiveFreqLabel,
            // tabs
            tabDefs, activeTab, switchTab,
            // filters
            filterDims, filterOptions, filterSelections, filterMode, filterSearch,
            openFilterPanel,
            toggleFilterPanel, activeFilterCount, filteredOptions, clearFilter, applyFilters,
            // canvas refs
            utilCanvas, depthCanvas,
            // Task A: Job Outcomes
            jobOutcomesCanvas, exitTaxonomyCanvas,
            loadingOutcomes, loadingTaxonomy,
            outcomesMeta, taxonomyMeta, outcomesLegendNote,
            outcomeMode, renderOutcomesFromCache,
            // Task B: walltime accuracy
            walltimeHistCanvas, loadingWalltime, errorWalltime, walltimeStats, walltimeScorecard,
            // Task E: collector health
            collectorGapCanvas, loadingCollectorHealth, collectorHealthError,
            collectorHealthData, collectorHealthMeta,
            // Task C: wait times
            waitEcdfCanvas, waitPercentilesCanvas,
            loadingWaitTimes, waitTimesError, waitPctView, switchWaitPctView,
            // Task D: reservations
            resvTimelineCanvas, resvRankingTable,
            loadingReservations, reservationsError,
            reservationsSummary, reservationsData,
            // actions
            setDays, reload, invalidateAndReload,
        };
    }
}).mount('#app');
