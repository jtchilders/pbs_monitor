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

        // ── Tab switching ─────────────────────────────────────────────────
        async function switchTab(tabId) {
            if (activeTab.value === tabId) return;

            // Destroy chart instances on the old tab to avoid canvas leaks
            if (activeTab.value === 'trends') _destroyTrendsCharts();

            activeTab.value = tabId;

            // Fetch data for the newly active tab
            if (tabId === 'trends') {
                await nextTick();
                await fetchTrends();
            }
            // Tabs A-E: no fetch needed — placeholder renders immediately.
            // When tasks A-E are implemented they will add their own fetch
            // calls here (or wire them into this switch statement).
        }

        // ── Global control changes ────────────────────────────────────────
        // Invalidate ALL cached tab data, then reload the currently active tab.
        function invalidateAndReload() {
            // Clear entire tabData cache
            for (const k of Object.keys(tabData)) delete tabData[k];
            // Reload active tab
            if (activeTab.value === 'trends') fetchTrends();
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
            // actions
            setDays, reload, invalidateAndReload,
        };
    }
}).mount('#app');
