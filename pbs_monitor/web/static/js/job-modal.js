/*
 * Shared Job Detail Modal component for PBS Monitor.
 *
 * Single source of truth for the job-detail popup used by the main dashboard
 * (index.html), the user detail page (user.html) and the project detail page
 * (project.html). Previously this markup + logic was duplicated in all three
 * places and drifted; keep it here only.
 *
 * Usage on a page:
 *   1. Load Vue, then this file:
 *        <script src="js/job-modal.js"></script>
 *   2. Register the component on your app:
 *        const app = Vue.createApp({ ... });
 *        app.component('job-detail-modal', window.JobDetailModal);
 *   3. Place the element once (anywhere in the mounted template) and keep a ref:
 *        <job-detail-modal ref="jobModal"></job-detail-modal>
 *   4. Open it from a row click:
 *        openJob(jid) { this.$refs.jobModal.open(jid); }
 *      (Composition API: const jobModal = ref(null); jobModal.value.open(jid))
 *
 * The component is fully self-contained: it owns its fetch, state, and all
 * formatting helpers (duration/date/score/queue-color) plus the raw-attribute
 * helpers, so a host page needs to provide nothing beyond loading the file.
 * Styling comes from the shared dashboard.css (.modal-*, .raw-pbs* classes).
 */
(function () {
    const { ref } = Vue;

    // Queue color: prefer the shared PBSColors registry (loaded via colors.js on
    // pages that include it) so colors match the rest of the UI; otherwise fall
    // back to a stable hash over the same 10-color palette.
    const QUEUE_COLORS = ['#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4',
                          '#10b981', '#f43f5e', '#eab308', '#14b8a6', '#ec4899'];
    function hashStr(s) { let h = 0; for (let i = 0; i < s.length; i++) { h = ((h << 5) - h) + s.charCodeAt(i); h |= 0; } return Math.abs(h); }
    function queueColor(name) {
        if (window.PBSColors && window.PBSColors.colorFor) return window.PBSColors.colorFor(String(name || ''));
        return QUEUE_COLORS[hashStr(String(name || '')) % QUEUE_COLORS.length];
    }

    function fmtDuration(totalSec) {
        if (totalSec == null) return '\u2014';
        const s = Math.max(0, Math.floor(totalSec));
        const d = Math.floor(s / 86400), h = Math.floor((s % 86400) / 3600), m = Math.floor((s % 3600) / 60);
        if (d > 0) return `${d}d ${h}h ${m}m`;
        if (h > 0) return `${h}h ${m}m`;
        return `${m}m`;
    }
    function fmtIso(isoStr) {
        if (!isoStr) return '\u2014';
        const d = new Date(isoStr);
        if (isNaN(d)) return isoStr;
        return d.toLocaleString(undefined, {
            year: 'numeric', month: 'short', day: 'numeric',
            hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
        });
    }
    function fmtScore(score) {
        if (score == null) return '\u2014';
        if (score >= 1e6) return (score / 1e6).toFixed(2) + 'M';
        if (score >= 1e3) return (score / 1e3).toFixed(1) + 'k';
        return score.toFixed(1);
    }

    // Raw PBS attribute helpers.
    const RAW_LONG_THRESHOLD = 200;   // chars — values this long get their own collapsible
    const RAW_ALWAYS_LONG = ['exec_host', 'exec_vnode', 'Resource_List.select', 'schedselect'];
    function rawKeyCount(raw) { return raw ? Object.keys(raw).length : 0; }
    function rawLongFields(raw) {
        if (!raw) return [];
        const out = [];
        for (const k of Object.keys(raw)) {
            const v = raw[k];
            if (typeof v === 'string' && (v.length > RAW_LONG_THRESHOLD || RAW_ALWAYS_LONG.includes(k))) {
                out.push({ key: k, value: v });
            }
        }
        return out;
    }
    function rawPretty(raw) {
        if (!raw) return '';
        const longKeys = new Set(rawLongFields(raw).map(e => e.key));
        const shown = {};
        for (const k of Object.keys(raw)) {
            shown[k] = longKeys.has(k)
                ? `[${raw[k].length.toLocaleString()} chars \u2014 see below]`
                : raw[k];
        }
        try { return JSON.stringify(shown, null, 2); }
        catch (e) { return String(raw); }
    }

    const TEMPLATE = `
    <div v-if="job" class="modal-backdrop" @click.self="close">
        <div class="modal-panel" role="dialog" aria-modal="true">
            <div class="modal-header">
                <div class="modal-title-group">
                    <span class="modal-job-id">Job {{ job.job_id }}</span>
                    <span class="modal-job-name">{{ job.job_name }}</span>
                    <span class="modal-state-badge" :class="'state-' + job.state">{{ job.state }}</span>
                </div>
                <button class="modal-close" @click="close">\u2715</button>
            </div>

            <div class="modal-body">
                <div v-if="job._loading" class="modal-loading">
                    <div class="spinner"></div>
                    <span>Loading job details\u2026</span>
                </div>

                <div v-else-if="job._error" class="modal-error">
                    \u26a0 Failed to load job details: {{ job._error }}
                </div>

                <template v-else>

                <div class="modal-section">
                    <h3 class="modal-section-title">Ownership</h3>
                    <div class="modal-grid">
                        <div class="modal-field"><span class="mf-label">Owner</span><span class="mf-value">{{ job.owner || '\u2014' }}</span></div>
                        <div class="modal-field"><span class="mf-label">Project</span><span class="mf-value">{{ job.project || '\u2014' }}</span></div>
                        <div class="modal-field"><span class="mf-label">Allocation</span><span class="mf-value">{{ job.allocation_type || '\u2014' }}</span></div>
                        <div class="modal-field"><span class="mf-label">Queue</span><span class="mf-value"><span class="queue-badge" :style="{ backgroundColor: queueColor(job.queue) }">{{ job.queue }}</span></span></div>
                        <div class="modal-field" v-if="job.score != null"><span class="mf-label">Score (WFP)</span><span class="mf-value score-cell">{{ fmtScore(job.score) }}</span></div>
                        <div class="modal-field" v-if="job.priority != null"><span class="mf-label">Priority</span><span class="mf-value">{{ job.priority }}</span></div>
                    </div>
                </div>

                <div class="modal-section">
                    <h3 class="modal-section-title">Resources Requested</h3>
                    <div class="modal-grid">
                        <div class="modal-field"><span class="mf-label">Nodes</span><span class="mf-value">{{ job.nodes || '\u2014' }}</span></div>
                        <div class="modal-field" v-if="job.total_cores"><span class="mf-label">Total CPUs</span><span class="mf-value">{{ job.total_cores }}</span></div>
                        <div class="modal-field" v-if="job.ncpus_requested"><span class="mf-label">NCPUs</span><span class="mf-value">{{ job.ncpus_requested }}</span></div>
                        <div class="modal-field" v-if="job.mpiprocs"><span class="mf-label">MPI Procs</span><span class="mf-value">{{ job.mpiprocs }}</span></div>
                        <div class="modal-field" v-if="job.ompthreads"><span class="mf-label">OMP Threads</span><span class="mf-value">{{ job.ompthreads }}</span></div>
                        <div class="modal-field"><span class="mf-label">Walltime</span><span class="mf-value">{{ job.walltime || '\u2014' }}</span></div>
                        <div class="modal-field" v-if="job.memory_requested"><span class="mf-label">Memory</span><span class="mf-value">{{ job.memory_requested }}</span></div>
                    </div>
                    <div v-if="job.select" class="modal-field-wide"><span class="mf-label">Select</span><span class="mf-value mono">{{ job.select }}</span></div>
                    <div v-if="job.place" class="modal-field-wide"><span class="mf-label">Place</span><span class="mf-value mono">{{ job.place }}</span></div>
                </div>

                <div class="modal-section">
                    <h3 class="modal-section-title">Timing</h3>
                    <div class="modal-grid">
                        <div class="modal-field" v-if="job.submit_time"><span class="mf-label">Submitted</span><span class="mf-value">{{ fmtIso(job.submit_time) }}</span></div>
                        <div class="modal-field" v-if="job.start_time"><span class="mf-label">Started</span><span class="mf-value">{{ fmtIso(job.start_time) }}</span></div>
                        <div class="modal-field" v-if="job.end_time"><span class="mf-label">Ended</span><span class="mf-value">{{ fmtIso(job.end_time) }}</span></div>
                        <div class="modal-field" v-if="job.queue_time_seconds != null"><span class="mf-label">Queue Wait</span><span class="mf-value">{{ fmtDuration(job.queue_time_seconds) }}</span></div>
                        <div class="modal-field" v-if="job.elapsed_seconds != null"><span class="mf-label">Elapsed</span><span class="mf-value">{{ fmtDuration(job.elapsed_seconds) }}</span></div>
                        <div class="modal-field" v-if="job.remaining_seconds != null"><span class="mf-label">Remaining</span><span class="mf-value" :class="{ 'text-danger': job.remaining_seconds <= 0 }">{{ job.remaining_seconds > 0 ? fmtDuration(job.remaining_seconds) : 'Overdue' }}</span></div>
                        <div class="modal-field" v-if="job.actual_runtime_seconds != null"><span class="mf-label">Runtime</span><span class="mf-value">{{ fmtDuration(job.actual_runtime_seconds) }}</span></div>
                        <div class="modal-field" v-if="job.walltime_used"><span class="mf-label">Walltime Used</span><span class="mf-value">{{ job.walltime_used }}</span></div>
                    </div>
                    <div v-if="job.walltime_seconds && (job.elapsed_seconds != null || job.actual_runtime_seconds != null)" class="modal-progress-container">
                        <div class="modal-progress-bar">
                            <div class="modal-progress-fill"
                                 :class="{ 'overdue': job.remaining_seconds != null && job.remaining_seconds <= 0 }"
                                 :style="{ width: Math.min(100, (job.actual_runtime_seconds ?? job.elapsed_seconds) / job.walltime_seconds * 100).toFixed(1) + '%' }"></div>
                        </div>
                        <span class="modal-progress-label">
                            {{ ((job.actual_runtime_seconds ?? job.elapsed_seconds) / job.walltime_seconds * 100).toFixed(1) }}% of walltime
                        </span>
                    </div>
                </div>

                <div class="modal-section" v-if="job.execution_nodes && job.execution_nodes.length">
                    <h3 class="modal-section-title">Execution Nodes ({{ job.execution_node_count }})</h3>
                    <div class="modal-nodes-list">
                        <span v-for="n in job.execution_nodes" :key="n" class="modal-node-tag">{{ n }}</span>
                    </div>
                </div>

                <div class="modal-section" v-if="job.mem_used || job.cpu_used || job.ncpus_used">
                    <h3 class="modal-section-title">Resources Used</h3>
                    <div class="modal-grid">
                        <div class="modal-field" v-if="job.mem_used"><span class="mf-label">Memory</span><span class="mf-value">{{ job.mem_used }}</span></div>
                        <div class="modal-field" v-if="job.vmem_used"><span class="mf-label">Virtual Mem</span><span class="mf-value">{{ job.vmem_used }}</span></div>
                        <div class="modal-field" v-if="job.cpu_used != null"><span class="mf-label">CPU %</span><span class="mf-value">{{ job.cpu_used }}%</span></div>
                        <div class="modal-field" v-if="job.ncpus_used != null"><span class="mf-label">NCPUs Used</span><span class="mf-value">{{ job.ncpus_used }}</span></div>
                    </div>
                </div>

                <div class="modal-section">
                    <h3 class="modal-section-title">PBS Details</h3>
                    <div class="modal-grid">
                        <div class="modal-field"><span class="mf-label">Full Job ID</span><span class="mf-value mono small">{{ job.full_job_id }}</span></div>
                        <div class="modal-field" v-if="job.eligible_time"><span class="mf-label">Eligible Time</span><span class="mf-value">{{ job.eligible_time }}</span></div>
                        <div class="modal-field" v-if="job.exit_status != null"><span class="mf-label">Exit Status</span><span class="mf-value">{{ job.exit_status }}</span></div>
                        <div class="modal-field" v-if="job.array_index != null"><span class="mf-label">Array Index</span><span class="mf-value">{{ job.array_index }}</span></div>
                    </div>
                    <div v-if="job.comment" class="modal-field-wide">
                        <span class="mf-label">Comment</span>
                        <span class="mf-value">{{ job.comment }}</span>
                    </div>
                </div>

                <div class="modal-section" v-if="job.raw_pbs_data">
                    <details class="raw-pbs">
                        <summary class="raw-pbs-summary">
                            <span class="modal-section-title" style="margin:0;">Raw PBS Attributes</span>
                            <span class="raw-pbs-hint">click to expand &middot; {{ rawKeyCount(job.raw_pbs_data) }} fields</span>
                            <button class="raw-pbs-copy" @click.stop.prevent="copyRaw(job.raw_pbs_data)">{{ rawCopyLabel }}</button>
                        </summary>
                        <div class="raw-pbs-body">
                            <pre class="raw-pbs-json mono">{{ rawPretty(job.raw_pbs_data) }}</pre>
                            <details v-for="ent in rawLongFields(job.raw_pbs_data)" :key="ent.key" class="raw-pbs raw-pbs-nested">
                                <summary class="raw-pbs-summary">
                                    <span class="raw-pbs-longkey mono">{{ ent.key }}</span>
                                    <span class="raw-pbs-hint">{{ ent.value.length.toLocaleString() }} chars</span>
                                </summary>
                                <pre class="raw-pbs-json mono wrap">{{ ent.value }}</pre>
                            </details>
                        </div>
                    </details>
                </div>

                </template>
            </div>
        </div>
    </div>`;

    window.JobDetailModal = {
        name: 'JobDetailModal',
        template: TEMPLATE,
        setup() {
            const job = ref(null);          // null = closed; object = open
            const rawCopyLabel = ref('Copy JSON');

            async function open(jid) {
                rawCopyLabel.value = 'Copy JSON';
                job.value = { job_id: jid, _loading: true };
                try {
                    const res = await fetch(`/api/jobs/${encodeURIComponent(jid)}`);
                    if (!res.ok) throw new Error(`HTTP ${res.status}`);
                    job.value = await res.json();
                } catch (e) {
                    console.error('Job detail fetch failed:', e);
                    job.value = { job_id: jid, _error: e.message };
                }
            }
            function close() { job.value = null; }
            function isOpen() { return job.value != null; }

            async function copyRaw(raw) {
                try {
                    await navigator.clipboard.writeText(JSON.stringify(raw, null, 2));
                    rawCopyLabel.value = 'Copied!';
                } catch (e) {
                    rawCopyLabel.value = 'Copy failed';
                }
                setTimeout(() => { rawCopyLabel.value = 'Copy JSON'; }, 1500);
            }

            // Close on Escape while the modal is open.
            function onKeyDown(e) { if (e.key === 'Escape' && job.value) close(); }
            Vue.onMounted(() => window.addEventListener('keydown', onKeyDown));
            Vue.onUnmounted(() => window.removeEventListener('keydown', onKeyDown));

            return {
                job, rawCopyLabel,
                open, close, isOpen, copyRaw,
                queueColor, fmtDuration, fmtIso, fmtScore,
                rawKeyCount, rawLongFields, rawPretty,
            };
        },
    };
})();
