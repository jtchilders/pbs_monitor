/**
 * PBS Monitor — Shared color registry (colors.js)
 *
 * Loaded via <script> BEFORE analytics.js and app.js.
 * Exposes window.PBSColors so non-module CDN scripts on both the front page
 * (index.html/app.js) and analytics page (analytics.html/analytics.js) share
 * the same palette and the same color assignments.
 *
 * Invariant: a given queue/project/allocation_type name gets the IDENTICAL
 * color on both pages because both pages call colorFor() from this registry.
 */

(function () {
    'use strict';

    // ── Shared 20-color palette ───────────────────────────────────────────────
    // DO NOT change order — slot assignments are stable across page loads.
    const ANALYTICS_PALETTE = [
        '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4',
        '#10b981', '#f43f5e', '#eab308', '#14b8a6', '#ec4899',
        '#a78bfa', '#fb923c', '#34d399', '#f472b6', '#60a5fa',
        '#fbbf24', '#22d3ee', '#a3e635', '#fda4af', '#c084fc',
    ];

    // ── Stable per-group color registry ──────────────────────────────────────
    // The first chart that encounters a group name claims a palette slot in
    // sorted order; all subsequent charts reuse the same slot.  Because both
    // pages share this registry (via window.PBSColors), a queue named "large"
    // always renders in the same color everywhere.
    const _colorRegistry = {};
    let _nextSlot = 0;

    /**
     * colorFor(groupName) → hex color string.
     *
     * Called before rendering any dataset.  Pre-register all groups in ONE
     * sorted pass (see analytics.js reload()) so colors are deterministic
     * regardless of which API response arrives first.
     *
     * @param {string} groupName  - Queue/project/allocation_type name.
     * @returns {string}          - Hex color string from ANALYTICS_PALETTE.
     */
    function colorFor(groupName) {
        if (!(groupName in _colorRegistry)) {
            _colorRegistry[groupName] = _nextSlot++;
        }
        return ANALYTICS_PALETTE[_colorRegistry[groupName] % ANALYTICS_PALETTE.length];
    }

    // ── Semantic outcome-class colors ─────────────────────────────────────────
    // Used by the Job Outcomes tab (tasks A) and exit-taxonomy bar.
    // Semantic mapping: green = good, red/amber = bad, grey = neutral/unknown.
    // DO NOT introduce a second set of outcome colors anywhere.
    const OUTCOME_COLORS = {
        success:        '#10b981',  // green
        signal_killed:  '#ef4444',  // red
        walltime_killed:'#f59e0b',  // amber
        requeued:       '#8b5cf6',  // purple (often benign — system requeue)
        could_not_run:  '#6b7280',  // grey   (PBS special, e.g. -29)
        error:          '#f43f5e',  // rose   (non-zero, non-signal user errors)
        other:          '#94a3b8',  // muted  (unclassified / unknown)
        unknown:        '#94a3b8',  // alias
    };

    // ── Public API ────────────────────────────────────────────────────────────
    window.PBSColors = {
        ANALYTICS_PALETTE,
        colorFor,
        OUTCOME_COLORS,
    };
}());
