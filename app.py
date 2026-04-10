# =====================================================
# FILE: app.py
# STREAMLIT UI — SCG VRP Optimizer
# Version: 3.2 (Fixed progress log, summary boxes,
#                route summary table, warnings below)
# =====================================================
import streamlit as st
import time
import io
import pandas as pd
from datetime import datetime
from collections import defaultdict

# =====================================================
# SECTION 1: PAGE CONFIG
# =====================================================
st.set_page_config(
    page_title="SCG VRP Optimizer",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =====================================================
# SECTION 2: IMPORTS
# =====================================================
from vrp_core import (
    run_pipeline, VRPProgress, VRPResult,
    ALNSConfig, DEFAULT_PRIORITY_CONFIG
)
from vrp_output import (
    generate_summary_report,
    generate_map,
    generate_convergence_data,
    generate_utilization_chart,
    generate_distance_chart,
    generate_convergence_chart,
    generate_temperature_chart,
    create_download_zip,
)

# =====================================================
# SECTION 3: CUSTOM CSS
# =====================================================
def apply_custom_css():
    st.markdown("""
    <style>
        .main-header {
            font-size: 2.2rem;
            font-weight: 700;
            color: #1e3a5f;
            text-align: center;
            padding: 0.5rem 0;
        }
        .sub-header {
            font-size: 1.0rem;
            color: #666;
            text-align: center;
            margin-bottom: 1.5rem;
        }
        /* ── Summary Metric Boxes ── */
        .metric-container {
            display: flex;
            gap: 12px;
            flex-wrap: wrap;
            margin: 0.8rem 0;
        }
        .metric-box {
            flex: 1;
            min-width: 200px;
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 10px;
            padding: 14px 18px;
            text-align: center;
        }
        .metric-box .metric-label {
            font-size: 0.78rem;
            color: #6c757d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }
        .metric-box .metric-value {
            font-size: 1.3rem;
            font-weight: 700;
            color: #1e3a5f;
        }
        .metric-box .metric-sub {
            font-size: 0.75rem;
            color: #888;
            margin-top: 2px;
        }
        /* ── Hard Constraint Box ── */
        .hard-ok {
            background: #d4edda;
            border-left: 4px solid #28a745;
            padding: 10px 14px;
            border-radius: 6px;
            margin: 6px 0;
        }
        .hard-fail {
            background: #f8d7da;
            border-left: 4px solid #dc3545;
            padding: 10px 14px;
            border-radius: 6px;
            margin: 6px 0;
        }
        /* ── Progress Log ── */
        .progress-log-wrapper {
            position: relative;
        }
        .progress-log {
            font-family: 'Courier New', monospace;
            font-size: 0.80rem;
            line-height: 1.35;
            background: #1e1e1e;
            color: #d4d4d4;
            padding: 1rem;
            border-radius: 8px;
            max-height: 420px;
            overflow-y: auto;
            white-space: pre-wrap;
            scroll-behavior: smooth;
        }
        .scroll-btn {
            position: sticky;
            float: right;
            bottom: 8px;
            right: 8px;
            margin-top: -40px;
            background: #4472C4;
            color: white;
            border: none;
            border-radius: 50%;
            width: 32px;
            height: 32px;
            font-size: 16px;
            cursor: pointer;
            z-index: 10;
            box-shadow: 0 2px 6px rgba(0,0,0,0.4);
            opacity: 0.85;
            transition: opacity 0.2s;
        }
        .scroll-btn:hover {
            opacity: 1;
            background: #365fa0;
        }
    </style>
    """, unsafe_allow_html=True)


# =====================================================
# SECTION 4: PROGRESS CALLBACK — FIX: header ไม่ซ้ำ
# =====================================================


# =====================================================
# SECTION 4: PROGRESS CALLBACK
# =====================================================
class StreamlitProgressLogger:
    """
    Progress adapter — สะสม log แบบ st.code()
    ✅ ไม่ใช้ components.html() → ปุ่มไม่พัง
    ✅ auto-scroll ควบคุมได้
    ✅ copy log ได้
    """

    def __init__(self, container, progress_bar, status_text):
        self.container = container
        self.progress_bar = progress_bar
        self.status_text = status_text
        self.log_lines = []
        self.log_placeholder = container.empty()
        self.vrp_progress = VRPProgress()
        self.vrp_progress.set_callback(self._on_progress)
        self._data_summary_shown = False
        self._alns_header_shown = False

    def _on_progress(self, phase: str, message: str,
                     percentage: int, details: dict):
        # ── Overall progress ──
        phase_weights = {
            'validate': (0, 5),
            'load': (5, 20),
            'construct': (20, 40),
            'alns': (40, 95),
            'post': (95, 98),
            'done': (98, 100),
        }
        start_pct, end_pct = phase_weights.get(phase, (0, 100))
        overall_pct = start_pct + (percentage / 100) * (end_pct - start_pct)
        overall_pct = min(100, max(0, int(overall_pct)))

        self.progress_bar.progress(overall_pct / 100)
        self.status_text.markdown(f"**⏳ {message}**")

        details = details or {}

        # ── DATA LOADING SUMMARY ──
        if phase == 'load' and percentage == 100:
            if not self._data_summary_shown:
                self._data_summary_shown = True
                filename = details.get('filename', '')
                depots = details.get('depots', 0)
                vehicles = details.get('vehicles', 0)
                orders = details.get('orders', 0)
                dist_entries = details.get('dist_matrix_entries', 0)
                priority_levels = details.get('priority_levels', 0)
                raw_lines = details.get('raw_order_lines', 0)
                load_time = details.get('load_time', 0)

                self.log_lines.append(f"📁 ไฟล์: {filename}")
                self.log_lines.append("")
                self.log_lines.append("📊 DATA LOADING SUMMARY")
                self.log_lines.append("=" * 50)
                self.log_lines.append(f"   🏭 Depots:           {depots}")
                self.log_lines.append(f"   🚛 Vehicles:         {vehicles}")
                self.log_lines.append(f"   📦 Orders:           {orders}")
                self.log_lines.append(f"   📏 Distance Matrix:  {dist_entries} entries")
                self.log_lines.append(f"   🎯 Priority Levels:  {priority_levels}")
                self.log_lines.append(f"   📋 Raw Order Lines:  {raw_lines}")
                self.log_lines.append(f"   ⏱️  Loading Time:     {load_time:.2f}s")
                self.log_lines.append("")
                self._render_log()

        # ── CONSTRUCTIVE ──
        if phase == 'construct' and percentage == 100:
            self.log_lines.append("🏗️ Constructive solution built")
            self.log_lines.append("")
            self._render_log()

        # ── ALNS PROGRESS ──
        if phase == 'alns' and 'iteration' in details:
            iteration = details.get('iteration', 0)
            best_cost = details.get('best_cost', 0)
            current_cost = details.get('current_cost', 0)
            num_vehicles = details.get('num_vehicles', 0)
            no_imp = details.get('no_improvement', 0)
            elapsed = details.get('elapsed', 0)

            if not self._alns_header_shown:
                self._alns_header_shown = True
                self.log_lines.append("🔄 ALNS Optimization:")
                self.log_lines.append(
                    f"   {'Iter':>6} | {'Time':>6} | "
                    f"{'Best':>10} | {'Current':>10} | "
                    f"{'V':>2} | {'No-imp':>6}"
                )
                self.log_lines.append("   " + "─" * 58)

            is_best = (details.get('score', 0) >= 15)
            marker = " 🌟" if is_best else ""
            self.log_lines.append(
                f"   {iteration:>6} | {elapsed:>5.1f}s | "
                f"{best_cost:>10,.0f} | {current_cost:>10,.0f} | "
                f"{num_vehicles:>2} | {no_imp:>6}{marker}"
            )
            self._render_log()

        # ── REHEAT ──
        if phase == 'alns' and '🔥' in message:
            self.log_lines.append(f"   {message}")
            self._render_log()

        # ── DONE ──
        if phase == 'done':
            self.log_lines.append("")
            self.log_lines.append(f"✅ {message}")
            self._render_log()

    def _render_log(self):
        """
        ✅ ใช้ st.code() แทน components.html()
        - ไม่มี iframe → copy ได้
        - ไม่มี auto-scroll → ไม่ดันลง
        - Streamlit native → ปุ่มไม่พัง
        """
        log_text = "\n".join(self.log_lines)
        self.log_placeholder.code(log_text, language=None)

    def get_vrp_progress(self) -> VRPProgress:
        return self.vrp_progress

    def clear(self):
        """Clear all progress UI elements"""
        try:
            self.log_placeholder.empty()
        except Exception:
            pass
        try:
            self.progress_bar.empty()
        except Exception:
            pass
        try:
            self.status_text.empty()
        except Exception:
            pass


# =====================================================
# SECTION 5: SIDEBAR
# =====================================================
def render_sidebar():
    with st.sidebar:
        st.markdown("## 🚛 SCG VRP Optimizer")
        st.markdown("---")

        st.markdown("### 📁 อัปโหลดข้อมูล")
        uploaded_file = st.file_uploader(
            "เลือกไฟล์ Excel (.xlsx)",
            type=['xlsx'],
            help="ไฟล์ Excel ที่มีข้อมูลออเดอร์, รถ, ระยะทาง, จุดส่ง, Depot"
        )

        if uploaded_file:
            st.success(f"📄 {uploaded_file.name}")
            file_size = uploaded_file.size / 1024
            st.caption(f"ขนาด: {file_size:.1f} KB")

        st.markdown("---")

        st.markdown("### ⚙️ ตั้งค่า ALNS")

        with st.expander("🔧 พารามิเตอร์", expanded=False):
            time_limit = st.slider(
                "Time Limit (วินาที)",
                min_value=30, max_value=600,
                value=ALNSConfig.TIME_LIMIT_SECONDS,
                step=30,
            )
            no_imp_limit = st.slider(
                "No-Improvement Limit",
                min_value=100, max_value=2000,
                value=ALNSConfig.NO_IMPROVEMENT_LIMIT,
                step=100,
            )
            initial_temp = st.slider(
                "Initial Temperature",
                min_value=500, max_value=5000,
                value=int(ALNSConfig.INITIAL_TEMPERATURE),
                step=500,
            )
            reheat_enabled = st.checkbox(
                "เปิด Reheating",
                value=ALNSConfig.REHEAT_ENABLED,
            )

        st.markdown("---")

        run_clicked = st.button(
            "🚀 เริ่มจัดเส้นทาง",
            type="primary",
            use_container_width=True,
            disabled=(uploaded_file is None)
        )

        if uploaded_file is None:
            st.info("📌 กรุณาอัปโหลดไฟล์ Excel ก่อน")

        st.markdown("---")
        st.markdown(
            "<small>SCG VRP Optimizer v3.2<br>"
            "ALNS + Multi-Depot + Priority</small>",
            unsafe_allow_html=True
        )

    return {
        'uploaded_file': uploaded_file,
        'run_clicked': run_clicked,
        'time_limit': time_limit if uploaded_file else ALNSConfig.TIME_LIMIT_SECONDS,
        'no_imp_limit': no_imp_limit if uploaded_file else ALNSConfig.NO_IMPROVEMENT_LIMIT,
        'initial_temp': initial_temp if uploaded_file else int(ALNSConfig.INITIAL_TEMPERATURE),
        'reheat_enabled': reheat_enabled if uploaded_file else ALNSConfig.REHEAT_ENABLED,
    }


# =====================================================
# SECTION 6: SUMMARY METRIC BOXES (ใหม่ — กล่องสวย)
# =====================================================
def render_summary_boxes(report, result):
    """แสดง Summary เป็นกล่อง metric 4 ช่อง"""
    ov = report['overview']

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label">📏 ระยะทางรวม</div>
            <div class="metric-value">{ov['total_distance']:,.1f} km</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        multi = ov.get('multi_depot_routes', 0)
        single = ov.get('single_depot_routes', 0)
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label">🚛 จำนวนรถ</div>
            <div class="metric-value">{ov['num_vehicles']} คัน</div>
            <div class="metric-sub">Single: {single} | Multi: {multi}</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        rate = ov['assignment_rate']
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label">📦 ออเดอร์</div>
            <div class="metric-value">{ov['assigned_orders']}/{ov['total_orders']}</div>
            <div class="metric-sub">{rate:.0f}% จัดได้</div>
        </div>
        """, unsafe_allow_html=True)

    with col4:
        op_stats = result.operator_stats or {}
        iterations = op_stats.get('iterations', 0)
        best_iter = op_stats.get('best_iteration', 0)
        elapsed = result.elapsed_time
        st.markdown(f"""
        <div class="metric-box">
            <div class="metric-label">⏱️ Runtime</div>
            <div class="metric-value">{elapsed:.1f}s</div>
            <div class="metric-sub">Iter: {iterations:,} | Best: #{best_iter:,}</div>
        </div>
        """, unsafe_allow_html=True)


# =====================================================
# SECTION 7: HARD CONSTRAINT STATUS (แทน Priority Miss)
# =====================================================
def render_hard_constraint_status(report, result):
    """
    แสดงสถานะ Hard Constraint (Priority 1, 2)
    — คนจัดรถอยากรู้ว่า "ส่งทันจริงมั้ย"
    """
    ov = report['overview']
    priority_summary = report.get('priority_summary', [])

    # ── Unassigned with Reasons ──
    if ov['unassigned_orders'] > 0:
        st.warning(f"⚠️ จัดเส้นทางไม่ได้ {ov['unassigned_orders']} ออเดอร์")

        unassigned_rows = []
        for o in result.solution.unassigned:
            reason = getattr(o, 'unassign_reason', '') or "ไม่ทราบสาเหตุ"

            # แปลง reason code → icon + ข้อความสั้น
            if "WEIGHT" in reason:
                icon = "⚖️"
                short = "น้ำหนักเกิน"
            elif "VOLUME" in reason:
                icon = "📦"
                short = "ปริมาตรเกิน"
            elif "PLANT_MISMATCH" in reason:
                icon = "🏭"
                short = "Plant ไม่ตรง"
            elif "DEADLINE" in reason:
                icon = "⏰"
                short = "ส่งไม่ทันเวลา"
            elif "OVERTIME" in reason:
                icon = "🕐"
                short = "เกินเวลาทำงาน"
            elif "NO_FEASIBLE" in reason or "NO_FIT" in reason:
                icon = "🚫"
                short = "ไม่มีรถที่ใส่ได้"
            else:
                icon = "❓"
                short = reason[:30]

            priority = getattr(o, 'priority', 4)
            unassigned_rows.append({
                'ลูกค้า': o.customer_name,
                'Plant': getattr(o, 'plant', ''),
                'Priority': priority,
                'น้ำหนัก (kg)': f"{o.weight_kg:,.0f}",
                'สาเหตุ': f"{icon} {short}",
            })

        df_unassigned = pd.DataFrame(unassigned_rows)

        # ── Style: priority สูง → แถวแดง ──
        def style_unassigned(df_style):
            styles = pd.DataFrame('', index=df_style.index,
                                  columns=df_style.columns)
            for idx in df_style.index:
                p = df_style.loc[idx, 'Priority']
                if p <= 2:
                    styles.loc[idx, :] = (
                        'background-color: #ffebee; '
                        'color: #c62828; font-weight: bold;'
                    )
            return styles

        styled = df_unassigned.style.apply(style_unassigned, axis=None)
        st.dataframe(
            styled,
            use_container_width=True,
            hide_index=True,
            height=min(300, 40 + len(unassigned_rows) * 36),
        )

    # ── Late ──
    if ov['num_late'] > 0:
        st.warning(f"⚠️ ส่งสาย {ov['num_late']} จุด")

    # ── Hard Constraint (Priority 1, 2) ──
    hard_priorities = []
    for ps in priority_summary:
        p = ps['priority']
        # ดึง config เดิมว่า hard หรือไม่
        cfg = DEFAULT_PRIORITY_CONFIG.get(p, {})
        if result.data and hasattr(result.data, 'priority_config'):
            cfg = result.data.priority_config.get(p, cfg)
        if cfg.get('hard', False):
            hard_priorities.append(ps)

    if hard_priorities:
        all_ok = all(ps['missed'] == 0 for ps in hard_priorities)
        for ps in hard_priorities:
            p = ps['priority']
            name = ps['name']
            assigned = ps['assigned']
            missed = ps['missed']

            if missed == 0:
                st.markdown(
                    f'<div class="hard-ok">'
                    f'✅ <b>P{p} ({name})</b>: {assigned} ออเดอร์ — ส่งทันทุกรายการ'
                    f'</div>',
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    f'<div class="hard-fail">'
                    f'❌ <b>P{p} ({name})</b>: ส่งไม่ทัน {missed} จาก {assigned + missed} ออเดอร์'
                    f'</div>',
                    unsafe_allow_html=True
                )
    else:
        # ไม่มี hard constraint → บอกว่าทุกอย่าง OK
        if ov['unassigned_orders'] == 0 and ov['num_late'] == 0:
            st.success("✅ ไม่มีปัญหาใดๆ — ทุกอย่างปกติ")


# =====================================================
# SECTION 8: ROUTE SUMMARY TABLE (st.dataframe — สวย)
# =====================================================
def render_route_summary_table(report):
    """แสดง Route Summary ด้วย st.dataframe — สีสวย อ่านง่าย"""
    route_summaries = report['route_summaries']
    if not route_summaries:
        st.info("ไม่มีเส้นทาง")
        return

    rows = []
    for rs in route_summaries:
        weight_str = f"{rs['total_weight']:.0f}/{rs['max_weight']:.0f} ({rs['weight_util']:.0f}%)"
        volume_str = f"{rs['total_volume']:.1f}/{rs['max_volume']:.0f} ({rs['volume_util']:.0f}%)"
        drops_str = f"{rs['num_stops']}/{rs['capacity_drop']}"

        rows.append({
            'Route': rs['route_number'],
            'Vehicle': rs['vehicle_id'],
            'Type': rs['vehicle_type'],
            'Drops': drops_str,
            'Weight': weight_str,
            'Volume': volume_str,
            'Distance (km)': round(rs['total_distance'], 1),
            'Hours': round(rs['working_hours'], 1),
            'Late': rs['late_count'],
        })

    df = pd.DataFrame(rows)

    # ── Style function ──
    def style_table(df_style):
        styles = pd.DataFrame('', index=df_style.index, columns=df_style.columns)

        # Late > 0 → แถวแดงอ่อน
        for idx in df_style.index:
            if df_style.loc[idx, 'Late'] > 0:
                styles.loc[idx, :] = 'background-color: #ffebee; color: #c62828;'

        return styles

    styled_df = df.style.apply(style_table, axis=None)
    styled_df = styled_df.format({
        'Distance (km)': '{:,.1f}',
        'Hours': '{:.1f}',
    })

    st.dataframe(
        styled_df,
        use_container_width=True,
        hide_index=True,
        height=min(400, 40 + len(rows) * 36),
    )

    # ── Totals row ──
    total_dist = sum(rs['total_distance'] for rs in route_summaries)
    total_drops = sum(rs['num_stops'] for rs in route_summaries)
    total_weight = sum(rs['total_weight'] for rs in route_summaries)
    total_late = sum(rs['late_count'] for rs in route_summaries)

    st.markdown(
        f"**รวม:** {len(route_summaries)} เส้นทาง | "
        f"{total_drops} จุดส่ง | "
        f"{total_weight:,.0f} kg | "
        f"{total_dist:,.1f} km | "
        f"Late: {total_late}"
    )


# =====================================================
# SECTION 9: DOWNLOAD BUTTON (ZIP)
# =====================================================
def render_download_button(result, report):
    st.markdown("---")

    zip_bytes = create_download_zip(
        result.solution, result.data,
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    st.download_button(
        label="📥 ดาวน์โหลดผลลัพธ์ (ZIP)",
        data=zip_bytes,
        file_name=f"VRP_Result_{timestamp}.zip",
        mime="application/zip",
        type="primary",
        use_container_width=True,
    )

    st.markdown("""
    <small>
    📥 <b>ไฟล์ที่ได้รับ:</b><br>
    &nbsp;&nbsp;• <b>Route_Detail.xlsx</b> — เส้นทางละเอียด, สรุป route, ออเดอร์ที่จัดไม่ได้<br>
    &nbsp;&nbsp;• <b>Material_by_Route.xlsx</b> — รายละเอียดสินค้าแยกตาม route
    </small>
    """, unsafe_allow_html=True)


# =====================================================
# SECTION 10: ROUTE DETAILS TAB
# =====================================================
def render_route_details(report):
    for rs in report['route_summaries']:
        depot_str = " → ".join(rs['depots']) if rs['depots'] else "N/A"
        with st.expander(
            f"🚛 Route {rs['route_number']} — "
            f"{rs['vehicle_id']} ({rs['vehicle_type']}) | "
            f"{rs['num_stops']} จุด | "
            f"{rs['total_weight']:.0f} kg | "
            f"{rs['total_distance']:.1f} km"
            f"{' ⚠️' if rs['late_count'] > 0 else ''}"
        ):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("จุดจอด", f"{rs['num_stops']}/{rs['capacity_drop']}")
            col2.metric("น้ำหนัก", f"{rs['weight_util']:.0f}%")
            col3.metric("ปริมาตร", f"{rs['volume_util']:.0f}%")
            col4.metric("ชั่วโมง", f"{rs['working_hours']:.1f}")

            st.markdown(f"**คลังสินค้า:** {depot_str}")
            st.markdown(f"**เวลา:** {rs['first_arrival']} — {rs['last_departure']}")

            if rs['late_count'] > 0:
                st.warning(f"⚠️ ส่งสาย {rs['late_count']} จุด")

            if rs['stops_detail']:
                stops_df = pd.DataFrame(rs['stops_detail'])
                # ✅ ตัด priority (ตัวเลข), time_window ออก
                # ✅ เหลือ priority_name
                # ✅ เพิ่ม district, province
                display_cols = [
                    'stop_number', 'customer_name',
                    'district', 'province',
                    'plant', 'priority_name',
                    'weight_kg', 'arrival_time',
                    'distance_from_prev', 'is_late'
                ]
                available = [c for c in display_cols if c in stops_df.columns]

                # Rename columns ให้อ่านง่าย
                rename_map = {
                    'stop_number': 'ลำดับ',
                    'customer_name': 'ลูกค้า',
                    'district': 'เขต/อำเภอ',
                    'province': 'จังหวัด',
                    'plant': 'คลัง',
                    'priority_name': 'Priority',
                    'weight_kg': 'น้ำหนัก (kg)',
                    'arrival_time': 'ถึงเวลา',
                    'distance_from_prev': 'ระยะทาง (km)',
                    'is_late': 'สาย',
                }
                display_df = stops_df[available].rename(
                    columns={k: v for k, v in rename_map.items() if k in available}
                )

                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True
                )


# =====================================================
# SECTION 11: MAP TAB — v3.3 แก้หน้าขาว
# =====================================================
def render_map_tab(result, report):
    st.markdown("### 📍 แผนที่เส้นทาง")

    try:
        map_obj = generate_map(
            result.solution, result.data, report
        )
    except Exception:
        map_obj = None

    if map_obj is None:
        st.warning(
            "⚠️ ไม่สามารถสร้างแผนที่ได้ — "
            "กรุณาตรวจสอบว่าติดตั้ง folium แล้ว: "
            "`pip install folium`"
        )
        return

    # ── แปลง Folium Map → HTML ──
    try:
        map_html = map_obj._repr_html_()
    except Exception:
        st.error("เกิดข้อผิดพลาดในการสร้าง HTML แผนที่")
        return

    if not map_html or len(map_html) < 100:
        st.warning("⚠️ แผนที่ว่างเปล่า — ไม่มีข้อมูลให้แสดง")
        return

    # ── แสดงแผนที่ ──
    import streamlit.components.v1 as components
    components.html(map_html, height=650, scrolling=True)

    # ── ปุ่ม Download HTML ──
    st.download_button(
        label="💾 ดาวน์โหลดแผนที่ (HTML)",
        data=map_html,
        file_name=f"VRP_Map_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
        mime="text/html",
    )


# =====================================================
# SECTION 12: CHARTS TAB
# =====================================================
def render_charts_tab(report):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    st.markdown("### 📊 กราฟวิเคราะห์")

    st.markdown("#### 📦 Weight & Volume Utilization")
    fig_util = generate_utilization_chart(report)
    if fig_util:
        st.pyplot(fig_util)
        plt.close(fig_util)
    else:
        st.info("ไม่มีข้อมูลเส้นทาง")

    st.markdown("---")

    st.markdown("#### 📏 Distance Breakdown")
    fig_dist = generate_distance_chart(report)
    if fig_dist:
        st.pyplot(fig_dist)
        plt.close(fig_dist)
    else:
        st.info("ไม่มีข้อมูลระยะทาง")


# =====================================================
# SECTION 13: ALNS TAB
# =====================================================
def render_alns_tab(result):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    op_stats = result.operator_stats or {}
    conv = generate_convergence_data(op_stats)

    st.markdown("### 🔄 ALNS Analysis")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Iterations", f"{conv['iterations']:,}")
    col2.metric("Best at", f"#{conv['best_iteration']:,}")
    col3.metric("Runtime", f"{conv['elapsed']:.1f}s")
    col4.metric("Reheats", f"{conv['reheat_count']}")

    st.markdown("#### 📈 Cost Convergence")
    fig_conv = generate_convergence_chart(op_stats)
    if fig_conv:
        st.pyplot(fig_conv)
        plt.close(fig_conv)

    st.markdown("---")

    st.markdown("#### 🌡️ Temperature Schedule")
    fig_temp = generate_temperature_chart(op_stats)
    if fig_temp:
        st.pyplot(fig_temp)
        plt.close(fig_temp)

    st.markdown("---")

    col_d, col_r = st.columns(2)
    with col_d:
        st.markdown("#### Destroy Operators")
        if conv['destroy_operators']:
            df = pd.DataFrame(conv['destroy_operators'])
            st.dataframe(df, use_container_width=True, hide_index=True)
    with col_r:
        st.markdown("#### Repair Operators")
        if conv['repair_operators']:
            df = pd.DataFrame(conv['repair_operators'])
            st.dataframe(df, use_container_width=True, hide_index=True)


# =====================================================
# SECTION 14: MAIN APP
# =====================================================
def main():
    apply_custom_css()

    st.markdown(
        '<div class="main-header">🚛 SCG VRP Optimizer</div>',
        unsafe_allow_html=True
    )
    st.markdown(
        '<div class="sub-header">'
        'Vehicle Routing Problem — ALNS + Multi-Depot + Priority'
        '</div>',
        unsafe_allow_html=True
    )

    sidebar = render_sidebar()
    uploaded_file = sidebar['uploaded_file']
    run_clicked = sidebar['run_clicked']

    # ══════════════════════════════════════════════════
    # RUN PIPELINE
    # ══════════════════════════════════════════════════
    if run_clicked and uploaded_file:

        ALNSConfig.TIME_LIMIT_SECONDS = sidebar['time_limit']
        ALNSConfig.NO_IMPROVEMENT_LIMIT = sidebar['no_imp_limit']
        ALNSConfig.INITIAL_TEMPERATURE = sidebar['initial_temp']
        ALNSConfig.REHEAT_ENABLED = sidebar['reheat_enabled']

        st.markdown("---")

        progress_bar = st.progress(0)
        status_text = st.empty()
        log_container = st.container()

        logger = StreamlitProgressLogger(
            log_container, progress_bar, status_text
        )

        try:
            result = run_pipeline(
                excel_file=uploaded_file,
                progress=logger.get_vrp_progress()
            )
        except Exception:
            logger.clear()
            st.error(
                "เกิดข้อผิดพลาดในการประมวลผล กรุณาตรวจสอบไฟล์แล้วลองใหม่อีกครั้ง"
            )
            return

        logger.clear()

        if not result.is_valid:
            st.error("❌ พบข้อผิดพลาดในข้อมูล")
            for e in result.errors:
                st.error(e)
            for w in result.warnings:
                st.warning(w)
            return

        st.session_state['result'] = result
        st.session_state['summary_report'] = generate_summary_report(
            result.solution, result.data
        )

    # ══════════════════════════════════════════════════
    # DISPLAY RESULTS
    # ══════════════════════════════════════════════════
    if 'result' in st.session_state and 'summary_report' in st.session_state:
        result = st.session_state['result']
        report = st.session_state['summary_report']

        st.markdown("---")
        st.markdown("## 📊 ผลลัพธ์การจัดเส้นทาง")

        # ── 1) Summary Boxes (4 กล่อง) ──
        render_summary_boxes(report, result)

        # ── 2) Warnings + Hard Constraint Status (ใต้ summary) ──
        render_hard_constraint_status(report, result)

        # ══════════════════════════════════════════════
        # TABS
        # ══════════════════════════════════════════════
        tab_summary, tab_routes, tab_map, tab_charts, tab_alns = st.tabs([
            "📋 สรุป", "🗺️ เส้นทาง", "📍 แผนที่",
            "📊 กราฟ", "🔄 ALNS"
        ])

        with tab_summary:
            st.markdown("### 📋 Route Summary")
            render_route_summary_table(report)
            render_download_button(result, report)

        with tab_routes:
            st.markdown("### 🗺️ รายละเอียดเส้นทาง")
            render_route_details(report)

        with tab_map:
            render_map_tab(result, report)

        with tab_charts:
            render_charts_tab(report)

        with tab_alns:
            render_alns_tab(result)

    else:
        # ══════════════════════════════════════════════
        # WELCOME SCREEN
        # ══════════════════════════════════════════════
        st.markdown("---")
        st.markdown("""
        ### 👋 ยินดีต้อนรับ!

        **วิธีใช้งาน:**
        1. 📁 อัปโหลดไฟล์ Excel ทางซ้ายมือ
        2. ⚙️ ปรับตั้งค่า ALNS (ถ้าต้องการ)
        3. 🚀 กดปุ่ม **"เริ่มจัดเส้นทาง"**

        **ไฟล์ Excel ต้องมีแท็บ:**
        | แท็บ | คำอธิบาย |
        |------|----------|
        | `Depots` | ข้อมูลคลังสินค้า |
        | `นำเข้าข้อมูลออเดอร์` | รายการสินค้า |
        | `01.ออเดอร์` | ข้อมูลลูกค้า + Priority |
        | `02.ข้อมูลรถ` | ข้อมูลรถที่ใช้ได้ |
        | `03.ระยะทาง` | ตารางระยะทาง |
        """)


# =====================================================
# SECTION 15: ENTRY POINT
# =====================================================
if __name__ == "__main__":
    main()