import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import json
from datetime import date, datetime as dt
import calendar
import io
import socket
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm

# --- FIX IPV4 ---
try:
    if not hasattr(socket, '_getaddrinfo_orig'):
        socket._getaddrinfo_orig = socket.getaddrinfo
    def getaddrinfo_ipv4_only(*args, **kwargs):
        responses = socket._getaddrinfo_orig(*args, **kwargs)
        return [r for r in responses if r[0] == socket.AF_INET]
    socket.getaddrinfo = getaddrinfo_ipv4_only
except Exception as e:
    pass

# --- CONFIGURATION ---
st.set_page_config(page_title="Paie & RH", layout="wide", page_icon="👥")

try:
    from jours_feries_france import JoursFeries
except ImportError:
    st.error("Manque : pip install jours-feries-france")
    st.stop()

DAYS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
OPTIONS_STATUT = ["Normal", "Congé", "Arrêt Maladie", "Absence Injustifiée", "Récupération"]
STD_MS, STD_ME, STD_AS, STD_AE = "08:30", "12:00", "14:00", "17:30"

# --- SESSION STATE ---
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'username' not in st.session_state: st.session_state.username = ""
if 'is_admin' not in st.session_state: st.session_state.is_admin = False
if 'curr_emp_id' not in st.session_state: st.session_state['curr_emp_id'] = None
if 'confirm_delete_id' not in st.session_state: st.session_state['confirm_delete_id'] = None

# ==================================================================================
# 1. FONCTIONS SYSTEME & BDD
# ==================================================================================

# --- CONNEXION SUPABASE ---
@st.cache_resource
def init_connection():
    try: return psycopg2.connect(st.secrets["postgres"]["url"])
    except Exception as e: st.error(f"Erreur DB: {e}"); st.stop()

def run_query(query, params=None, fetch="all"):
    conn = init_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(query, params)
            if fetch == "all": return cur.fetchall()
            elif fetch == "one": return cur.fetchone()
            elif fetch == "none": conn.commit(); return None
        except Exception as e:
            conn.rollback(); st.error(f"SQL Error: {e}"); return None

def init_db():
    queries = [
        '''CREATE TABLE IF NOT EXISTS salaries (id SERIAL PRIMARY KEY, nom TEXT NOT NULL, mode_alternance INTEGER DEFAULT 0, solde_banque REAL DEFAULT 0, config_horaires TEXT, is_archived INTEGER DEFAULT 0)''',
        '''CREATE TABLE IF NOT EXISTS pointages (id SERIAL PRIMARY KEY, salarie_id INTEGER, date_pointage DATE, m_start TEXT, m_end TEXT, a_start TEXT, a_end TEXT, statut TEXT DEFAULT 'Normal', comment TEXT, UNIQUE(salarie_id, date_pointage))''',
        '''CREATE TABLE IF NOT EXISTS banque_history (id SERIAL PRIMARY KEY, salarie_id INTEGER, date_mouv DATE, montant REAL, motif TEXT, type_mouv TEXT, auteur TEXT)''',
        '''CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT, is_admin INTEGER DEFAULT 0, is_active INTEGER DEFAULT 0)'''
    ]
    for q in queries: run_query(q, fetch="none")
# init_db()

# --- UTILITAIRES TEMPS ---
def str_to_time(t): return dt.strptime(t, "%H:%M").time() if t else None
def time_to_str(t): return t.strftime("%H:%M") if t else None
def calc_duree_journee(m_s, m_e, a_s, a_e):
    def d(s, e):
        if s and e:
            try: return max(0.0, (dt.strptime(str(e)[:5],"%H:%M") - dt.strptime(str(s)[:5],"%H:%M")).total_seconds()/3600)
            except: return 0.0
        return 0.0
    return d(m_s, m_e) + d(a_s, a_e)
def has_ticket_resto(row):
    if row['statut'] != 'Normal': return False
    m = calc_duree_journee(row.get('m_start'), row.get('m_end'), None, None)
    a = calc_duree_journee(None, None, row.get('a_start'), row.get('a_end'))
    return True if (m > 0 and a > 0) else False
def is_even_week(d): return d.isocalendar()[1] % 2 == 0
def get_config_for_day(emp_json, d_obj):
    if not emp_json: return None, None, None, None
    config = json.loads(emp_json)
    key = 'paire' if is_even_week(d_obj) else 'impaire'
    if key not in config: key = 'paire'
    d = config[key][d_obj.weekday()]
    return d['ms'], d['me'], d['as'], d['ae']
def get_default_schedule():
    std = {'ms': STD_MS, 'me': STD_ME, 'as': STD_AS, 'ae': STD_AE}
    empty = {'ms': None, 'me': None, 'as': None, 'ae': None}
    week = [std.copy() for _ in range(5)] + [empty.copy()] + [empty.copy()]
    return {'paire': week, 'impaire': week}

# --- FONCTIONS BASE DE DONNEES ---
def db_upsert_salarie(id_s, nom, mode, sched):
    j = json.dumps(sched)
    chk = run_query("SELECT id FROM salaries WHERE nom=%s", (nom,), fetch="one")
    if chk and (id_s is None or chk['id'] != id_s): return False, "Nom pris."
    if id_s is None: run_query('INSERT INTO salaries (nom, mode_alternance, config_horaires, is_archived) VALUES (%s,%s,%s,0)', (nom, mode, j), fetch="none")
    else: run_query('UPDATE salaries SET nom=%s, mode_alternance=%s, config_horaires=%s WHERE id=%s', (nom, mode, j, id_s), fetch="none")
    return True, "Sauvegardé."
def db_archive_salarie(s_id): run_query('UPDATE salaries SET is_archived = 1 WHERE id = %s', (s_id,), fetch="none")
def db_restore_salarie(s_id): run_query('UPDATE salaries SET is_archived = 0 WHERE id = %s', (s_id,), fetch="none")
def db_delete_salarie_total(s_id):
    run_query('DELETE FROM pointages WHERE salarie_id = %s', (s_id,), fetch="none")
    run_query('DELETE FROM banque_history WHERE salarie_id = %s', (s_id,), fetch="none")
    run_query('DELETE FROM salaries WHERE id = %s', (s_id,), fetch="none")
def db_save_pointage(s_id, d_obj, ms, me, ads, ae, stat, cmt):
    if isinstance(d_obj, str):
        try: d_iso = dt.strptime(d_obj, "%d/%m/%Y").strftime("%Y-%m-%d")
        except: d_iso = d_obj 
    else: d_iso = d_obj.strftime("%Y-%m-%d")
    run_query('''INSERT INTO pointages (salarie_id, date_pointage, m_start, m_end, a_start, a_end, statut, comment) 
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (salarie_id, date_pointage) 
        DO UPDATE SET m_start=EXCLUDED.m_start, m_end=EXCLUDED.m_end, a_start=EXCLUDED.a_start, a_end=EXCLUDED.a_end, statut=EXCLUDED.statut, comment=EXCLUDED.comment''', 
        (s_id, d_iso, ms, me, ads, ae, stat, cmt), fetch="none")
def db_update_banque(s_id, montant, motif, type_mouv="Manuel"):
    aut = st.session_state.username
    td = date.today().strftime("%Y-%m-%d")
    run_query('INSERT INTO banque_history (salarie_id, date_mouv, montant, motif, type_mouv, auteur) VALUES (%s,%s,%s,%s,%s,%s)', (s_id, td, montant, motif, type_mouv, aut), fetch="none")
    run_query('UPDATE salaries SET solde_banque = solde_banque + %s WHERE id = %s', (montant, s_id), fetch="none")

def db_get_transferred_hs_for_month(s_id, month_label):
    pattern = f"%HS% {month_label}%"
    row = run_query('SELECT SUM(montant) as total FROM banque_history WHERE salarie_id=%s AND motif LIKE %s', (s_id, pattern), fetch="one")
    return row['total'] if row['total'] else 0.0

def db_get_banque_history(s_id): return run_query('SELECT * FROM banque_history WHERE salarie_id=%s ORDER BY id DESC', (s_id,), fetch="all")
def db_get_pointages(s_id, y, m):
    last = calendar.monthrange(y, m)[1]
    rows = run_query('SELECT * FROM pointages WHERE salarie_id=%s AND date_pointage BETWEEN %s AND %s', (s_id, f"{y}-{m:02d}-01", f"{y}-{m:02d}-{last}"), fetch="all")
    return {str(r['date_pointage']): dict(r) for r in rows} if rows else {}

# --- USERS ---
def create_user(u, p):
    cnt = run_query('SELECT count(*) as cnt FROM users', fetch="one")['cnt']
    adm, act = (1, 1) if cnt == 0 else (0, 0)
    msg = "Admin créé !" if adm else "Attente validation."
    if run_query('SELECT username FROM users WHERE username = %s', (u,), fetch="one"): return False, "Pris."
    run_query('INSERT INTO users (username, password, is_admin, is_active) VALUES (%s,%s,%s,%s)', (u, p, adm, act), fetch="none")
    return True, msg
def check_login(u, p):
    user = run_query('SELECT * FROM users WHERE username = %s AND password = %s', (u, p), fetch="one")
    if user: return ("OK", bool(user['is_admin'])) if user['is_active'] else ("PENDING", False)
    return "FAIL", False
def get_all_users(): return run_query('SELECT username, is_admin, is_active FROM users', fetch="all")
def admin_actions_user(act, tgt, val=None):
    if act=="approve": run_query('UPDATE users SET is_active=1 WHERE username=%s', (tgt,), fetch="none")
    elif act=="reject": run_query('DELETE FROM users WHERE username=%s', (tgt,), fetch="none")
    elif act=="reset": run_query('UPDATE users SET password=%s WHERE username=%s', (val, tgt), fetch="none")
    elif act=="promote": run_query('UPDATE users SET is_admin=1 WHERE username=%s', (tgt,), fetch="none")
    elif act=="demote": run_query('UPDATE users SET is_admin=0 WHERE username=%s', (tgt,), fetch="none")
    elif act=="transfer":
        curr = st.session_state.username
        run_query('UPDATE users SET is_admin=1 WHERE username=%s', (tgt,), fetch="none")
        run_query('UPDATE users SET is_admin=0 WHERE username=%s', (curr,), fetch="none")

# --- BACKUP ---
def create_backup_json():
    d = {t: run_query(f"SELECT * FROM {t}", fetch="all") for t in ["salaries", "pointages", "banque_history", "users"]}
    return json.dumps(d, indent=4, default=str)
def restore_backup_json(f):
    try:
        d = json.load(f)
        run_query("TRUNCATE pointages, banque_history, salaries, users RESTART IDENTITY", fetch="none")
        for u in d.get('users', []): run_query("INSERT INTO users VALUES (%s,%s,%s,%s)", (u['username'], u['password'], u['is_admin'], u['is_active']), fetch="none")
        for s in d.get('salaries', []): run_query("INSERT INTO salaries (id, nom, mode_alternance, solde_banque, config_horaires, is_archived) OVERRIDING SYSTEM VALUE VALUES (%s,%s,%s,%s,%s,COALESCE(%s,0))", (s['id'], s['nom'], s['mode_alternance'], s['solde_banque'], s['config_horaires'], s.get('is_archived',0)), fetch="none")
        for p in d.get('pointages', []): run_query("INSERT INTO pointages VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (p['id'], p['salarie_id'], p['date_pointage'], p['m_start'], p['m_end'], p['a_start'], p['a_end'], p['statut'], p['comment']), fetch="none")
        for b in d.get('banque_history', []): run_query("INSERT INTO banque_history VALUES (%s,%s,%s,%s,%s,%s,%s)", (b['id'], b['salarie_id'], b['date_mouv'], b['montant'], b['motif'], b['type_mouv'], b['auteur']), fetch="none")
        return True
    except Exception as e: st.error(f"Err: {e}"); return False

# --- CALCUL STATS (HARMONISÉ) ---
def calculate_stats(sid, y, m, cfg):
    db_pts = db_get_pointages(sid, y, m)
    banked = db_get_transferred_hs_for_month(sid, f"{m}/{y}")
    _, last = calendar.monthrange(y, m)
    days = [date(y, m, d) for d in range(1, last+1)]
    feries = JoursFeries.for_year(y)
    wh, nr, nt, nc, nm, na, tr, det = {}, 0, 0, 0, 0, 0, 0, []
    
    for d in days:
        row = db_pts.get(d.strftime("%Y-%m-%d"), {})
        stt = row.get('statut', 'Normal')
        rms, rme, ras, rae = row.get('m_start'), row.get('m_end'), row.get('a_start'), row.get('a_end')
        tms, tme, tas, tae = get_config_for_day(cfg, d)
        if not row: rms=rme=ras=rae=None; stt="Normal"
        hr = calc_duree_journee(rms, rme, ras, rae)
        ht = calc_duree_journee(tms, tme, tas, tae)
        
        # Compteurs
        if stt=="Congé": nc+=1
        elif stt=="Arrêt Maladie": nm+=1
        elif stt=="Absence Injustifiée": na+=1
        
        if has_ticket_resto({'statut':stt,'m_start':rms,'m_end':rme,'a_start':ras,'a_end':rae}): tr+=1
        
        h_bk = ht if (stt!="Normal" and stt!="Récupération") else (0.0 if stt=="Récupération" else hr)
        nr+=h_bk; nt+=ht
        wn=d.isocalendar()[1]
        if wn not in wh: wh[wn]=0.0
        wh[wn]+=hr
        det.append({"Date": d.strftime("%d/%m/%Y"), "Jour": DAYS_FR[d.weekday()], "Statut": stt, "Matin": f"{rms}-{rme}" if rms else "", "Aprem": f"{ras}-{rae}" if ras else "", "Heures": hr})
    
    h25, h50, ghs = 0, 0, 0
    for w, h in wh.items():
        if h>35:
            s = h-35; ghs+=s
            h25+=min(s,8); h50+=max(0,s-8)
            
    # RETOUR DICTIONNAIRE
    return {
        "total_real": nr, 
        "total_tr": tr, 
        "nb_conge": nc, 
        "nb_maladie": nm, 
        "nb_abs": na, 
        "gen_hs_total": ghs, 
        "hs_25": h25, 
        "hs_50": h50, 
        "banked": banked, 
        "hs_payable": max(0, ghs-banked), 
        "delta_bank": nr-nt, 
        "details": det
    }

def create_pdf_releve(nom, per, st):
    b = io.BytesIO(); doc = SimpleDocTemplate(b, pagesize=A4); el = []; s = getSampleStyleSheet()
    el.append(Paragraph(f"Relevé: {nom} - {per}", s['Heading1'])); el.append(Spacer(1, 0.5*cm))
    d = [["H. Trav.", f"{st['total_real']:.2f}", "TR", f"{st['total_tr']}"], ["HS Tot", f"{st['gen_hs_total']:.2f}", "Congé", f"{st['nb_conge']}"], ["A Payer", f"{st['hs_payable']:.2f}", "Maladie", f"{st['nb_maladie']}"]]
    t = Table(d, colWidths=[4*cm,3*cm,4*cm,3*cm]); t.setStyle(TableStyle([('GRID',(0,0),(-1,-1),1,colors.black),('BACKGROUND',(0,0),(-1,-1),colors.whitesmoke)])); el.append(t); el.append(Spacer(1, 0.5*cm))
    det = [["Date","Jour","Statut","M","A","Tot"]]
    for r in st['details']: det.append([r['Date'],r['Jour'][:3],r['Statut'][:10],r['Matin'],r['Aprem'],f"{r['Heures']:.2f}"])
    t2 = Table(det, colWidths=[2.5*cm,2*cm,3*cm,3*cm,3*cm,1.5*cm]); t2.setStyle(TableStyle([('GRID',(0,0),(-1,-1),0.5,colors.grey)])); el.append(t2); doc.build(el); b.seek(0)
    return b

def render_week_inputs_simple(prefix, default_data):
    if st.button("⚡ Remplir Formulaire", key=f"btn_{prefix}"):
        for i in range(5): 
            st.session_state[f"{prefix}_{i}_ms"] = str_to_time(STD_MS); st.session_state[f"{prefix}_{i}_me"] = str_to_time(STD_ME)
            st.session_state[f"{prefix}_{i}_as"] = str_to_time(STD_AS); st.session_state[f"{prefix}_{i}_ae"] = str_to_time(STD_AE)
        st.rerun()
    new_data = []
    for i, day in enumerate(DAYS_FR):
        c1, c2, c3, c4 = st.columns(4)
        d = default_data[i]
        def g(k, v): 
            key=f"{prefix}_{i}_{k}"; 
            if key not in st.session_state: st.session_state[key] = str_to_time(v)
            return key
        ms = c1.time_input(f"{day[:3]} M", key=g("ms", d['ms']), label_visibility="collapsed")
        me = c2.time_input("M", key=g("me", d['me']), label_visibility="collapsed")
        ads = c3.time_input("A", key=g("as", d['as']), label_visibility="collapsed")
        ae = c4.time_input("A", key=g("ae", d['ae']), label_visibility="collapsed")
        new_data.append({'ms': time_to_str(ms), 'me': time_to_str(me), 'as': time_to_str(ads), 'ae': time_to_str(ae)})
    return new_data

# --- LOGIN UI ---
if not st.session_state.logged_in:
    st.title("☁️ Connexion")
    with st.expander("📤 RESTAURER"):
        up = st.file_uploader("JSON", type=['json'])
        if up and st.button("CONFIRMER"): 
            if restore_backup_json(up): st.success("OK"); st.rerun()
    t1, t2 = st.tabs(["Login", "Créer"])
    with t1:
        with st.form("l"):
            u = st.text_input("ID"); p = st.text_input("MDP", type="password")
            if st.form_submit_button("Go"):
                s, adm = check_login(u, p)
                if s=="OK": st.session_state.logged_in=True; st.session_state.username=u; st.session_state.is_admin=adm; st.rerun()
                else: st.error(s)
    with t2:
        with st.form("c"):
            nu = st.text_input("ID"); np = st.text_input("MDP", type="password")
            if st.form_submit_button("Créer"):
                ok, m = create_user(nu, np)
                if ok: st.success(m)
                else: st.error(m)
    st.stop()

# --- APP ---
with st.sidebar:
    role = "Admin" if st.session_state.is_admin else "User"
    st.write(f"👤 **{st.session_state.username}** ({role})")
    st.download_button("⬇️ BACKUP JSON", create_backup_json(), f"Backup_{date.today()}.json", "application/json")
    st.markdown("---")
    
    if st.session_state.is_admin:
        st.header("🛠️ Admin")
        users = get_all_users()
        pending = [u['username'] for u in users if u['is_active'] == 0]
        active = [u['username'] for u in users if u['is_active'] == 1 and u['username'] != st.session_state.username]
        if pending:
            st.error(f"{len(pending)} demande(s)")
            tp = st.selectbox("Valider", pending)
            c1,c2=st.columns(2)
            if c1.button("✅"): admin_actions_user("approve", tp); st.rerun()
            if c2.button("❌"): admin_actions_user("reject", tp); st.rerun()
        with st.expander("Utilisateurs"):
            if active:
                tu = st.selectbox("Cible", active)
                act = st.selectbox("Action", ["Reset MDP", "Supprimer", "Co-Admin", "Transférer droits", "Rétrograder"])
                if act == "Reset MDP":
                    np = st.text_input("New Pass", type="password")
                    if st.button("OK"): admin_actions_user("reset", tu, np); st.success("Fait")
                elif act == "Supprimer" and st.button("Confirmer"): admin_actions_user("reject", tu); st.rerun()
                elif act == "Co-Admin" and st.button("Promouvoir"): admin_actions_user("promote", tu); st.rerun()
                elif act == "Transférer droits" and st.button("Transférer"): admin_actions_user("transfer", tu); st.session_state.is_admin=False; st.rerun()
                elif act == "Rétrograder" and st.button("Enlever Admin"): admin_actions_user("demote", tu); st.rerun()
        
        with st.expander("Salariés (Archives)"):
            act_sals = run_query("SELECT * FROM salaries WHERE is_archived=0", fetch="all")
            if act_sals:
                ts = st.selectbox("Actif", [s['nom'] for s in act_sals])
                tid = next(s['id'] for s in act_sals if s['nom']==ts)
                if st.button("🗄️ Archiver"): db_archive_salarie(tid); st.rerun()
                if st.button("🗑️ Demander Suppr"): st.session_state['confirm_delete_id'] = tid
                if st.session_state.get('confirm_delete_id') == tid:
                    st.error("⚠️ Irréversible !")
                    if st.button("🔥 CONFIRMER"): db_delete_salarie_total(tid); st.session_state['confirm_delete_id']=None; st.rerun()
            arc_sals = run_query("SELECT * FROM salaries WHERE is_archived=1", fetch="all")
            if arc_sals:
                st.write("---")
                tas = st.selectbox("Archivé", [s['nom'] for s in arc_sals])
                taid = next(s['id'] for s in arc_sals if s['nom']==tas)
                if st.button("♻️ Restaurer"): db_restore_salarie(taid); st.rerun()

    st.markdown("---")
    st.header("⚙️ Salarié")
    mode = st.radio("Mode", ["Nouveau", "Modifier"], horizontal=True)
    f_nom, f_alt, f_sch, f_id = "", False, get_default_schedule(), None
    
    emps = run_query("SELECT * FROM salaries WHERE is_archived=0", fetch="all")
    
    if mode == "Modifier":
        if emps:
            sel = st.selectbox("Choisir", [e['nom'] for e in emps])
            e_obj = next(e for e in emps if e['nom'] == sel)
            if st.session_state['curr_emp_id'] != e_obj['id']:
                for k in list(st.session_state.keys()): 
                    if k.startswith(("p_","i_","std_")): del st.session_state[k]
                st.session_state['curr_emp_id'] = e_obj['id']; st.rerun()
            f_id, f_nom, f_alt = e_obj['id'], e_obj['nom'], bool(e_obj['mode_alternance'])
            if e_obj['config_horaires']: f_sch = json.loads(e_obj['config_horaires'])
    else:
        if st.session_state['curr_emp_id']: st.session_state['curr_emp_id'] = None; st.rerun()

    use_alt = st.checkbox("Alternance", value=f_alt)
    n_in = st.text_input("Nom", value=f_nom)
    fp, fi = [], []
    if use_alt:
        t1, t2 = st.tabs(["Paire", "Impaire"])
        with t1: fp = render_week_inputs_simple("p", f_sch['paire'])
        with t2: fi = render_week_inputs_simple("i", f_sch['impaire'])
    else:
        fp = render_week_inputs_simple("std", f_sch['paire'])
        fi = fp
        
    if st.button("💾 SAUVEGARDER CONFIG"):
        if n_in:
            ok, m = db_upsert_salarie(f_id, n_in, 1 if use_alt else 0, {'paire':fp, 'impaire':fi})
            if ok: st.success(m); st.rerun()
            else: st.error(m)

    if st.button("Déconnexion", type="secondary"): st.session_state.logged_in=False; st.rerun()

# --- MAIN ---
st.title("🗓️ Planning Cloud")
employees = run_query("SELECT * FROM salaries WHERE is_archived=0", fetch="all")

if not employees: st.warning("Aucun salarié actif.")
else:
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1.5])
    emp_map = {e['nom']: e for e in employees}
    try: curr_emp = emp_map[c1.selectbox("Salarié", list(emp_map.keys()))]
    except: curr_emp = list(emp_map.values())[0]
    today = date.today()
    yr = c2.number_input("Année", 2024, 2030, today.year)
    mo = c3.selectbox("Mois", range(1, 13), index=today.month-1, format_func=lambda x: calendar.month_name[x])
    
    # --- CALCUL DES STATS ---
    stats = calculate_stats(curr_emp['id'], yr, mo, curr_emp['config_horaires'])
    
    with c4:
        st.write("")
        c_b1, c_b2 = st.columns(2)
        if c_b1.button("📥 EXCEL"):
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='openpyxl') as w:
                g_rows = []
                for emp in employees:
                    s = calculate_stats(emp['id'], yr, mo, emp['config_horaires'])
                    g_rows.append({"Salarié": emp['nom'], "H. Trav": s['total_real'], "Congés": s['nb_conge'], "HS 25%": s['hs_25'], "HS 50%": s['hs_50'], "Reste Payer": s['hs_payable'], "Solde Bq": emp['solde_banque'] + s['delta_bank']})
                pd.DataFrame(g_rows).to_excel(w, index=False, sheet_name="Global")
                for emp in employees:
                    s = calculate_stats(emp['id'], yr, mo, emp['config_horaires'])
                    name = emp['nom'][:30].replace(":", "")
                    pd.DataFrame(s['details']).to_excel(w, index=False, sheet_name=name)
            out.seek(0); st.download_button("⬇️", out, f"Paie_Global_{mo}_{yr}.xlsx")
        if c_b2.button("📄 PDF"):
            pdf = create_pdf_releve(curr_emp['nom'], f"{mo}/{yr}", stats)
            st.download_button("⬇️", pdf, f"Releve_{curr_emp['nom']}.pdf", "application/pdf")

    st.markdown("### 🗓️ Saisie")
    col_fill, col_dummy = st.columns([1, 3])
    with col_fill:
        if st.button("✨ Remplir vides"):
            ld = calendar.monthrange(yr, mo)[1]
            existing = run_query('SELECT date_pointage FROM pointages WHERE salarie_id=%s AND date_pointage BETWEEN %s AND %s', (curr_emp['id'], f"{yr}-{mo:02d}-01", f"{yr}-{mo:02d}-{ld}"), fetch="all")
            ex_d = [str(r['date_pointage']) for r in existing] if existing else []
            days = [date(yr, mo, d) for d in range(1, ld+1)]
            feries = JoursFeries.for_year(yr)
            cnt = 0
            for d in days:
                iso = d.strftime("%Y-%m-%d")
                if iso not in ex_d:
                    ms, me, ads, ae = get_config_for_day(curr_emp['config_horaires'], d)
                    stat = "Normal"
                    if feries.get(d): stat = "Normal"
                    elif d.weekday() == 6: ms=me=ads=ae=None
                    db_save_pointage(curr_emp['id'], iso, ms, me, ads, ae, stat, "")
                    cnt+=1
            st.success(f"{cnt} jours."); st.rerun()

    ld = calendar.monthrange(yr, mo)[1]
    s, e = f"{yr}-{mo:02d}-01", f"{yr}-{mo:02d}-{ld}"
    rows = run_query('SELECT * FROM pointages WHERE salarie_id=%s AND date_pointage BETWEEN %s AND %s', (curr_emp['id'], s, e), fetch="all")
    db_pts = {str(r['date_pointage']): dict(r) for r in rows} if rows else {}
    days = [date(yr, mo, d) for d in range(1, ld+1)]
    feries = JoursFeries.for_year(yr)
    data_list = []
    for d in days:
        d_iso, d_fr = d.strftime("%Y-%m-%d"), d.strftime("%d/%m/%Y")
        row = db_pts.get(d_iso, {})
        stat = row.get('statut', 'Normal')
        rms, rme = row.get('m_start'), row.get('m_end')
        ras, rae = row.get('a_start'), row.get('a_end')
        cmt = row.get('comment', '')
        if d_iso not in db_pts: rms=rme=ras=rae=None; stat="Normal"
        h_real = calc_duree_journee(rms, rme, ras, rae)
        row_sim = {'statut': stat, 'm_start': rms, 'm_end': rme, 'a_start': ras, 'a_end': rae}
        nb_tr = 1 if has_ticket_resto(row_sim) else 0
        is_f = 1 if feries.get(d) else 0
        is_s = 1 if d.weekday() == 6 else 0
        if is_f and not cmt: cmt = f"Férié : {feries.get(d)}"
        data_list.append({"Date": d_fr, "Jour": DAYS_FR[d.weekday()], "Type": stat, "Matin Début": rms, "Matin Fin": rme, "Aprèm Début": ras, "Aprèm Fin": rae, "Total": h_real, "TR": nb_tr, "Commentaire": cmt, "is_ferie": is_f, "is_sun": is_s})
    df = pd.DataFrame(data_list)
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_column("is_ferie", hide=True); gb.configure_column("is_sun", hide=True)
    gb.configure_column("Date", width=90); gb.configure_column("Jour", width=120); gb.configure_column("Total", width=60); gb.configure_column("TR", width=50)
    gb.configure_column("Type", editable=True, cellEditor='agSelectCellEditor', cellEditorParams={'values': OPTIONS_STATUT}, width=130)
    gb.configure_column("Commentaire", editable=True, width=200)
    for c in ["Matin Début", "Matin Fin", "Aprèm Début", "Aprèm Fin"]: gb.configure_column(c, editable=True, width=90)
    
    jscode = JsCode("""function(params) {
        let style = {'color': 'black', 'background-color': 'white'};
        if (params.data.is_ferie === 1 || params.data.is_sun === 1) style['background-color'] = '#e0e0e0';
        if (params.data.Type === 'Arrêt Maladie') style['background-color'] = '#ffb3b3';
        if (params.data.Type === 'Congé') style['background-color'] = '#b3d9ff';
        if (params.data.Type === 'Absence Injustifiée') { style['background-color'] = '#ff4d4d'; style['color'] = 'white'; }
        if (params.data.Type === 'Récupération') style['background-color'] = '#ccffcc';
        return style;
    }""")
    gb.configure_grid_options(getRowStyle=jscode)
    grid_resp = AgGrid(df, gridOptions=gb.build(), height=500, allow_unsafe_jscode=True, theme='streamlit', update_mode=GridUpdateMode.VALUE_CHANGED)
    updated_df = pd.DataFrame(grid_resp['data'])
    if st.button("💾 SAUVEGARDER SAISIE", type="primary"):
        for i, r in updated_df.iterrows():
            def cl(v): return v if v and v != "None" and v != "" else None
            d_obj = dt.strptime(r['Date'], "%d/%m/%Y").strftime("%Y-%m-%d")
            db_save_pointage(curr_emp['id'], d_obj, cl(r['Matin Début']), cl(r['Matin Fin']), cl(r['Aprèm Début']), cl(r['Aprèm Fin']), r['Type'], r['Commentaire'])
        st.toast("Sauvegardé !", icon="✅"); st.rerun()

    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.subheader("🏖️ Absences")
        st.write(f"Congés: **{stats['nb_conge']}**")
        st.write(f"Maladie: **{stats['nb_maladie']}**")
        st.write(f"Injust.: **{stats['nb_abs']}**")
    with c2:
        st.subheader("💰 Paie")
        st.write(f"HS Tot: **{stats['gen_hs_total']:.2f}h**")
        st.write(f"Banked: **-{stats['banked']:.2f}h**")
        st.metric("Reste", f"{stats['hs_payable']:.2f} h")
    with c3:
        st.subheader("🏦 Transfert")
        with st.form("trf"):
            amt = st.number_input("Heures", max_value=float(stats['hs_payable']))
            if st.form_submit_button("Verser"):
                db_update_banque(curr_emp['id'], amt, f"Transf HS {mo}/{yr}", "Auto")
                st.rerun()
    with c4:
        st.subheader("📊 Solde / TR")
        st.metric("SOLDE BANQUE", f"{curr_emp['solde_banque']:.2f} h")
        st.metric("🎟️ TR", f"{stats['total_tr']}")
        with st.expander("Correction"):
            with st.form("adj"):
                v = st.number_input("+/-"); m = st.text_input("Motif")
                if st.form_submit_button("OK"): db_update_banque(curr_emp['id'], v, m); st.rerun()

    st.markdown("---")
    st.caption("Historique Banque")
    rh = run_query('SELECT * FROM banque_history WHERE salarie_id=%s ORDER BY id DESC', (curr_emp['id'],), fetch="all")
    if rh: st.dataframe(pd.DataFrame([dict(r) for r in rh]), use_container_width=True)