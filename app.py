import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import datetime
import json
from datetime import date, datetime as dt
import calendar
import io
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm

# --- CONFIGURATION ---
st.set_page_config(page_title="Paie Cloud", layout="wide", page_icon="☁️")

try:
    from jours_feries_france import JoursFeries
except ImportError:
    st.error("Manque : pip install jours-feries-france")
    st.stop()

DAYS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
OPTIONS_STATUT = ["Normal", "Congé", "Arrêt Maladie", "Absence Injustifiée", "Récupération"]

STD_MS, STD_ME = "08:30", "12:00"
STD_AS, STD_AE = "14:00", "17:30"

# --- SESSION STATE ---
if 'undo_stack' not in st.session_state: st.session_state.undo_stack = []
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'username' not in st.session_state: st.session_state.username = ""
if 'is_admin' not in st.session_state: st.session_state.is_admin = False
if 'curr_emp_id' not in st.session_state: st.session_state['curr_emp_id'] = None

# --- CONNEXION SUPABASE (PostgreSQL) ---
@st.cache_resource
def init_connection():
    try:
        return psycopg2.connect(st.secrets["postgres"]["url"])
    except Exception as e:
        st.error(f"Erreur de connexion à Supabase: {e}")
        st.stop()

def run_query(query, params=None, fetch="all"):
    conn = init_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            cur.execute(query, params)
            if fetch == "all":
                return cur.fetchall()
            elif fetch == "one":
                return cur.fetchone()
            elif fetch == "none":
                conn.commit()
                return None
        except Exception as e:
            conn.rollback()
            st.error(f"Erreur SQL: {e}")
            return None

def init_db():
    queries = [
        '''CREATE TABLE IF NOT EXISTS salaries (
            id SERIAL PRIMARY KEY, 
            nom TEXT NOT NULL, 
            mode_alternance INTEGER DEFAULT 0, 
            solde_banque REAL DEFAULT 0, 
            config_horaires TEXT)''',
        '''CREATE TABLE IF NOT EXISTS pointages (
            id SERIAL PRIMARY KEY, 
            salarie_id INTEGER, 
            date_pointage DATE, 
            m_start TEXT, m_end TEXT, a_start TEXT, a_end TEXT, 
            statut TEXT DEFAULT 'Normal', 
            comment TEXT,
            UNIQUE(salarie_id, date_pointage))''',
        '''CREATE TABLE IF NOT EXISTS banque_history (
            id SERIAL PRIMARY KEY, 
            salarie_id INTEGER, 
            date_mouv DATE, 
            montant REAL, 
            motif TEXT, 
            type_mouv TEXT, 
            auteur TEXT)''',
        '''CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY, 
            password TEXT, 
            is_admin INTEGER DEFAULT 0, 
            is_active INTEGER DEFAULT 0)'''
    ]
    for q in queries:
        run_query(q, fetch="none")

init_db()

# --- GESTION UTILISATEURS ---
def create_user(username, password):
    res = run_query('SELECT count(*) as cnt FROM users', fetch="one")
    count = res['cnt']
    is_admin = 1 if count == 0 else 0
    is_active = 1 if count == 0 else 0
    msg = "Admin créé !" if is_admin else "Compte créé, attente validation."
    check = run_query('SELECT username FROM users WHERE username = %s', (username,), fetch="one")
    if check: return False, "Existe déjà."
    run_query('INSERT INTO users (username, password, is_admin, is_active) VALUES (%s, %s, %s, %s)', 
              (username, password, is_admin, is_active), fetch="none")
    return True, msg

def check_login(username, password):
    user = run_query('SELECT * FROM users WHERE username = %s AND password = %s', (username, password), fetch="one")
    if user:
        if user['is_active'] == 1: return "OK", bool(user['is_admin'])
        else: return "PENDING", False
    return "FAIL", False

def get_all_users():
    return run_query('SELECT username, is_admin, is_active FROM users', fetch="all")

def admin_actions_user(action, target, val=None):
    if action == "approve": run_query('UPDATE users SET is_active = 1 WHERE username = %s', (target,), fetch="none")
    elif action == "reject": run_query('DELETE FROM users WHERE username = %s', (target,), fetch="none")
    elif action == "reset": run_query('UPDATE users SET password = %s WHERE username = %s', (val, target), fetch="none")
    elif action == "promote": run_query('UPDATE users SET is_admin = 1 WHERE username = %s', (target,), fetch="none")
    elif action == "demote": run_query('UPDATE users SET is_admin = 0 WHERE username = %s', (target,), fetch="none")
    elif action == "transfer":
        curr = st.session_state.username
        run_query('UPDATE users SET is_admin = 1 WHERE username = %s', (target,), fetch="none")
        run_query('UPDATE users SET is_admin = 0 WHERE username = %s', (curr,), fetch="none")

# --- BACKUP SYSTEM (JSON) ---
def create_backup_json():
    data = {
        "salaries": run_query("SELECT * FROM salaries", fetch="all"),
        "pointages": run_query("SELECT * FROM pointages", fetch="all"),
        "banque_history": run_query("SELECT * FROM banque_history", fetch="all"),
        "users": run_query("SELECT * FROM users", fetch="all")
    }
    return json.dumps(data, indent=4, default=str)

def restore_backup_json(json_file):
    try:
        data = json.load(json_file)
        run_query("TRUNCATE pointages, banque_history, salaries, users RESTART IDENTITY", fetch="none")
        for u in data.get('users', []):
            run_query("INSERT INTO users (username, password, is_admin, is_active) VALUES (%s,%s,%s,%s)", 
                      (u['username'], u['password'], u['is_admin'], u['is_active']), fetch="none")
        for s in data.get('salaries', []):
            run_query("INSERT INTO salaries (id, nom, mode_alternance, solde_banque, config_horaires) OVERRIDING SYSTEM VALUE VALUES (%s,%s,%s,%s,%s)",
                      (s['id'], s['nom'], s['mode_alternance'], s['solde_banque'], s['config_horaires']), fetch="none")
        for p in data.get('pointages', []):
            run_query("INSERT INTO pointages (salarie_id, date_pointage, m_start, m_end, a_start, a_end, statut, comment) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                      (p['salarie_id'], p['date_pointage'], p['m_start'], p['m_end'], p['a_start'], p['a_end'], p['statut'], p['comment']), fetch="none")
        for b in data.get('banque_history', []):
            run_query("INSERT INTO banque_history (salarie_id, date_mouv, montant, motif, type_mouv, auteur) VALUES (%s,%s,%s,%s,%s,%s)",
                      (b['salarie_id'], b['date_mouv'], b['montant'], b['motif'], b['type_mouv'], b['auteur']), fetch="none")
        return True
    except Exception as e:
        st.error(f"Erreur restauration: {e}")
        return False

# --- UI LOGIN ---
if not st.session_state.logged_in:
    st.title("☁️ Connexion Supabase")
    with st.expander("📤 RESTAURER UNE SAUVEGARDE (JSON)", expanded=False):
        st.warning("Ceci écrasera toutes les données en ligne !")
        up_json = st.file_uploader("Fichier backup.json", type=['json'])
        if up_json:
            if st.button("⚠️ CONFIRMER RESTAURATION"):
                if restore_backup_json(up_json): st.success("Restauré ! Reconnectez-vous.")
    t1, t2 = st.tabs(["Connexion", "Créer Compte"])
    with t1:
        with st.form("log"):
            u = st.text_input("Identifiant")
            p = st.text_input("Mot de passe", type="password")
            if st.form_submit_button("Se connecter"):
                s, adm = check_login(u, p)
                if s == "OK":
                    st.session_state.logged_in=True
                    st.session_state.username=u
                    st.session_state.is_admin=adm
                    st.rerun()
                elif s == "PENDING": st.warning("Attente validation admin.")
                else: st.error("Erreur.")
    with t2:
        st.caption("Premier compte = Admin.")
        with st.form("sign"):
            nu = st.text_input("ID")
            np = st.text_input("MDP", type="password")
            if st.form_submit_button("Créer"):
                ok, m = create_user(nu, np)
                if ok: st.success(m)
                else: st.error(m)
    st.stop()

# =======================================================
# APP PRINCIPALE
# =======================================================

with st.sidebar:
    role = "Admin" if st.session_state.is_admin else "User"
    st.write(f"👤 **{st.session_state.username}** ({role})")
    
    st.markdown("### ☁️ Cloud Backup")
    json_data = create_backup_json()
    st.download_button("⬇️ TÉLÉCHARGER SAUVEGARDE (JSON)", json_data, f"Backup_{date.today()}.json", "application/json")
    
    st.markdown("---")
    if st.session_state.is_admin:
        st.header("🛠️ Admin")
        users = get_all_users()
        pending = [u['username'] for u in users if u['is_active'] == 0]
        active = [u['username'] for u in users if u['is_active'] == 1 and u['username'] != st.session_state.username]
        if pending:
            st.error(f"{len(pending)} demande(s)")
            tp = st.selectbox("Valider", pending)
            c1,c2 = st.columns(2)
            if c1.button("✅"): admin_actions_user("approve", tp); st.rerun()
            if c2.button("❌"): admin_actions_user("reject", tp); st.rerun()
        with st.expander("Gestion"):
            if active:
                tu = st.selectbox("Cible", active)
                act = st.selectbox("Action", ["Reset MDP", "Supprimer", "Co-Admin", "Donner droits", "Rétrograder"])
                if act == "Reset MDP":
                    np = st.text_input("New Pass", type="password")
                    if st.button("OK"): admin_actions_user("reset", tu, np); st.success("Fait.")
                elif act == "Supprimer":
                    if st.button("Confirm"): admin_actions_user("reject", tu); st.rerun()
                elif act == "Co-Admin":
                    if st.button("Promouvoir"): admin_actions_user("promote", tu); st.rerun()
                elif act == "Donner droits":
                    if st.button("Transférer"): admin_actions_user("transfer", tu); st.session_state.is_admin=False; st.rerun()
                elif act == "Rétrograder":
                    if st.button("Enlever Admin"): admin_actions_user("demote", tu); st.rerun()

    if st.button("Déconnexion", type="secondary"):
        st.session_state.logged_in = False; st.rerun()

# --- UTILS ---
def str_to_time(time_str):
    if not time_str: return None
    try: return dt.strptime(time_str, "%H:%M").time()
    except: return None

def time_to_str(t_obj):
    if t_obj: return t_obj.strftime("%H:%M")
    return None

def calc_duree_journee(m_s, m_e, a_s, a_e):
    total = 0.0
    def diff(s, e):
        if s and e:
            try:
                d1 = dt.strptime(str(s)[:5], "%H:%M")
                d2 = dt.strptime(str(e)[:5], "%H:%M")
                return max(0.0, (d2 - d1).total_seconds() / 3600)
            except: return 0.0
        return 0.0
    total += diff(m_s, m_e)
    total += diff(a_s, a_e)
    return total

def has_ticket_resto(row):
    if row['statut'] != 'Normal': return False
    
    # Calcul durée matin
    m_dur = calc_duree_journee(row.get('m_start'), row.get('m_end'), None, None)
    # Calcul durée aprem
    a_dur = calc_duree_journee(None, None, row.get('a_start'), row.get('a_end'))
    
    # Règle Stricte : Doit avoir travaillé > 0h le matin ET > 0h l'aprem
    return True if (m_dur > 0 and a_dur > 0) else False

def is_even_week(d): return d.isocalendar()[1] % 2 == 0

def get_config_for_day(emp_json, d_obj):
    if not emp_json: return None, None, None, None
    config = json.loads(emp_json)
    key = 'paire' if is_even_week(d_obj) else 'impaire'
    if key not in config: key = 'paire'
    d = config[key][d_obj.weekday()]
    return d['ms'], d['me'], d['as'], d['ae']

# --- DB FUNCTIONS ADAPTED TO POSTGRES ---
def db_upsert_salarie(id_s, nom, mode, sched):
    j = json.dumps(sched)
    chk = run_query("SELECT id FROM salaries WHERE nom=%s", (nom,), fetch="one")
    if chk and (id_s is None or chk['id'] != id_s):
        return False, "Nom déjà existant."
    if id_s is None: run_query('INSERT INTO salaries (nom, mode_alternance, config_horaires) VALUES (%s,%s,%s)', (nom, mode, j), fetch="none")
    else: run_query('UPDATE salaries SET nom=%s, mode_alternance=%s, config_horaires=%s WHERE id=%s', (nom, mode, j, id_s), fetch="none")
    return True, "Sauvegardé."

def db_save_pointage(s_id, d_obj, ms, me, ads, ae, stat, cmt):
    if isinstance(d_obj, str):
        try: d_iso = dt.strptime(d_obj, "%d/%m/%Y").strftime("%Y-%m-%d")
        except: d_iso = d_obj 
    else: d_iso = d_obj.strftime("%Y-%m-%d")
    run_query('''
        INSERT INTO pointages (salarie_id, date_pointage, m_start, m_end, a_start, a_end, statut, comment) 
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (salarie_id, date_pointage) 
        DO UPDATE SET m_start=EXCLUDED.m_start, m_end=EXCLUDED.m_end, a_start=EXCLUDED.a_start, a_end=EXCLUDED.a_end, statut=EXCLUDED.statut, comment=EXCLUDED.comment
    ''', (s_id, d_iso, ms, me, ads, ae, stat, cmt), fetch="none")

def db_update_banque(s_id, montant, motif, type_mouv="Manuel"):
    aut = st.session_state.username
    td = date.today().strftime("%Y-%m-%d")
    run_query('INSERT INTO banque_history (salarie_id, date_mouv, montant, motif, type_mouv, auteur) VALUES (%s,%s,%s,%s,%s,%s)',
              (s_id, td, montant, motif, type_mouv, aut), fetch="none")
    run_query('UPDATE salaries SET solde_banque = solde_banque + %s WHERE id = %s', (montant, s_id), fetch="none")

# --- CALCS ---
def calculate_stats(salarie_id, year, month, config_horaires):
    s, e = f"{year}-{month:02d}-01", f"{year}-{month:02d}-31"
    rows = run_query('SELECT * FROM pointages WHERE salarie_id=%s AND date_pointage BETWEEN %s AND %s', (salarie_id, s, e), fetch="all")
    db_pts = {str(r['date_pointage']): dict(r) for r in rows} if rows else {}
    
    pat = f"%HS Mois {month}/{year}%"
    row_b = run_query('SELECT SUM(montant) as total FROM banque_history WHERE salarie_id=%s AND motif LIKE %s', (salarie_id, pat), fetch="one")
    banked = row_b['total'] if row_b and row_b['total'] else 0.0

    _, last = calendar.monthrange(year, month)
    days = [date(year, month, d) for d in range(1, last+1)]
    feries = JoursFeries.for_year(year)
    
    weekly_hours = {}
    total_real, total_theo = 0.0, 0.0
    nb_conge, nb_maladie, nb_abs, total_tr = 0, 0, 0, 0
    details_rows = []

    for d in days:
        d_iso = d.strftime("%Y-%m-%d")
        row = db_pts.get(d_iso, {})
        stat = row.get('statut', 'Normal')
        rms, rme = row.get('m_start'), row.get('m_end')
        ras, rae = row.get('a_start'), row.get('a_end')
        tms, tme, tas, tae = get_config_for_day(config_horaires, d)
        
        if d_iso not in db_pts: rms=rme=ras=rae=None; stat="Normal"
        
        h_real = calc_duree_journee(rms, rme, ras, rae)
        h_theo = calc_duree_journee(tms, tme, tas, tae)
        
        if stat == "Congé": nb_conge += 1
        elif stat == "Arrêt Maladie": nb_maladie += 1
        elif stat == "Absence Injustifiée": nb_abs += 1
        
        # CHECK TR RENFORCÉ (Durée > 0 matin ET durée > 0 aprem)
        row_sim = {'statut': stat, 'm_start': rms, 'm_end': rme, 'a_start': ras, 'a_end': rae}
        if has_ticket_resto(row_sim): total_tr += 1

        if stat != "Normal" and stat != "Récupération": h_real_bank = h_theo
        elif stat == "Récupération": h_real_bank = 0.0
        else: h_real_bank = h_real
        
        total_real += h_real_bank
        total_theo += h_theo
        wn = d.isocalendar()[1]
        if wn not in weekly_hours: weekly_hours[wn] = 0.0
        weekly_hours[wn] += h_real
        details_rows.append({"Date": d.strftime("%d/%m/%Y"), "Jour": DAYS_FR[d.weekday()], "Statut": stat, "Matin": f"{rms}-{rme}" if rms else "", "Aprem": f"{ras}-{rae}" if ras else "", "Heures": h_real})

    hs_25, hs_50, gen_hs = 0.0, 0.0, 0.0
    for w, h in weekly_hours.items():
        if h > 35:
            surplus = h - 35
            gen_hs += surplus
            hs_25 += min(surplus, 8)
            hs_50 += max(0, surplus - 8)
    
    return {"total_real": total_real, "nb_conge": nb_conge, "nb_maladie": nb_maladie, "nb_abs": nb_abs, "total_tr": total_tr, "gen_hs_total": gen_hs, "hs_25": hs_25, "hs_50": hs_50, "banked": banked, "hs_payable": max(0, gen_hs - banked), "delta_bank": total_real - total_theo, "details": details_rows}

# --- PDF ---
def create_pdf_releve(nom, per, st):
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4)
    el = []
    styles = getSampleStyleSheet()
    el.append(Paragraph(f"Relevé: {nom} - {per}", styles['Heading1'])); el.append(Spacer(1, 0.5*cm))
    d = [["Heures Trav.", f"{st['total_real']:.2f}", "TR", f"{st['total_tr']}"],
         ["HS Total", f"{st['gen_hs_total']:.2f}", "Congé", f"{st['nb_conge']}"],
         ["Reste Payer", f"{st['hs_payable']:.2f}", "Maladie", f"{st['nb_maladie']}"]]
    t = Table(d, colWidths=[4*cm,3*cm,4*cm,3*cm])
    t.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.black), ('BACKGROUND', (0,0), (-1,-1), colors.whitesmoke)]))
    el.append(t); el.append(Spacer(1, 0.5*cm))
    det = [["Date","Jour","Statut","Matin","Aprem","Total"]]
    for r in st['details']: det.append([r['Date'],r['Jour'],r['Statut'],r['Matin'],r['Aprem'],f"{r['Heures']:.2f}"])
    t2 = Table(det, colWidths=[2.5*cm,3*cm,3.5*cm,3*cm,3*cm,2*cm])
    t2.setStyle(TableStyle([('GRID',(0,0),(-1,-1),0.5,colors.grey)]))
    el.append(t2)
    doc.build(el)
    buf.seek(0)
    return buf

# --- SIDEBAR CONFIG ---
def render_week_inputs_simple(prefix, default_data):
    if st.button("⚡ Remplir Formulaire", key=f"btn_{prefix}"):
        for i in range(5): 
            st.session_state[f"{prefix}_{i}_ms"] = str_to_time(STD_MS)
            st.session_state[f"{prefix}_{i}_me"] = str_to_time(STD_ME)
            st.session_state[f"{prefix}_{i}_as"] = str_to_time(STD_AS)
            st.session_state[f"{prefix}_{i}_ae"] = str_to_time(STD_AE)
        st.rerun()
    new_data = []
    for i, day in enumerate(DAYS_FR):
        c1, c2, c3, c4 = st.columns(4)
        d = default_data[i]
        def g(k, v): 
            key=f"{prefix}_{i}_{k}"
            if key not in st.session_state: st.session_state[key] = str_to_time(v)
            return key
        ms = c1.time_input(f"{day[:3]} M", key=g("ms", d['ms']), label_visibility="collapsed")
        me = c2.time_input("M", key=g("me", d['me']), label_visibility="collapsed")
        ads = c3.time_input("A", key=g("as", d['as']), label_visibility="collapsed")
        ae = c4.time_input("A", key=g("ae", d['ae']), label_visibility="collapsed")
        new_data.append({'ms': time_to_str(ms), 'me': time_to_str(me), 'as': time_to_str(ads), 'ae': time_to_str(ae)})
    return new_data

with st.sidebar:
    st.header("⚙️ Salarié")
    mode = st.radio("Mode", ["Nouveau", "Modifier"], horizontal=True)
    f_nom, f_alt, f_sch, f_id = "", False, get_default_schedule(), None
    
    if mode == "Modifier":
        emps = run_query("SELECT * FROM salaries", fetch="all")
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
        
    if st.button("💾 SAUVEGARDER"):
        if n_in:
            ok, m = db_upsert_salarie(f_id, n_in, 1 if use_alt else 0, {'paire':fp, 'impaire':fi})
            if ok: st.success(m); st.rerun()
            else: st.error(m)

# --- MAIN ---
st.title("🗓️ Planning Cloud")
employees = run_query("SELECT * FROM salaries", fetch="all")

if not employees:
    st.warning("Créez un salarié.")
else:
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1.5])
    emp_map = {e['nom']: e for e in employees}
    try: curr_emp = emp_map[c1.selectbox("Salarié", list(emp_map.keys()))]
    except: curr_emp = list(emp_map.values())[0]
    
    today = date.today()
    yr = c2.number_input("Année", 2024, 2030, today.year)
    mo = c3.selectbox("Mois", range(1, 13), index=today.month-1, format_func=lambda x: calendar.month_name[x])
    
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
            out.seek(0)
            st.download_button("⬇️", out, f"Paie_Global_{mo}_{yr}.xlsx")
        
        if c_b2.button("📄 PDF"):
            pdf = create_pdf_releve(curr_emp['nom'], f"{mo}/{yr}", stats)
            st.download_button("⬇️", pdf, f"Releve_{curr_emp['nom']}.pdf", "application/pdf")

    # TABLEAU
    s, e = f"{yr}-{mo:02d}-01", f"{yr}-{mo:02d}-31"
    rows = run_query('SELECT * FROM pointages WHERE salarie_id=%s AND date_pointage BETWEEN %s AND %s', (curr_emp['id'], s, e), fetch="all")
    db_pts = {str(r['date_pointage']): dict(r) for r in rows} if rows else {}
    
    days = [date(yr, mo, d) for d in range(1, calendar.monthrange(yr, mo)[1]+1)]
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
        // Jaune si Normal et Matin vide/zero
        if (params.data.Type === 'Normal' && (!params.data['Matin Début'] || params.data['Matin Début'] === '00:00' || params.data['Matin Début'] === 'None')) {
             return {'background-color': '#fff9c4'};
        }
        if (params.data.is_ferie === 1 || params.data.is_sun === 1) return {'background-color': '#e0e0e0'};
        if (params.data.Type === 'Arrêt Maladie') return {'background-color': '#ffb3b3'};
        if (params.data.Type === 'Congé') return {'background-color': '#b3d9ff'};
        if (params.data.Type === 'Absence Injustifiée') return {'background-color': '#ff4d4d', 'color': 'white'};
        return {'background-color': 'white'};
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
    col_u, col_f = st.columns([1, 3])
    with col_f:
        if st.button("✨ Remplir vides"):
            cnt = 0
            for d in days:
                iso = d.strftime("%Y-%m-%d")
                if iso not in db_pts:
                    ms, me, ads, ae = get_config_for_day(curr_emp['config_horaires'], d)
                    stat = "Normal"
                    if feries.get(d): stat = "Normal"
                    elif d.weekday() == 6: ms=me=ads=ae=None
                    db_save_pointage(curr_emp['id'], iso, ms, me, ads, ae, stat, "")
                    cnt += 1
            st.success(f"{cnt} jours."); st.rerun()

    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.subheader("Absences")
        st.write(f"Congés: **{stats['nb_conge']}**")
        st.write(f"Maladie: **{stats['nb_maladie']}**")
    with c2:
        st.subheader("Paie")
        st.write(f"HS Tot: **{stats['gen_hs_total']:.2f}h**")
        st.write(f"Banked: **-{stats['banked']:.2f}h**")
        st.metric("Reste", f"{stats['hs_payable']:.2f} h")
    with c3:
        st.subheader("Transfert")
        with st.form("trf"):
            amt = st.number_input("Heures", max_value=float(stats['hs_payable']))
            if st.form_submit_button("Verser"):
                db_update_banque(curr_emp['id'], amt, f"Transf HS {mo}/{yr}", "Auto")
                st.rerun()
    with c4:
        st.subheader("Banque")
        st.metric("Solde", f"{curr_emp['solde_banque']:.2f} h")
        with st.expander("Correction"):
            with st.form("adj"):
                v = st.number_input("+/-"); m = st.text_input("Motif")
                if st.form_submit_button("OK"): db_update_banque(curr_emp['id'], v, m); st.rerun()

    st.markdown("---")
    st.caption("Historique Banque")
    rh = run_query('SELECT * FROM banque_history WHERE salarie_id=%s ORDER BY id DESC', (curr_emp['id'],), fetch="all")
    if rh: st.dataframe(pd.DataFrame([dict(r) for r in rh]), use_container_width=True)