import streamlit as st
import pandas as pd
import sqlite3
import datetime
import json
from datetime import date, datetime as dt
import calendar
import io
import os
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm

# --- CONFIGURATION ---
st.set_page_config(page_title="Paie & Planning", layout="wide", page_icon="🔐")

try:
    from jours_feries_france import JoursFeries
except ImportError:
    st.error("Manque : pip install jours-feries-france")
    st.stop()

DAYS_FR = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
OPTIONS_STATUT = ["Normal", "Congé", "Arrêt Maladie", "Absence Injustifiée", "Récupération"]
DB_FILE = 'gestion_heures.db'

STD_MS, STD_ME = "08:30", "12:00"
STD_AS, STD_AE = "14:00", "17:30"

# --- SESSION STATE ---
if 'undo_stack' not in st.session_state: st.session_state.undo_stack = []
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'username' not in st.session_state: st.session_state.username = ""
if 'is_admin' not in st.session_state: st.session_state.is_admin = False
if 'curr_emp_id' not in st.session_state: st.session_state['curr_emp_id'] = None

# --- BASE DE DONNEES ---
def get_db_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    conn.execute('''CREATE TABLE IF NOT EXISTS salaries (
        id INTEGER PRIMARY KEY AUTOINCREMENT, nom TEXT NOT NULL, 
        mode_alternance INTEGER DEFAULT 0, solde_banque REAL DEFAULT 0, config_horaires TEXT)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS pointages (
        id INTEGER PRIMARY KEY AUTOINCREMENT, salarie_id INTEGER, date_pointage DATE, 
        m_start TEXT, m_end TEXT, a_start TEXT, a_end TEXT, statut TEXT DEFAULT 'Normal',
        comment TEXT,
        UNIQUE(salarie_id, date_pointage))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS banque_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, salarie_id INTEGER, 
        date_mouv DATE, montant REAL, motif TEXT, type_mouv TEXT, auteur TEXT)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, password TEXT, is_admin INTEGER DEFAULT 0, is_active INTEGER DEFAULT 0)''')
    conn.commit()
    conn.close()

init_db()

# --- GESTION UTILISATEURS ---
def create_user(username, password):
    conn = get_db_connection()
    try:
        count = conn.execute('SELECT count(*) FROM users').fetchone()[0]
        is_admin = 1 if count == 0 else 0
        is_active = 1 if count == 0 else 0
        msg = "Compte Admin créé !" if is_admin else "Compte créé, en attente de validation."
        conn.execute('INSERT INTO users (username, password, is_admin, is_active) VALUES (?, ?, ?, ?)', 
                     (username, password, is_admin, is_active))
        conn.commit()
        return True, msg
    except sqlite3.IntegrityError:
        return False, "Cet identifiant existe déjà."
    finally: conn.close()

def check_login(username, password):
    conn = get_db_connection()
    user = conn.execute('SELECT * FROM users WHERE username = ? AND password = ?', (username, password)).fetchone()
    conn.close()
    if user:
        if user['is_active'] == 1: return "OK", bool(user['is_admin'])
        else: return "PENDING", False
    return "FAIL", False

def get_all_users():
    conn = get_db_connection()
    users = conn.execute('SELECT username, is_admin, is_active FROM users').fetchall()
    conn.close()
    return users

def admin_actions_user(action, target_user, new_pass=None):
    conn = get_db_connection()
    if action == "approve":
        conn.execute('UPDATE users SET is_active = 1 WHERE username = ?', (target_user,))
    elif action == "reject" or action == "delete":
        conn.execute('DELETE FROM users WHERE username = ?', (target_user,))
    elif action == "reset_pass":
        conn.execute('UPDATE users SET password = ? WHERE username = ?', (new_pass, target_user))
    elif action == "promote":
        conn.execute('UPDATE users SET is_admin = 1 WHERE username = ?', (target_user,))
    elif action == "demote":
        conn.execute('UPDATE users SET is_admin = 0 WHERE username = ?', (target_user,))
    elif action == "transfer":
        current = st.session_state.username
        conn.execute('UPDATE users SET is_admin = 1 WHERE username = ?', (target_user,))
        conn.execute('UPDATE users SET is_admin = 0 WHERE username = ?', (current,))
    conn.commit()
    conn.close()

# --- UI CONNEXION ---
if not st.session_state.logged_in:
    st.title("🔐 Connexion (Mode Sécurisé)")
    with st.expander("📤 RESTAURER UNE SAUVEGARDE (.db)", expanded=True):
        uploaded_db = st.file_uploader("Charger un fichier .db", type=['db', 'sqlite'])
        if uploaded_db is not None:
            with open(DB_FILE, "wb") as f: f.write(uploaded_db.getbuffer())
            st.success("Données restaurées ! Connectez-vous.")

    t1, t2 = st.tabs(["Connexion", "Créer un compte"])
    with t1:
        with st.form("login_form"):
            u = st.text_input("Identifiant")
            p = st.text_input("Mot de passe", type="password")
            if st.form_submit_button("Se connecter"):
                status, is_adm = check_login(u, p)
                if status == "OK":
                    st.session_state.logged_in = True
                    st.session_state.username = u
                    st.session_state.is_admin = is_adm
                    st.rerun()
                elif status == "PENDING": st.warning("Compte en attente de validation.")
                else: st.error("Erreur d'identification.")
    with t2:
        st.info("Le premier utilisateur créé devient Administrateur.")
        with st.form("signup_form"):
            nu = st.text_input("Nouvel Identifiant")
            np = st.text_input("Nouveau Mot de passe", type="password")
            if st.form_submit_button("Créer"):
                if nu and np:
                    ok, m = create_user(nu, np)
                    if ok: st.success(m)
                    else: st.error(m)
                else: st.warning("Champs vides.")
    st.stop()

# =======================================================
# APPLICATION
# =======================================================

with st.sidebar:
    role = "Administrateur" if st.session_state.is_admin else "Utilisateur"
    st.write(f"👤 **{st.session_state.username}** ({role})")
    
    st.markdown("### 💾 Sauvegarde")
    with open(DB_FILE, "rb") as f:
        st.download_button("⬇️ Télécharger Backup (.db)", f, f"Backup_{date.today()}.db", "application/x-sqlite3")
    
    st.markdown("---")
    if st.session_state.is_admin:
        st.header("🛠️ Admin")
        users = get_all_users()
        pending = [u['username'] for u in users if u['is_active'] == 0]
        active = [u['username'] for u in users if u['is_active'] == 1 and u['username'] != st.session_state.username]
        
        if pending:
            st.error(f"{len(pending)} demande(s) !")
            target_p = st.selectbox("Valider", pending)
            c1, c2 = st.columns(2)
            if c1.button("✅"): admin_actions_user("approve", target_p); st.rerun()
            if c2.button("❌"): admin_actions_user("reject", target_p); st.rerun()
            
        with st.expander("Gérer Utilisateurs"):
            if active:
                target_u = st.selectbox("Utilisateur", active)
                action = st.selectbox("Action", ["Réinitialiser MDP", "Supprimer", "Nommer Co-Admin", "Transférer mes droits", "Rétrograder"])
                if action == "Réinitialiser MDP":
                    new_p = st.text_input("Nouveau MDP", type="password")
                    if st.button("Valider"): admin_actions_user("reset_pass", target_u, new_p); st.success("Fait.")
                elif action == "Supprimer":
                    if st.button("Confirmer"): admin_actions_user("delete", target_u); st.rerun()
                elif action == "Nommer Co-Admin":
                    if st.button("Promouvoir"): admin_actions_user("promote", target_u); st.rerun()
                elif action == "Transférer mes droits":
                    if st.button("Transférer"): 
                        admin_actions_user("transfer", target_u)
                        st.session_state.is_admin = False
                        st.rerun()
                elif action == "Rétrograder":
                    if st.button("Rétrograder"): admin_actions_user("demote", target_u); st.rerun()
            else: st.caption("Aucun autre utilisateur.")
        st.markdown("---")

    if st.button("Déconnexion", type="secondary"):
        st.session_state.logged_in = False
        st.rerun()

# --- UTILITAIRES METIER ---
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
    morning_ok = row.get('m_start') and row.get('m_end')
    afternoon_ok = row.get('a_start') and row.get('a_end')
    return True if (morning_ok and afternoon_ok) else False

def is_even_week(d): return d.isocalendar()[1] % 2 == 0

def get_config_for_day(emp_json, d_obj):
    if not emp_json: return None, None, None, None
    config = json.loads(emp_json)
    key = 'paire' if is_even_week(d_obj) else 'impaire'
    if key not in config: key = 'paire'
    d = config[key][d_obj.weekday()]
    return d['ms'], d['me'], d['as'], d['ae']

def calculate_stats(salarie_id, year, month, config_horaires):
    db_pts = db_get_pointages(salarie_id, year, month)
    _, last = calendar.monthrange(year, month)
    days = [date(year, month, d) for d in range(1, last+1)]
    
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
        if has_ticket_resto({'statut': stat, 'm_start': rms, 'm_end': rme, 'a_start': ras, 'a_end': rae}): total_tr += 1

        if stat != "Normal" and stat != "Récupération": h_real_bank = h_theo
        elif stat == "Récupération": h_real_bank = 0.0
        else: h_real_bank = h_real
        
        total_real += h_real_bank
        total_theo += h_theo
        
        wn = d.isocalendar()[1]
        if wn not in weekly_hours: weekly_hours[wn] = 0.0
        weekly_hours[wn] += h_real
        
        details_rows.append({
            "Date": d.strftime("%d/%m/%Y"), "Jour": DAYS_FR[d.weekday()], "Statut": stat,
            "Matin": f"{rms}-{rme}" if rms else "", "Aprem": f"{ras}-{rae}" if ras else "",
            "Heures": h_real
        })

    hs_25, hs_50, gen_hs_total = 0.0, 0.0, 0.0
    for w, h_sem in weekly_hours.items():
        if h_sem > 35:
            surplus = h_sem - 35
            gen_hs_total += surplus
            hs_25 += min(surplus, 8)
            hs_50 += max(0, surplus - 8)
            
    month_label = f"{month}/{year}"
    banked = db_get_transferred_hs_for_month(salarie_id, month_label)
    hs_payable = max(0.0, gen_hs_total - banked)
    delta_bank = total_real - total_theo
    
    return {
        "nb_conge": nb_conge, "nb_maladie": nb_maladie, "nb_abs": nb_abs, "total_tr": total_tr, 
        "total_real": total_real, "total_theo": total_theo,
        "hs_25": hs_25, "hs_50": hs_50, "gen_hs_total": gen_hs_total,
        "banked": banked, "hs_payable": hs_payable, "delta_bank": delta_bank,
        "details": details_rows
    }

def create_pdf_releve(nom_salarie, mois_annee, stats):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    elements = []
    styles = getSampleStyleSheet()
    elements.append(Paragraph(f"Relevé: {nom_salarie} - {mois_annee}", styles['Heading1']))
    elements.append(Spacer(1, 0.5 * cm))
    data_recap = [
        ["Heures Travaillées", f"{stats['total_real']:.2f}", "Tickets Resto", f"{stats['total_tr']}"],
        ["HS Totales", f"{stats['gen_hs_total']:.2f}", "Congés", f"{stats['nb_conge']}"],
        ["Reste à Payer", f"{stats['hs_payable']:.2f}", "Maladie", f"{stats['nb_maladie']}"]
    ]
    t_recap = Table(data_recap, colWidths=[4*cm, 3*cm, 4*cm, 3*cm])
    t_recap.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 1, colors.black), ('BACKGROUND', (0,0), (-1,-1), colors.whitesmoke)]))
    elements.append(t_recap)
    elements.append(Spacer(1, 0.5 * cm))
    data_detail = [["Date", "Jour", "Statut", "Matin", "Aprem", "Total"]]
    for r in stats['details']:
        data_detail.append([r['Date'], r['Jour'], r['Statut'], r['Matin'], r['Aprem'], f"{r['Heures']:.2f}"])
    t_detail = Table(data_detail, colWidths=[2.5*cm, 3*cm, 3.5*cm, 3*cm, 3*cm, 2*cm])
    t_detail.setStyle(TableStyle([('GRID', (0,0), (-1,-1), 0.5, colors.grey), ('FONTSIZE', (0,0), (-1,-1), 8)]))
    elements.append(t_detail)
    doc.build(elements)
    buffer.seek(0)
    return buffer

# --- DB CRUD ---
def db_upsert_salarie(id_s, nom, mode, sched):
    conn = get_db_connection()
    
    # CHECK DOUBLON
    existing = conn.execute('SELECT id FROM salaries WHERE nom = ?', (nom,)).fetchone()
    if existing and (id_s is None or existing['id'] != id_s):
        conn.close()
        return False, "Erreur: Un salarié avec ce nom existe déjà."
    
    j = json.dumps(sched)
    if id_s is None: conn.execute('INSERT INTO salaries (nom, mode_alternance, config_horaires) VALUES (?,?,?)', (nom, mode, j))
    else: conn.execute('UPDATE salaries SET nom=?, mode_alternance=?, config_horaires=? WHERE id=?', (nom, mode, j, id_s))
    conn.commit(); conn.close()
    return True, "Sauvegardé."

def db_get_salaries():
    conn = get_db_connection(); res = [dict(r) for r in conn.execute('SELECT * FROM salaries').fetchall()]; conn.close(); return res

def db_save_pointage(s_id, d_obj, ms, me, ads, ae, stat, cmt):
    conn = get_db_connection()
    if isinstance(d_obj, str):
        try: d_iso = dt.strptime(d_obj, "%d/%m/%Y").strftime("%Y-%m-%d")
        except: d_iso = d_obj 
    else: d_iso = d_obj.strftime("%Y-%m-%d")
    conn.execute('INSERT OR REPLACE INTO pointages (salarie_id, date_pointage, m_start, m_end, a_start, a_end, statut, comment) VALUES (?,?,?,?,?,?,?,?)',
                 (s_id, d_iso, ms, me, ads, ae, stat, cmt))
    conn.commit(); conn.close()

def db_get_pointages(s_id, y, m):
    s, e = f"{y}-{m:02d}-01", f"{y}-{m:02d}-31"
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM pointages WHERE salarie_id=? AND date_pointage BETWEEN ? AND ?', (s_id, s, e)).fetchall()
    conn.close()
    return {r['date_pointage']: dict(r) for r in rows}

def db_update_banque(s_id, montant, motif, type_mouv="Manuel"):
    auteur = st.session_state.username
    conn = get_db_connection()
    today_str = date.today().strftime("%Y-%m-%d")
    conn.execute('INSERT INTO banque_history (salarie_id, date_mouv, montant, motif, type_mouv, auteur) VALUES (?,?,?,?,?,?)',
                 (s_id, today_str, montant, motif, type_mouv, auteur))
    conn.execute('UPDATE salaries SET solde_banque = solde_banque + ? WHERE id = ?', (montant, s_id))
    conn.commit(); conn.close()

def db_get_transferred_hs_for_month(s_id, month_label):
    conn = get_db_connection()
    pattern = f"%HS Mois {month_label}%"
    row = conn.execute('SELECT SUM(montant) as total FROM banque_history WHERE salarie_id=? AND motif LIKE ?', (s_id, pattern)).fetchone()
    conn.close()
    return row['total'] if row['total'] else 0.0

def db_get_banque_history(s_id):
    conn = get_db_connection()
    rows = conn.execute('SELECT * FROM banque_history WHERE salarie_id=? ORDER BY id DESC', (s_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def save_state_for_undo(s_id, year, month):
    current_data = db_get_pointages(s_id, year, month)
    st.session_state.undo_stack.append({'s_id': s_id, 'year': year, 'month': month, 'data': current_data})
    if len(st.session_state.undo_stack) > 5: st.session_state.undo_stack.pop(0)

def restore_last_state():
    if not st.session_state.undo_stack: return False
    last_state = st.session_state.undo_stack.pop()
    s_id, year, month = last_state['s_id'], last_state['year'], last_state['month']
    data = last_state['data']
    conn = get_db_connection()
    s_date, e_date = f"{year}-{month:02d}-01", f"{year}-{month:02d}-31"
    conn.execute("DELETE FROM pointages WHERE salarie_id=? AND date_pointage BETWEEN ? AND ?", (s_id, s_date, e_date))
    for d_str, row in data.items():
        conn.execute('INSERT INTO pointages (salarie_id, date_pointage, m_start, m_end, a_start, a_end, statut, comment) VALUES (?,?,?,?,?,?,?,?)', 
                        (s_id, d_str, row['m_start'], row['m_end'], row['a_start'], row['a_end'], row['statut'], row['comment']))
    conn.commit(); conn.close()
    return True

def get_default_schedule():
    std = {'ms': STD_MS, 'me': STD_ME, 'as': STD_AS, 'ae': STD_AE}
    empty = {'ms': None, 'me': None, 'as': None, 'ae': None}
    week = [std.copy() for _ in range(5)] + [empty.copy()] + [empty.copy()]
    return {'paire': week, 'impaire': week}

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
        ms = c1.time_input(f"{day[:3]} Mat", key=g("ms", d['ms']), label_visibility="collapsed")
        me = c2.time_input("M-F", key=g("me", d['me']), label_visibility="collapsed")
        ads = c3.time_input("Apr", key=g("as", d['as']), label_visibility="collapsed")
        ae = c4.time_input("A-F", key=g("ae", d['ae']), label_visibility="collapsed")
        new_data.append({'ms': time_to_str(ms), 'me': time_to_str(me), 'as': time_to_str(ads), 'ae': time_to_str(ae)})
    return new_data

# --- GESTION SALARIÉ SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Configuration Salarié")
    mode = st.radio("Mode", ["Nouveau Salarié", "Modifier Salarié"], horizontal=True)
    
    f_nom, f_alt, f_sch, f_id = "", False, get_default_schedule(), None

    if mode == "Modifier Salarié":
        emps = db_get_salaries()
        if emps:
            sel = st.selectbox("Choisir", [e['nom'] for e in emps])
            e_obj = next(e for e in emps if e['nom'] == sel)
            if st.session_state['curr_emp_id'] != e_obj['id']:
                for k in list(st.session_state.keys()): 
                    if k.startswith(("p_","i_","std_")): del st.session_state[k]
                st.session_state['curr_emp_id'] = e_obj['id']; st.rerun()
                
            f_id, f_nom, f_alt = e_obj['id'], e_obj['nom'], bool(e_obj['mode_alternance'])
            if e_obj['config_horaires']: f_sch = json.loads(e_obj['config_horaires'])
        else: st.warning("Liste vide.")
    else:
        if st.session_state['curr_emp_id'] is not None:
            st.session_state['curr_emp_id'] = None
            for k in list(st.session_state.keys()): 
                if k.startswith(("p_","i_","std_")): del st.session_state[k]
            st.rerun()

    st.markdown("---")
    st.caption("Définit les horaires par défaut pour l'import.")
    use_alt = st.checkbox("Horaires Alternés", value=f_alt)
    n_in = st.text_input("Nom Prénom", value=f_nom)
    
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
            ok, msg = db_upsert_salarie(f_id, n_in, 1 if use_alt else 0, {'paire':fp, 'impaire':fi})
            if ok: st.success(msg); st.rerun()
            else: st.error(msg)
        else: st.error("Nom requis.")

# --- MAIN CONTENT ---
st.title("🗓️ Paie & Planning")
employees = db_get_salaries()

if not employees:
    st.warning("👈 Créez votre premier salarié.")
else:
    c1, c2, c3, c4 = st.columns([2, 1, 1, 1.5])
    emp_map = {e['nom']: e for e in employees}
    try: curr_emp = emp_map[c1.selectbox("Salarié (Vue détaillée)", list(emp_map.keys()))]
    except: curr_emp = list(emp_map.values())[0]
    
    today = date.today()
    yr = c2.number_input("Année", 2024, 2030, today.year)
    mo = c3.selectbox("Mois", range(1, 13), index=today.month-1, format_func=lambda x: calendar.month_name[x])
    
    stats_curr = calculate_stats(curr_emp['id'], yr, mo, curr_emp['config_horaires'])

    with c4:
        st.write("")
        c_b1, c_b2 = st.columns(2)
        if c_b1.button("📥 EXCEL (Tous)"):
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine='openpyxl') as w:
                global_rows = []
                for emp in employees:
                    s = calculate_stats(emp['id'], yr, mo, emp['config_horaires'])
                    global_rows.append({
                        "Salarié": emp['nom'],
                        "H. Travail": s['total_real'],
                        "Congés": s['nb_conge'],
                        "Maladie": s['nb_maladie'],
                        "Abs. Inj.": s['nb_abs'],
                        "TR": s['total_tr'],
                        "HS 25%": s['hs_25'],
                        "HS 50%": s['hs_50'],
                        "Reste Payer": s['hs_payable'],
                        "Solde Bq": emp['solde_banque'] + s['delta_bank']
                    })
                pd.DataFrame(global_rows).to_excel(w, index=False, sheet_name="Synthèse Globale")
                for emp in employees:
                    s = calculate_stats(emp['id'], yr, mo, emp['config_horaires'])
                    df_det = pd.DataFrame(s['details'])
                    sheet_name = emp['nom'][:30].replace(":", "").replace("/", "")
                    df_det.to_excel(w, index=False, sheet_name=sheet_name)
            out.seek(0)
            st.download_button("⬇️ Excel", out, f"Paie_Global_{mo}_{yr}.xlsx")
        
        if c_b2.button("📄 PDF (Actuel)"):
            pdf_data = create_pdf_releve(curr_emp['nom'], f"{mo}/{yr}", stats_curr)
            st.download_button("⬇️ PDF", pdf_data, file_name=f"Releve_{curr_emp['nom']}_{mo}_{yr}.pdf", mime="application/pdf")

    # SAISIE
    st.markdown("### 🗓️ Saisie")
    col_undo, col_fill = st.columns([1, 3])
    with col_undo:
        cnt = len(st.session_state.undo_stack)
        if st.button(f"↩️ Annuler ({cnt})", disabled=cnt==0):
            if restore_last_state(): st.toast("Annulé !"); st.rerun()
    with col_fill:
        if st.button("✨ Remplir vides (Selon config)"):
            save_state_for_undo(curr_emp['id'], yr, mo)
            existing = db_get_pointages(curr_emp['id'], yr, mo)
            days = [date(yr, mo, d) for d in range(1, calendar.monthrange(yr, mo)[1]+1)]
            feries = JoursFeries.for_year(yr)
            cnt = 0
            for d in days:
                iso = d.strftime("%Y-%m-%d")
                if iso not in existing:
                    ms, me, ads, ae = get_config_for_day(curr_emp['config_horaires'], d)
                    stat = "Normal"
                    if feries.get(d): stat = "Normal"
                    elif d.weekday() == 6: ms=me=ads=ae=None
                    db_save_pointage(curr_emp['id'], d, ms, me, ads, ae, stat, "")
                    cnt += 1
            st.success(f"{cnt} jours."); st.rerun()

    # AGGRID
    db_pts = db_get_pointages(curr_emp['id'], yr, mo)
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
        if (params.data.is_ferie === 1 || params.data.is_sun === 1) return {'background-color': '#e0e0e0'};
        if (params.data.Type === 'Arrêt Maladie') return {'background-color': '#ffb3b3'};
        if (params.data.Type === 'Congé') return {'background-color': '#b3d9ff'};
        if (params.data.Type === 'Absence Injustifiée') return {'background-color': '#ff4d4d', 'color': 'white'};
        return {'background-color': 'white'};
    }""")
    gb.configure_grid_options(getRowStyle=jscode)
    grid_resp = AgGrid(df, gridOptions=gb.build(), height=500, allow_unsafe_jscode=True, theme='streamlit', update_mode=GridUpdateMode.VALUE_CHANGED)
    
    updated_df = pd.DataFrame(grid_resp['data'])
    if st.button("💾 SAUVEGARDER LES SAISIES", type="primary"):
        save_state_for_undo(curr_emp['id'], yr, mo)
        for i, r in updated_df.iterrows():
            def cl(v): return v if v and v != "None" and v != "" else None
            d_obj = dt.strptime(r['Date'], "%d/%m/%Y").strftime("%Y-%m-%d")
            db_save_pointage(curr_emp['id'], d_obj, cl(r['Matin Début']), cl(r['Matin Fin']), cl(r['Aprèm Début']), cl(r['Aprèm Fin']), r['Type'], r['Commentaire'])
        st.toast("Sauvegardé !", icon="✅"); st.rerun()

    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.subheader("🏖️ Absences")
        st.write(f"Congés: **{stats_curr['nb_conge']}**")
        st.write(f"Maladie: **{stats_curr['nb_maladie']}**")
        st.write(f"Injust.: **{stats_curr['nb_abs']}**")
    with c2:
        st.subheader("💰 Paie")
        st.write(f"HS Totales: **{stats_curr['gen_hs_total']:.2f}h**")
        st.write(f"En Banque: **-{stats_curr['banked']:.2f}h**")
        st.metric("À PAYER", f"{stats_curr['hs_payable']:.2f} h")
    with c3:
        st.subheader("🏦 Transfert Banque")
        with st.form("trf"):
            amt = st.number_input("Heures", max_value=float(stats_curr['hs_payable']))
            if st.form_submit_button("Verser"):
                db_update_banque(curr_emp['id'], amt, f"Transfert HS {mo}/{yr}", "Transfert HS")
                st.rerun()
    with c4:
        st.subheader("📊 Solde / TR")
        st.metric("SOLDE BANQUE", f"{curr_emp['solde_banque']:.2f} h")
        st.metric("🎟️ TR", f"{stats_curr['total_tr']}")
        with st.expander("Correction"):
            with st.form("adj"):
                v = st.number_input("+/-"); m = st.text_input("Motif")
                if st.form_submit_button("OK"): db_update_banque(curr_emp['id'], v, m, "Manuel"); st.rerun()

    st.markdown("---")
    st.subheader("📜 Historique Banque")
    h = db_get_banque_history(curr_emp['id'])
    if h: st.dataframe(pd.DataFrame(h)[['date_mouv', 'auteur', 'type_mouv', 'motif', 'montant']], use_container_width=True)