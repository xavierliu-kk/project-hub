# ==============================================================
# 企業專案管理系統 — 主程式 (app.py)
# 技術：Flask + PostgreSQL (psycopg2)
# 本機啟動：python app.py
# 雲端部署：gunicorn app:app
# ==============================================================

import os
from datetime import datetime, date, timedelta
from flask import (Flask, render_template, request, redirect,
                   url_for, flash, session, abort, g, jsonify)
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg
from psycopg.rows import dict_row
from psycopg import errors as pg_errors
from psycopg_pool import ConnectionPool
from functools import wraps
from translations import TRANSLATIONS

# ── App config ────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-only-change-in-production')

DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://localhost/project_hub')

# Connection pool：2 workers × 4 threads = 最多 8 條並發，預留 10 條
pool = ConnectionPool(
    DATABASE_URL,
    min_size=2,
    max_size=10,
    kwargs={'row_factory': dict_row},
    open=True,
)

DEPARTMENTS = ['會計','財務','投資人','財務規劃','法務','自動化','財務長']

# Emails that can access the manager dashboard
MANAGER_EMAILS = {'xavier.liu@kkday.com'}

PULSE_STATUS = {
    'new':     {'t_key': 'pulse_new',     'color': '#6b7280'},
    'process': {'t_key': 'pulse_process', 'color': '#f59e0b'},
    'done':    {'t_key': 'pulse_done',    'color': '#22c55e'},
}


# ── Jinja2 date filters ───────────────────────────────────────
@app.template_filter('datefmt')
def datefmt(dt):
    """YYYY-MM-DD"""
    if not dt:
        return ''
    return dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else str(dt)[:10]

@app.template_filter('dtfmt')
def dtfmt(dt):
    """YYYY-MM-DD HH:MM"""
    if not dt:
        return ''
    return dt.strftime('%Y-%m-%d %H:%M') if hasattr(dt, 'strftime') else str(dt)[:16].replace('T', ' ')

@app.template_filter('dtshort')
def dtshort(dt):
    """MM-DD HH:MM"""
    if not dt:
        return ''
    return dt.strftime('%m-%d %H:%M') if hasattr(dt, 'strftime') else str(dt)[5:16].replace('T', ' ')


# ── i18n helpers ──────────────────────────────────────────────

def _get_lang():
    return session.get('lang', 'zh')

def _t(key, **kwargs):
    """Backend translation helper (for flash messages)."""
    lang = _get_lang()
    msg = TRANSLATIONS.get(lang, TRANSLATIONS['zh']).get(key, key)
    if kwargs:
        try:
            msg = msg.format(**kwargs)
        except (KeyError, IndexError):
            pass
    return msg

@app.context_processor
def inject_i18n():
    lang = _get_lang()
    def t(key, fallback=None):
        return TRANSLATIONS.get(lang, TRANSLATIONS['zh']).get(key, fallback or key)
    return {'t': t, 'current_lang': lang}


@app.context_processor
def inject_manager_flag():
    if 'user_id' not in session:
        return {'current_user_is_manager': False}
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT email, is_manager FROM users WHERE id=%s', (session['user_id'],))
    u = c.fetchone()
    flag = bool(u and (u['email'] in MANAGER_EMAILS or u.get('is_manager')))
    return {'current_user_is_manager': flag}


# ── DB helpers ────────────────────────────────────────────────

def get_db():
    """每個 request 共用同一條連線，request 結束自動歸還 pool。"""
    if 'db' not in g:
        g.db = pool.getconn()
    return g.db


@app.teardown_appcontext
def close_db(error):
    conn = g.pop('db', None)
    if conn is not None:
        pool.putconn(conn)


def release_db(conn):
    pass  # 保留呼叫點不報錯，實際由 teardown 處理


def init_db():
    """建立所有資料表（若不存在）。"""
    conn = get_db()
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id         SERIAL PRIMARY KEY,
            name       TEXT    NOT NULL,
            email      TEXT    UNIQUE NOT NULL,
            password   TEXT    NOT NULL,
            department TEXT    NOT NULL,
            language   VARCHAR(5) DEFAULT 'zh',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add language column to existing tables (idempotent)
    c.execute('''
        ALTER TABLE users ADD COLUMN IF NOT EXISTS language VARCHAR(5) DEFAULT 'zh'
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id              SERIAL PRIMARY KEY,
            title           TEXT    NOT NULL,
            description     TEXT,
            creator_id      INTEGER NOT NULL REFERENCES users(id),
            assignee_id     INTEGER REFERENCES users(id),
            parent_id       INTEGER REFERENCES projects(id),
            launch_date     DATE,
            benefit         TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Add new columns to existing tables (idempotent)
    c.execute('ALTER TABLE projects ADD COLUMN IF NOT EXISTS launch_date DATE')
    c.execute('ALTER TABLE projects ADD COLUMN IF NOT EXISTS benefit TEXT')
    c.execute("ALTER TABLE projects ADD COLUMN IF NOT EXISTS priority VARCHAR(10) NOT NULL DEFAULT 'low'")

    c.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            id         SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            user_id    INTEGER NOT NULL REFERENCES users(id),
            content    TEXT    NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS project_pulse (
            id         SERIAL PRIMARY KEY,
            project_id INTEGER NOT NULL REFERENCES projects(id),
            user_id    INTEGER NOT NULL REFERENCES users(id),
            status     TEXT    NOT NULL,
            message    TEXT    NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS activity_log (
            id           SERIAL PRIMARY KEY,
            project_id   INTEGER NOT NULL REFERENCES projects(id),
            user_id      INTEGER NOT NULL REFERENCES users(id),
            action_type  VARCHAR(20) NOT NULL,
            action_label TEXT        NOT NULL,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    c.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS is_manager BOOLEAN DEFAULT FALSE')

    conn.commit()
    release_db(conn)


# ── Auth helpers ──────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            flash(_t('flash_login_required'), 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def get_current_user():
    if 'user_id' in session:
        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT * FROM users WHERE id = %s', (session['user_id'],))
        user = c.fetchone()
        release_db(conn)
        return user
    return None


# ══════════════════════════════════════════════════════════════
# 語言切換
# ══════════════════════════════════════════════════════════════

@app.route('/set-language', methods=['POST'])
def set_language():
    data = request.get_json() or {}
    lang = data.get('lang', 'zh')
    if lang not in ('zh', 'en', 'ja'):
        lang = 'zh'
    session['lang'] = lang
    # Persist to DB if logged in
    if 'user_id' in session:
        conn = get_db()
        c = conn.cursor()
        c.execute('UPDATE users SET language = %s WHERE id = %s', (lang, session['user_id']))
        conn.commit()
        release_db(conn)
    return jsonify(ok=True)


# ══════════════════════════════════════════════════════════════
# 使用者路由
# ══════════════════════════════════════════════════════════════

@app.route('/register', methods=['GET', 'POST'])
@login_required
def register():
    if request.method == 'POST':
        name       = request.form.get('name', '').strip()
        email      = request.form.get('email', '').strip()
        password   = request.form.get('password', '')
        confirm    = request.form.get('confirm', '')
        department = request.form.get('department', '').strip()

        if not name or not email or not password or not department:
            flash(_t('flash_fill_all'), 'danger')
            return render_template('register.html', departments=DEPARTMENTS)
        if department not in DEPARTMENTS:
            flash(_t('flash_invalid_dept'), 'danger')
            return render_template('register.html', departments=DEPARTMENTS)
        if password != confirm:
            flash(_t('flash_pw_mismatch'), 'danger')
            return render_template('register.html', departments=DEPARTMENTS)
        if len(password) < 6:
            flash(_t('flash_pw_short'), 'danger')
            return render_template('register.html', departments=DEPARTMENTS)

        hashed = generate_password_hash(password)
        conn = get_db()
        c = conn.cursor()
        try:
            c.execute(
                'INSERT INTO users (name, email, password, department) VALUES (%s, %s, %s, %s)',
                (name, email, hashed, department)
            )
            conn.commit()
            flash(_t('flash_register_success'), 'success')
            return redirect(url_for('login'))
        except pg_errors.UniqueViolation:
            conn.rollback()
            flash(_t('flash_email_exists'), 'danger')
        finally:
            release_db(conn)

    return render_template('register.html', departments=DEPARTMENTS)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if 'user_id' in session:
        return redirect(url_for('index'))

    if request.method == 'POST':
        email    = request.form.get('email', '').strip()
        password = request.form.get('password', '')

        if not email or not password:
            flash(_t('flash_fill_email_pw'), 'danger')
            return render_template('login.html')

        conn = get_db()
        c = conn.cursor()
        c.execute('SELECT * FROM users WHERE email = %s', (email,))
        user = c.fetchone()
        release_db(conn)

        if user and check_password_hash(user['password'], password):
            session['user_id']   = user['id']
            session['user_name'] = user['name']
            # Load user's language preference
            session['lang'] = user.get('language', 'zh') or 'zh'
            flash(_t('flash_welcome', name=user['name']), 'success')
            return redirect(url_for('index'))
        else:
            flash(_t('flash_login_error'), 'danger')

    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    flash(_t('flash_logout'), 'info')
    return redirect(url_for('login'))


@app.route('/change-password', methods=['POST'])
@login_required
def change_password():
    data = request.get_json()
    current_pw = (data.get('current_password') or '').strip()
    new_pw     = (data.get('new_password') or '').strip()

    if not current_pw or not new_pw:
        return jsonify(ok=False, error=_t('flash_pw_fill_all')), 400
    if len(new_pw) < 6:
        return jsonify(ok=False, error=_t('flash_pw_short2')), 400

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM users WHERE id = %s', (session['user_id'],))
    user = c.fetchone()

    if not check_password_hash(user['password'], current_pw):
        release_db(conn)
        return jsonify(ok=False, error=_t('flash_pw_wrong')), 400

    c.execute('UPDATE users SET password = %s WHERE id = %s',
              (generate_password_hash(new_pw), session['user_id']))
    conn.commit()
    release_db(conn)
    return jsonify(ok=True)


# ══════════════════════════════════════════════════════════════
# 首頁
# ══════════════════════════════════════════════════════════════

@app.route('/')
@login_required
def index():
    conn = get_db()
    c = conn.cursor()

    c.execute('''
        SELECT
            p.*,
            u.name       AS creator_name,
            u.department AS creator_department,
            ua.name      AS assignee_name,
            (SELECT COUNT(*) FROM projects sub WHERE sub.parent_id = p.id)  AS sub_count,
            (SELECT COUNT(*) FROM comments  cm  WHERE cm.project_id = p.id) AS comment_count,
            (SELECT pp.status FROM project_pulse pp
             WHERE pp.project_id = p.id ORDER BY pp.created_at DESC LIMIT 1) AS latest_pulse
        FROM projects p
        JOIN  users u  ON p.creator_id  = u.id
        LEFT JOIN users ua ON p.assignee_id = ua.id
        WHERE p.parent_id IS NULL
        ORDER BY p.created_at DESC
    ''')
    projects = c.fetchall()

    c.execute('''
        SELECT p.*, u.name AS creator_name, u.department AS creator_department,
               ua.name AS assignee_name,
               (SELECT COUNT(*) FROM comments cm WHERE cm.project_id = p.id) AS comment_count
        FROM projects p
        JOIN  users u  ON p.creator_id = u.id
        LEFT JOIN users ua ON p.assignee_id = ua.id
        ORDER BY p.created_at DESC LIMIT 5
    ''')
    recent_projects = c.fetchall()

    c.execute('''
        SELECT c.*, u.name AS user_name, u.department AS user_dept,
               p.title AS project_title, p.id AS project_id_ref
        FROM comments c
        JOIN users u    ON c.user_id    = u.id
        JOIN projects p ON c.project_id = p.id
        ORDER BY c.created_at DESC LIMIT 6
    ''')
    recent_comments = c.fetchall()

    c.execute('''
        SELECT pp.*, u.name AS user_name, u.department AS user_dept,
               p.title AS project_title, p.id AS project_id_ref
        FROM project_pulse pp
        JOIN users u    ON pp.user_id    = u.id
        JOIN projects p ON pp.project_id = p.id
        ORDER BY pp.created_at DESC LIMIT 6
    ''')
    recent_pulse = c.fetchall()

    c.execute('SELECT COUNT(*) FROM project_pulse')
    pulse_count = c.fetchone()['count']

    release_db(conn)

    current_user = get_current_user()
    return render_template(
        'index.html',
        projects=projects,
        recent_projects=recent_projects,
        recent_comments=recent_comments,
        recent_pulse=recent_pulse,
        pulse_count=pulse_count,
        pulse_status=PULSE_STATUS,
        current_user=current_user
    )


# ══════════════════════════════════════════════════════════════
# 專案 CRUD
# ══════════════════════════════════════════════════════════════

def _get_all_users(c):
    c.execute('SELECT id, name, department FROM users ORDER BY name')
    return c.fetchall()


@app.route('/project/new', methods=['GET', 'POST'])
@login_required
def project_new():
    conn = get_db()
    c = conn.cursor()
    if request.method == 'POST':
        title       = request.form.get('title', '').strip()
        description = request.form.get('description', '').strip()
        assignee_id = request.form.get('assignee_id') or None
        launch_date = request.form.get('launch_date') or None
        benefit     = request.form.get('benefit', '').strip() or None
        priority    = request.form.get('priority', 'low')
        if priority not in ('high', 'medium', 'low'):
            priority = 'low'

        if not title:
            flash(_t('flash_project_empty'), 'danger')
            return render_template('project_new.html', parent=None,
                                   users=_get_all_users(c))

        c.execute(
            '''INSERT INTO projects (title, description, creator_id, assignee_id, parent_id, launch_date, benefit, priority)
               VALUES (%s, %s, %s, %s, NULL, %s, %s, %s)''',
            (title, description, session['user_id'], assignee_id, launch_date, benefit, priority)
        )
        conn.commit()
        release_db(conn)
        flash(_t('flash_project_created', title=title), 'success')
        return redirect(url_for('index'))

    users = _get_all_users(c)
    release_db(conn)
    return render_template('project_new.html', parent=None, users=users)


@app.route('/project/<int:project_id>/sub/new', methods=['GET', 'POST'])
@login_required
def sub_project_new(project_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM projects WHERE id = %s', (project_id,))
    parent = c.fetchone()

    if not parent:
        release_db(conn)
        flash(_t('flash_project_not_found'), 'danger')
        return redirect(url_for('index'))
    if parent['parent_id'] is not None:
        release_db(conn)
        flash(_t('flash_no_nested'), 'warning')
        return redirect(url_for('project_detail', project_id=project_id))

    if request.method == 'POST':
        title       = request.form.get('title', '').strip()
        description = request.form.get('description', '').strip()
        assignee_id = request.form.get('assignee_id') or None
        launch_date = request.form.get('launch_date') or None
        benefit     = request.form.get('benefit', '').strip() or None
        priority    = request.form.get('priority', 'low')
        if priority not in ('high', 'medium', 'low'):
            priority = 'low'

        if not title:
            flash(_t('flash_sub_name_empty'), 'danger')
            return render_template('project_new.html', parent=parent,
                                   users=_get_all_users(c))

        c.execute(
            '''INSERT INTO projects (title, description, creator_id, assignee_id, parent_id, launch_date, benefit, priority)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''',
            (title, description, session['user_id'], assignee_id, project_id, launch_date, benefit, priority)
        )
        c.execute(
            'INSERT INTO activity_log (project_id, user_id, action_type, action_label) VALUES (%s,%s,%s,%s)',
            (project_id, session['user_id'], 'sub_project',
             f'{session["user_name"]} 新增了子專案「{title}」')
        )
        conn.commit()
        release_db(conn)
        flash(_t('flash_sub_created', title=title), 'success')
        return redirect(url_for('project_detail', project_id=project_id))

    users = _get_all_users(c)
    release_db(conn)
    return render_template('project_new.html', parent=parent, users=users)


@app.route('/project/<int:project_id>')
def project_detail(project_id):
    conn = get_db()
    c = conn.cursor()

    c.execute('''
        SELECT p.*, u.name AS creator_name, u.department AS creator_department,
               ua.name AS assignee_name, ua.department AS assignee_department
        FROM projects p
        JOIN  users u  ON p.creator_id  = u.id
        LEFT JOIN users ua ON p.assignee_id = ua.id
        WHERE p.id = %s
    ''', (project_id,))
    project = c.fetchone()

    if not project:
        release_db(conn)
        flash(_t('flash_project_not_found'), 'danger')
        return redirect(url_for('index'))

    c.execute('''
        SELECT p.*, u.name AS creator_name, u.department AS creator_department,
               ua.name AS assignee_name,
               (SELECT COUNT(*) FROM comments cm WHERE cm.project_id = p.id) AS comment_count,
               (SELECT pp.status FROM project_pulse pp WHERE pp.project_id = p.id
                ORDER BY pp.created_at DESC LIMIT 1) AS latest_pulse
        FROM projects p
        JOIN  users u  ON p.creator_id  = u.id
        LEFT JOIN users ua ON p.assignee_id = ua.id
        WHERE p.parent_id = %s
        ORDER BY p.created_at ASC
    ''', (project_id,))
    sub_projects = c.fetchall()

    c.execute('''
        SELECT c.*, u.name AS user_name, u.department AS user_department
        FROM comments c
        JOIN users u ON c.user_id = u.id
        WHERE c.project_id = %s
        ORDER BY c.created_at ASC
    ''', (project_id,))
    comments = c.fetchall()

    c.execute('''
        SELECT pp.*, u.name AS user_name, u.department AS user_department
        FROM project_pulse pp
        JOIN users u ON pp.user_id = u.id
        WHERE pp.project_id = %s
        ORDER BY pp.created_at DESC
    ''', (project_id,))
    pulses = c.fetchall()

    parent = None
    if project['parent_id']:
        c.execute('SELECT * FROM projects WHERE id = %s', (project['parent_id'],))
        parent = c.fetchone()

    release_db(conn)
    current_user = get_current_user()
    can_update_pulse = bool(
        current_user and (
            current_user['id'] == project['creator_id'] or
            current_user['id'] == project['assignee_id']
        )
    )
    return render_template(
        'project_detail.html',
        project=project,
        sub_projects=sub_projects,
        comments=comments,
        pulses=pulses,
        parent=parent,
        current_user=current_user,
        pulse_status=PULSE_STATUS,
        can_update_pulse=can_update_pulse,
    )


@app.route('/project/<int:project_id>/edit', methods=['GET', 'POST'])
@login_required
def project_edit(project_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM projects WHERE id = %s', (project_id,))
    project = c.fetchone()

    if not project:
        release_db(conn)
        flash(_t('flash_project_not_found'), 'danger')
        return redirect(url_for('index'))
    if project['creator_id'] != session['user_id']:
        release_db(conn)
        flash(_t('flash_creator_edit_only'), 'danger')
        return redirect(url_for('project_detail', project_id=project_id))

    if request.method == 'POST':
        title       = request.form.get('title', '').strip()
        description = request.form.get('description', '').strip()
        assignee_id = request.form.get('assignee_id') or None
        launch_date = request.form.get('launch_date') or None
        benefit     = request.form.get('benefit', '').strip() or None
        priority    = request.form.get('priority', 'low')
        if priority not in ('high', 'medium', 'low'):
            priority = 'low'

        if not title:
            flash(_t('flash_project_empty'), 'danger')
            return render_template('project_edit.html', project=project,
                                   users=_get_all_users(c))

        c.execute(
            '''UPDATE projects
               SET title=%s, description=%s, assignee_id=%s, launch_date=%s, benefit=%s,
                   priority=%s, updated_at=CURRENT_TIMESTAMP
               WHERE id=%s''',
            (title, description, assignee_id, launch_date, benefit, priority, project_id)
        )
        _changes = []
        if title != project['title']:
            _changes.append('名稱')
        if str(assignee_id or '') != str(project['assignee_id'] or ''):
            _changes.append('負責人')
        if (launch_date or '') != (str(project['launch_date'])[:10] if project['launch_date'] else ''):
            _changes.append('預計上線日期')
        if priority != (project['priority'] or 'low'):
            _changes.append(f'重要性→{priority.upper()}')
        if description != (project['description'] or ''):
            _changes.append('說明')
        if (benefit or '') != (project['benefit'] or ''):
            _changes.append('效益')
        _desc = '、'.join(_changes) if _changes else '資訊'
        c.execute(
            'INSERT INTO activity_log (project_id, user_id, action_type, action_label) VALUES (%s,%s,%s,%s)',
            (project_id, session['user_id'], 'edit',
             f'{session["user_name"]} 編輯了專案（更新：{_desc}）')
        )
        conn.commit()
        release_db(conn)
        flash(_t('flash_project_updated'), 'success')
        return redirect(url_for('project_detail', project_id=project_id))

    users = _get_all_users(c)
    release_db(conn)
    return render_template('project_edit.html', project=project, users=users)


@app.route('/project/<int:project_id>/delete', methods=['POST'])
@login_required
def project_delete(project_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM projects WHERE id = %s', (project_id,))
    project = c.fetchone()

    if not project:
        release_db(conn)
        flash(_t('flash_project_not_found'), 'danger')
        return redirect(url_for('index'))
    if project['creator_id'] != session['user_id']:
        release_db(conn)
        flash(_t('flash_creator_del_only'), 'danger')
        return redirect(url_for('project_detail', project_id=project_id))

    parent_id = project['parent_id']

    def _delete_project_data(pid):
        c.execute('DELETE FROM comments      WHERE project_id=%s', (pid,))
        c.execute('DELETE FROM project_pulse WHERE project_id=%s', (pid,))

    if parent_id is None:
        c.execute('SELECT id FROM projects WHERE parent_id=%s', (project_id,))
        for sub in c.fetchall():
            _delete_project_data(sub['id'])
        c.execute('DELETE FROM projects WHERE parent_id=%s', (project_id,))

    _delete_project_data(project_id)
    c.execute('DELETE FROM projects WHERE id=%s', (project_id,))
    conn.commit()
    release_db(conn)

    flash(_t('flash_project_deleted'), 'success')
    if parent_id:
        return redirect(url_for('project_detail', project_id=parent_id))
    return redirect(url_for('index'))


# ══════════════════════════════════════════════════════════════
# 專案清單（可排序）
# ══════════════════════════════════════════════════════════════

ALLOWED_SORT_COLS = {
    'title':       'p.title',
    'assignee':    'assignee_name',
    'department':  'ua.department',
    'status':      'latest_pulse',
    'launch_date': 'p.launch_date',
    'priority':    "CASE p.priority WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END",
}

@app.route('/projects/list')
@login_required
def project_list():
    sort_by  = request.args.get('sort', 'title')
    sort_dir = request.args.get('dir', 'asc')

    if sort_by not in ALLOWED_SORT_COLS:
        sort_by = 'title'
    if sort_dir not in ('asc', 'desc'):
        sort_dir = 'asc'

    col = ALLOWED_SORT_COLS[sort_by]
    null_order = 'NULLS LAST' if sort_dir == 'asc' else 'NULLS FIRST'

    conn = get_db()
    c = conn.cursor()
    c.execute(f'''
        SELECT
            p.id, p.title, p.launch_date, p.parent_id,
            p.priority, p.creator_id, p.assignee_id,
            ua.name AS assignee_name,
            ua.department AS assignee_department,
            (SELECT pp.status FROM project_pulse pp
             WHERE pp.project_id = p.id ORDER BY pp.created_at DESC LIMIT 1) AS latest_pulse
        FROM projects p
        LEFT JOIN users ua ON p.assignee_id = ua.id
        WHERE p.parent_id IS NULL
        ORDER BY {col} {sort_dir} {null_order}
    ''')
    parents = c.fetchall()

    c.execute('''
        SELECT
            p.id, p.title, p.launch_date, p.parent_id,
            p.priority, p.creator_id, p.assignee_id,
            ua.name AS assignee_name,
            ua.department AS assignee_department,
            (SELECT pp.status FROM project_pulse pp
             WHERE pp.project_id = p.id ORDER BY pp.created_at DESC LIMIT 1) AS latest_pulse
        FROM projects p
        LEFT JOIN users ua ON p.assignee_id = ua.id
        WHERE p.parent_id IS NOT NULL
        ORDER BY p.parent_id, p.created_at ASC
    ''')
    all_children = c.fetchall()

    c.execute('SELECT DISTINCT name FROM users WHERE name IS NOT NULL ORDER BY name')
    all_users = [r['name'] for r in c.fetchall()]

    c.execute('''
        SELECT DISTINCT ua.department
        FROM projects p
        LEFT JOIN users ua ON p.assignee_id = ua.id
        WHERE ua.department IS NOT NULL
        ORDER BY ua.department
    ''')
    departments = [r['department'] for r in c.fetchall()]

    release_db(conn)

    children_map = {}
    for child in all_children:
        children_map.setdefault(child['parent_id'], []).append(child)

    next_dir = 'desc' if sort_dir == 'asc' else 'asc'
    return render_template(
        'project_list.html',
        parents=parents,
        children_map=children_map,
        sort_by=sort_by,
        sort_dir=sort_dir,
        next_dir=next_dir,
        pulse_status=PULSE_STATUS,
        departments=departments,
        all_users=all_users,
    )


@app.route('/manager')
@login_required
def manager():
    # Access control
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT email, is_manager FROM users WHERE id=%s', (session['user_id'],))
    _u = c.fetchone()
    if not _u or (_u['email'] not in MANAGER_EMAILS and not _u.get('is_manager')):
        release_db(conn)
        abort(403)

    # Last week date range (previous Sun → this Sun)
    today = date.today()
    days_since_sunday = (today.weekday() + 1) % 7
    this_week_start = datetime.combine(today - timedelta(days=days_since_sunday), datetime.min.time())
    last_week_start = this_week_start - timedelta(days=7)

    # 1. Summary counts (all-time)
    c.execute('''
        SELECT
            COUNT(*) FILTER (WHERE parent_id IS NULL) AS total,
            COUNT(*) FILTER (WHERE parent_id IS NULL AND (
                SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
            ) = 'process') AS in_progress,
            COUNT(*) FILTER (WHERE parent_id IS NULL AND (
                SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
            ) = 'done') AS done_count,
            COUNT(DISTINCT assignee_id) FILTER (WHERE parent_id IS NULL AND assignee_id IS NOT NULL) AS assignee_count
        FROM projects p
    ''')
    summary = c.fetchone()

    # 2. Last-week activity count
    c.execute('''
        SELECT COUNT(DISTINCT project_id) AS cnt
        FROM activity_log
        WHERE created_at >= %s AND created_at < %s
    ''', (last_week_start, this_week_start))
    last_week_updated = c.fetchone()['cnt']

    # 3. Dept stats
    c.execute('''
        SELECT
            COALESCE(ua.department, '未指派') AS department,
            COUNT(p.id) AS total,
            COUNT(*) FILTER (WHERE (
                SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
            ) = 'new') AS new_count,
            COUNT(*) FILTER (WHERE (
                SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
            ) = 'process') AS process_count,
            COUNT(*) FILTER (WHERE (
                SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
            ) = 'done') AS done_count
        FROM projects p
        LEFT JOIN users ua ON p.assignee_id = ua.id
        WHERE p.parent_id IS NULL
        GROUP BY COALESCE(ua.department, '未指派')
        ORDER BY total DESC
    ''')
    dept_stats = c.fetchall()

    # 4. Person stats
    c.execute('''
        SELECT
            ua.name AS assignee_name,
            COALESCE(ua.department, '') AS department,
            COUNT(p.id) AS total,
            COUNT(*) FILTER (WHERE (
                SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
            ) = 'process') AS in_progress
        FROM projects p
        JOIN users ua ON p.assignee_id = ua.id
        WHERE p.parent_id IS NULL
        GROUP BY ua.name, ua.department
        ORDER BY total DESC
    ''')
    person_stats = c.fetchall()

    # 5. Unified activity log — all recent (150 entries)
    _log_sql = '''
        SELECT
            al.id, al.project_id, al.action_type, al.action_label, al.created_at,
            u.name AS user_name, u.department AS user_dept,
            p.title AS project_title,
            COALESCE(root.title, p.title) AS root_title,
            COALESCE(p.parent_id, p.id)   AS root_project_id
        FROM activity_log al
        JOIN users u    ON al.user_id    = u.id
        JOIN projects p ON al.project_id = p.id
        LEFT JOIN projects root ON p.parent_id = root.id
        {where}
        ORDER BY al.created_at DESC
        LIMIT 150
    '''
    c.execute(_log_sql.format(where=''))
    all_logs = c.fetchall()

    c.execute(_log_sql.format(where='WHERE al.created_at >= %s AND al.created_at < %s'),
              (last_week_start, this_week_start))
    last_week_logs = c.fetchall()

    release_db(conn)

    dept_chart = {
        'labels':  [r['department']   for r in dept_stats],
        'new':     [r['new_count']     for r in dept_stats],
        'process': [r['process_count'] for r in dept_stats],
        'done':    [r['done_count']    for r in dept_stats],
    }
    person_chart = {
        'labels': [r['assignee_name'] for r in person_stats],
        'totals': [r['total']         for r in person_stats],
    }
    status_chart = {
        'new':     (summary['total'] or 0) - (summary['in_progress'] or 0) - (summary['done_count'] or 0),
        'process': summary['in_progress'] or 0,
        'done':    summary['done_count']  or 0,
    }

    return render_template(
        'manager.html',
        summary=summary,
        last_week_updated=last_week_updated,
        dept_stats=dept_stats,
        person_stats=person_stats,
        all_logs=all_logs,
        last_week_logs=last_week_logs,
        dept_chart=dept_chart,
        person_chart=person_chart,
        status_chart=status_chart,
        pulse_status=PULSE_STATUS,
        last_week_start=last_week_start.date(),
        last_week_end=(this_week_start - timedelta(days=1)).date(),
    )


@app.route('/project/<int:project_id>/priority', methods=['POST'])
@login_required
def project_set_priority(project_id):
    data     = request.get_json() or {}
    priority = data.get('priority', 'low')
    if priority not in ('high', 'medium', 'low'):
        return jsonify(ok=False, error='invalid priority'), 400

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT creator_id, assignee_id FROM projects WHERE id=%s', (project_id,))
    proj = c.fetchone()
    if not proj:
        release_db(conn)
        return jsonify(ok=False, error='not found'), 404

    uid = session['user_id']
    if uid != proj['creator_id'] and uid != proj['assignee_id']:
        release_db(conn)
        return jsonify(ok=False, error='no permission'), 403

    c.execute('UPDATE projects SET priority=%s, updated_at=CURRENT_TIMESTAMP WHERE id=%s',
              (priority, project_id))
    conn.commit()
    release_db(conn)
    return jsonify(ok=True)


@app.route('/kanban')
@login_required
def kanban():
    conn = get_db()
    c = conn.cursor()
    c.execute('''
        SELECT
            p.id, p.title, p.parent_id, p.launch_date,
            par.title AS parent_title,
            (SELECT pp.status FROM project_pulse pp
             WHERE pp.project_id = p.id ORDER BY pp.created_at DESC LIMIT 1) AS latest_pulse
        FROM projects p
        LEFT JOIN projects par ON p.parent_id = par.id
        WHERE p.assignee_id = %s
        ORDER BY p.parent_id NULLS FIRST, p.title
    ''', (session['user_id'],))
    projects = c.fetchall()
    release_db(conn)

    columns = {key: [] for key in PULSE_STATUS}
    for proj in projects:
        status = proj['latest_pulse'] if proj['latest_pulse'] in PULSE_STATUS else 'new'
        columns[status].append(proj)

    return render_template('kanban.html',
                           columns=columns,
                           pulse_status=PULSE_STATUS)


# ══════════════════════════════════════════════════════════════
# 留言路由
# ══════════════════════════════════════════════════════════════

@app.route('/project/<int:project_id>/comment', methods=['POST'])
@login_required
def comment_add(project_id):
    content = request.form.get('content', '').strip()
    if not content:
        if request.headers.get('X-Requested-With') == 'fetch':
            return jsonify(ok=False, error=_t('flash_comment_empty')), 400
        flash(_t('flash_comment_empty'), 'danger')
        return redirect(url_for('project_detail', project_id=project_id))

    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO comments (project_id, user_id, content) VALUES (%s, %s, %s) RETURNING id, created_at',
        (project_id, session['user_id'], content)
    )
    row = c.fetchone()
    _preview = content[:60] + ('…' if len(content) > 60 else '')
    c.execute(
        'INSERT INTO activity_log (project_id, user_id, action_type, action_label) VALUES (%s,%s,%s,%s)',
        (project_id, session['user_id'], 'comment',
         f'{session["user_name"]} 新增留言：{_preview}')
    )
    conn.commit()
    release_db(conn)

    if request.headers.get('X-Requested-With') == 'fetch':
        return jsonify(ok=True,
                       id=row['id'],
                       user_name=session['user_name'],
                       created_at=row['created_at'].strftime('%Y-%m-%d %H:%M'),
                       content=content)
    return redirect(url_for('project_detail', project_id=project_id) + '#comments')


@app.route('/comment/<int:comment_id>/delete', methods=['POST'])
@login_required
def comment_delete(comment_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM comments WHERE id=%s', (comment_id,))
    comment = c.fetchone()

    if not comment:
        release_db(conn)
        return redirect(url_for('index'))

    project_id = comment['project_id']
    if comment['user_id'] != session['user_id']:
        release_db(conn)
        flash(_t('flash_comment_own'), 'danger')
        return redirect(url_for('project_detail', project_id=project_id))

    c.execute('DELETE FROM comments WHERE id=%s', (comment_id,))
    conn.commit()
    release_db(conn)
    return redirect(url_for('project_detail', project_id=project_id) + '#comments')


# ══════════════════════════════════════════════════════════════
# 專案進度脈動（Pulse）
# ══════════════════════════════════════════════════════════════

@app.route('/project/<int:project_id>/pulse', methods=['POST'])
@login_required
def pulse_add(project_id):
    status  = request.form.get('status', '').strip()
    message = request.form.get('message', '').strip()

    def _err(msg):
        if request.headers.get('X-Requested-With') == 'fetch':
            return jsonify(ok=False, error=msg), 400
        flash(msg, 'danger')
        return redirect(url_for('project_detail', project_id=project_id))

    # 權限：只有負責人或建立者可以更新進度
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT creator_id, assignee_id FROM projects WHERE id = %s', (project_id,))
    proj = c.fetchone()
    release_db(conn)
    if not proj:
        return _err(_t('flash_project_not_found'))
    uid = session['user_id']
    if uid != proj['creator_id'] and uid != proj['assignee_id']:
        return _err(_t('flash_pulse_no_permission'))

    if status not in PULSE_STATUS:
        return _err(_t('flash_pulse_invalid'))
    if len(message) > 300:
        return _err(_t('flash_pulse_long'))

    conn = get_db()
    c = conn.cursor()
    c.execute(
        'INSERT INTO project_pulse (project_id, user_id, status, message) VALUES (%s, %s, %s, %s) RETURNING id, created_at',
        (project_id, session['user_id'], status, message)
    )
    row = c.fetchone()
    _status_names = {'new': '未開始', 'process': '進行中', 'done': '已完成'}
    _preview = message[:60] + ('…' if len(message) > 60 else '')
    c.execute(
        'INSERT INTO activity_log (project_id, user_id, action_type, action_label) VALUES (%s,%s,%s,%s)',
        (project_id, session['user_id'], 'pulse',
         f'{session["user_name"]} 更新進度 → {_status_names.get(status, status)}：{_preview}')
    )
    conn.commit()
    release_db(conn)

    if request.headers.get('X-Requested-With') == 'fetch':
        ps = PULSE_STATUS[status]
        return jsonify(ok=True,
                       id=row['id'],
                       user_name=session['user_name'],
                       created_at=row['created_at'].strftime('%Y-%m-%d %H:%M'),
                       status=status,
                       status_label=_t(ps['t_key']),
                       status_color=ps['color'],
                       message=message)
    flash(_t('flash_pulse_published'), 'success')
    return redirect(url_for('project_detail', project_id=project_id) + '#pulse')


@app.route('/pulse/<int:pulse_id>/delete', methods=['POST'])
@login_required
def pulse_delete(pulse_id):
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM project_pulse WHERE id=%s', (pulse_id,))
    pulse = c.fetchone()

    if not pulse:
        release_db(conn)
        return redirect(url_for('index'))

    project_id = pulse['project_id']
    if pulse['user_id'] != session['user_id']:
        release_db(conn)
        flash(_t('flash_pulse_own'), 'danger')
        return redirect(url_for('project_detail', project_id=project_id))

    c.execute('DELETE FROM project_pulse WHERE id=%s', (pulse_id,))
    conn.commit()
    release_db(conn)
    return redirect(url_for('project_detail', project_id=project_id) + '#pulse')


# ══════════════════════════════════════════════════════════════
# 程式進入點
# ══════════════════════════════════════════════════════════════

# 無論是 gunicorn 還是直接執行，都執行資料庫初始化
with app.app_context():
    try:
        init_db()
    except Exception as _e:
        print(f"⚠️  init_db warning: {_e}")

if __name__ == '__main__':
    print("🚀 伺服器啟動中，請在瀏覽器開啟：http://127.0.0.1:5000")
    app.run(debug=True, port=5000)
