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
        return {'current_user_is_manager': False, 'manager_dept': None}
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT managed_dept, is_manager FROM users WHERE id=%s', (session['user_id'],))
    u = c.fetchone()
    if not u:
        return {'current_user_is_manager': False, 'manager_dept': None}
    raw = u.get('managed_dept')
    if raw:
        # 'ALL' means no dept filter; specific string means filter to that dept
        return {'current_user_is_manager': True, 'manager_dept': None if raw == 'ALL' else raw}
    if u.get('is_manager'):
        return {'current_user_is_manager': True, 'manager_dept': None}
    return {'current_user_is_manager': False, 'manager_dept': None}


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
    c.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS managed_dept TEXT DEFAULT NULL')

    # Seed initial dept manager assignments (idempotent: only set if currently NULL)
    _initial_managers = [
        ('xavier.liu@kkday.com',   'ALL'),
        ('victor.tseng@kkday.com', 'ALL'),
        ('shannen.lin@kkday.com',  '投資人'),
        ('johnson.tsai@kkday.com', '法務'),
        ('mj.chiang@kkday.com',    '財務'),
        ('steven.yang@kkday.com',  '財務規劃'),
        ('jensen.huang@kkday.com', '會計'),
    ]
    for _email, _dept in _initial_managers:
        c.execute('''
            UPDATE users SET managed_dept = %s
            WHERE email = %s AND managed_dept IS NULL
        ''', (_dept, _email))

    # weekly_reports table
    c.execute('''
        CREATE TABLE IF NOT EXISTS weekly_reports (
            id          SERIAL PRIMARY KEY,
            report_date DATE      NOT NULL,
            user_id     INTEGER   NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            content     TEXT      NOT NULL DEFAULT '',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    c.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS uq_weekly_report
        ON weekly_reports (user_id, DATE_TRUNC('week', report_date::timestamp))
    ''')

    # One-time migration: backfill existing pulse / comment / sub-project records
    c.execute('SELECT COUNT(*) FROM activity_log')
    if c.fetchone()['count'] == 0:
        # Backfill pulse logs
        _status_names = {'new': '未開始', 'process': '進行中', 'done': '已完成'}
        c.execute('''
            SELECT pp.project_id, pp.user_id, pp.status, pp.message, pp.created_at,
                   u.name AS user_name
            FROM project_pulse pp
            JOIN users u ON pp.user_id = u.id
            ORDER BY pp.created_at
        ''')
        for row in c.fetchall():
            _preview = row['message'][:60] + ('…' if len(row['message']) > 60 else '')
            _sname   = _status_names.get(row['status'], row['status'])
            c.execute(
                '''INSERT INTO activity_log (project_id, user_id, action_type, action_label, created_at)
                   VALUES (%s,%s,%s,%s,%s)''',
                (row['project_id'], row['user_id'], 'pulse',
                 f'{row["user_name"]} 更新進度 → {_sname}：{_preview}',
                 row['created_at'])
            )
        # Backfill comments
        c.execute('''
            SELECT cm.project_id, cm.user_id, cm.content, cm.created_at,
                   u.name AS user_name
            FROM comments cm
            JOIN users u ON cm.user_id = u.id
            ORDER BY cm.created_at
        ''')
        for row in c.fetchall():
            _preview = row['content'][:60] + ('…' if len(row['content']) > 60 else '')
            c.execute(
                '''INSERT INTO activity_log (project_id, user_id, action_type, action_label, created_at)
                   VALUES (%s,%s,%s,%s,%s)''',
                (row['project_id'], row['user_id'], 'comment',
                 f'{row["user_name"]} 新增留言：{_preview}',
                 row['created_at'])
            )
        # Backfill sub-project creations (log to parent project)
        c.execute('''
            SELECT p.id, p.parent_id, p.title, p.creator_id, p.created_at,
                   u.name AS user_name
            FROM projects p
            JOIN users u ON p.creator_id = u.id
            WHERE p.parent_id IS NOT NULL
            ORDER BY p.created_at
        ''')
        for row in c.fetchall():
            c.execute(
                '''INSERT INTO activity_log (project_id, user_id, action_type, action_label, created_at)
                   VALUES (%s,%s,%s,%s,%s)''',
                (row['parent_id'], row['creator_id'], 'sub_project',
                 f'{row["user_name"]} 新增了子專案「{row["title"]}」',
                 row['created_at'])
            )

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
    c.execute('SELECT managed_dept, is_manager FROM users WHERE id=%s', (session['user_id'],))
    _u = c.fetchone()
    if not _u or (not _u.get('managed_dept') and not _u.get('is_manager')):
        release_db(conn)
        abort(403)

    # Dept scope: None = all depts, string = specific dept
    _raw_dept = _u.get('managed_dept')
    managed_dept = None if (not _raw_dept or _raw_dept == 'ALL') else _raw_dept

    # Last week date range (previous Sun → this Sun)
    today = date.today()
    days_since_sunday = (today.weekday() + 1) % 7
    this_week_start = datetime.combine(today - timedelta(days=days_since_sunday), datetime.min.time())
    last_week_start = this_week_start - timedelta(days=7)

    # 1. Summary counts (all-time)
    if managed_dept:
        c.execute('''
            SELECT
                COUNT(*) FILTER (WHERE p.parent_id IS NULL) AS total,
                COUNT(*) FILTER (WHERE p.parent_id IS NULL AND (
                    SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
                ) = 'process') AS in_progress,
                COUNT(*) FILTER (WHERE p.parent_id IS NULL AND (
                    SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
                ) = 'done') AS done_count,
                COUNT(DISTINCT p.assignee_id) FILTER (WHERE p.parent_id IS NULL AND p.assignee_id IS NOT NULL) AS assignee_count
            FROM projects p
            JOIN users ua ON p.assignee_id = ua.id
            WHERE ua.department = %s
        ''', (managed_dept,))
    else:
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
    if managed_dept:
        c.execute('''
            SELECT COUNT(DISTINCT al.project_id) AS cnt
            FROM activity_log al
            JOIN users u ON al.user_id = u.id
            WHERE al.created_at >= %s AND al.created_at < %s
              AND u.department = %s
        ''', (last_week_start, this_week_start, managed_dept))
    else:
        c.execute('''
            SELECT COUNT(DISTINCT project_id) AS cnt
            FROM activity_log
            WHERE created_at >= %s AND created_at < %s
        ''', (last_week_start, this_week_start))
    last_week_updated = c.fetchone()['cnt']

    # 3. Dept stats
    if managed_dept:
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
            WHERE p.parent_id IS NULL AND ua.department = %s
            GROUP BY COALESCE(ua.department, '未指派')
            ORDER BY total DESC
        ''', (managed_dept,))
    else:
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
    if managed_dept:
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
            WHERE p.parent_id IS NULL AND ua.department = %s
            GROUP BY ua.name, ua.department
            ORDER BY total DESC
        ''', (managed_dept,))
    else:
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

    # 5. Week-mode summary stats
    if managed_dept:
        c.execute('''
            SELECT
                COUNT(*)                                          AS total_actions,
                COUNT(DISTINCT al.project_id)                     AS active_projects,
                COUNT(DISTINCT al.user_id)                        AS active_users,
                COUNT(*) FILTER (WHERE al.action_type='pulse')       AS pulse_count,
                COUNT(*) FILTER (WHERE al.action_type='comment')     AS comment_count,
                COUNT(*) FILTER (WHERE al.action_type='edit')        AS edit_count,
                COUNT(*) FILTER (WHERE al.action_type='sub_project') AS sub_count
            FROM activity_log al
            JOIN users u ON al.user_id = u.id
            WHERE al.created_at >= %s AND al.created_at < %s
              AND u.department = %s
        ''', (last_week_start, this_week_start, managed_dept))
    else:
        c.execute('''
            SELECT
                COUNT(*)                                          AS total_actions,
                COUNT(DISTINCT project_id)                        AS active_projects,
                COUNT(DISTINCT user_id)                           AS active_users,
                COUNT(*) FILTER (WHERE action_type='pulse')       AS pulse_count,
                COUNT(*) FILTER (WHERE action_type='comment')     AS comment_count,
                COUNT(*) FILTER (WHERE action_type='edit')        AS edit_count,
                COUNT(*) FILTER (WHERE action_type='sub_project') AS sub_count
            FROM activity_log
            WHERE created_at >= %s AND created_at < %s
        ''', (last_week_start, this_week_start))
    week_summary = c.fetchone()

    # 6. Week dept stats (activity counts by dept)
    if managed_dept:
        c.execute('''
            SELECT
                COALESCE(u.department, '未指派') AS department,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE al.action_type='pulse')   AS pulse_count,
                COUNT(*) FILTER (WHERE al.action_type='comment') AS comment_count,
                COUNT(*) FILTER (WHERE al.action_type='edit')    AS edit_count
            FROM activity_log al
            JOIN users u ON al.user_id = u.id
            WHERE al.created_at >= %s AND al.created_at < %s
              AND u.department = %s
            GROUP BY COALESCE(u.department, '未指派')
            ORDER BY total DESC
        ''', (last_week_start, this_week_start, managed_dept))
    else:
        c.execute('''
            SELECT
                COALESCE(u.department, '未指派') AS department,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE al.action_type='pulse')   AS pulse_count,
                COUNT(*) FILTER (WHERE al.action_type='comment') AS comment_count,
                COUNT(*) FILTER (WHERE al.action_type='edit')    AS edit_count
            FROM activity_log al
            JOIN users u ON al.user_id = u.id
            WHERE al.created_at >= %s AND al.created_at < %s
            GROUP BY COALESCE(u.department, '未指派')
            ORDER BY total DESC
        ''', (last_week_start, this_week_start))
    week_dept_stats = c.fetchall()

    # 7. Week person stats (activity counts by person)
    if managed_dept:
        c.execute('''
            SELECT
                u.name AS user_name,
                COALESCE(u.department, '') AS department,
                COUNT(*) AS action_count,
                COUNT(DISTINCT al.project_id) AS project_count
            FROM activity_log al
            JOIN users u ON al.user_id = u.id
            WHERE al.created_at >= %s AND al.created_at < %s
              AND u.department = %s
            GROUP BY u.name, u.department
            ORDER BY action_count DESC
        ''', (last_week_start, this_week_start, managed_dept))
    else:
        c.execute('''
            SELECT
                u.name AS user_name,
                COALESCE(u.department, '') AS department,
                COUNT(*) AS action_count,
                COUNT(DISTINCT al.project_id) AS project_count
            FROM activity_log al
            JOIN users u ON al.user_id = u.id
            WHERE al.created_at >= %s AND al.created_at < %s
            GROUP BY u.name, u.department
            ORDER BY action_count DESC
        ''', (last_week_start, this_week_start))
    week_person_stats = c.fetchall()

    # 8. Unified activity log — all recent (150 entries) + last week
    if managed_dept:
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
            WHERE u.department = %s {extra}
            ORDER BY al.created_at DESC
            LIMIT 150
        '''
        c.execute(_log_sql.format(extra=''), (managed_dept,))
        all_logs = c.fetchall()
        c.execute(_log_sql.format(extra='AND al.created_at >= %s AND al.created_at < %s'),
                  (managed_dept, last_week_start, this_week_start))
        last_week_logs = c.fetchall()
    else:
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

    # All-mode chart data
    dept_chart = {
        'labels':  [r['department']    for r in dept_stats],
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

    # Week-mode chart data
    week_dept_chart = {
        'labels':  [r['department']   for r in week_dept_stats],
        'pulse':   [r['pulse_count']   for r in week_dept_stats],
        'comment': [r['comment_count'] for r in week_dept_stats],
        'edit':    [r['edit_count']    for r in week_dept_stats],
    }
    week_person_chart = {
        'labels': [r['user_name']    for r in week_person_stats],
        'totals': [r['action_count'] for r in week_person_stats],
    }
    week_action_chart = {
        'pulse':       week_summary['pulse_count']   or 0,
        'comment':     week_summary['comment_count'] or 0,
        'edit':        week_summary['edit_count']    or 0,
        'sub_project': week_summary['sub_count']     or 0,
    }

    return render_template(
        'manager.html',
        summary=summary,
        last_week_updated=last_week_updated,
        week_summary=week_summary,
        dept_stats=dept_stats,
        person_stats=person_stats,
        week_dept_stats=week_dept_stats,
        week_person_stats=week_person_stats,
        all_logs=all_logs,
        last_week_logs=last_week_logs,
        dept_chart=dept_chart,
        person_chart=person_chart,
        status_chart=status_chart,
        week_dept_chart=week_dept_chart,
        week_person_chart=week_person_chart,
        week_action_chart=week_action_chart,
        pulse_status=PULSE_STATUS,
        last_week_start=last_week_start.date(),
        last_week_end=(this_week_start - timedelta(days=1)).date(),
        managed_dept=managed_dept,
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
# 週報模組
# ══════════════════════════════════════════════════════════════

@app.route('/admin/seed-test-reports', methods=['POST'])
@login_required
def seed_test_reports():
    """Temporary: seed fake users + reports for April last week UI testing."""
    conn = get_db()
    c = conn.cursor()

    fake_users = [
        # name, email, dept, report_date (last week of April 2026)
        ('陳小華', 'chen.xiaohua@test.kkday.com', '會計', date(2026, 4, 27)),
        ('林美玲', 'lin.meiling@test.kkday.com',  '會計', date(2026, 4, 28)),
        ('王大明', 'wang.daming@test.kkday.com',  '會計', date(2026, 4, 29)),
        ('吳志豪', 'wu.zhihao@test.kkday.com',    '會計', date(2026, 4, 30)),
        ('李建國', 'li.jianguo@test.kkday.com',   '法務', date(2026, 4, 27)),
        ('張美惠', 'zhang.meihui@test.kkday.com', '法務', date(2026, 4, 28)),
        ('劉雅婷', 'liu.yating@test.kkday.com',   '法務', date(2026, 4, 29)),
        ('黃俊傑', 'huang.junjie@test.kkday.com', '法務', date(2026, 4, 30)),
    ]
    _pw = generate_password_hash('test1234')
    created = 0
    for name, email, dept, rdate in fake_users:
        # Insert user if not exists
        c.execute('SELECT id FROM users WHERE email=%s', (email,))
        row = c.fetchone()
        if row:
            uid = row['id']
        else:
            c.execute(
                'INSERT INTO users (name, email, password, department) VALUES (%s,%s,%s,%s) RETURNING id',
                (name, email, _pw, dept)
            )
            uid = c.fetchone()['id']

        # Insert report (skip if week already exists)
        try:
            c.execute(
                'INSERT INTO weekly_reports (report_date, user_id, content) VALUES (%s,%s,%s)',
                (rdate, uid,
                 f'【{dept}部門週報】\n\n本週專案進度順利，各項工作如期推進。\n\n■ 專案A（進行中）\n  負責人：{name}\n  近兩週異動：04/28 更新進度 → 進行中\n\n■ 專案B（未開始）\n  近兩週無異動')
            )
            created += 1
        except pg_errors.UniqueViolation:
            conn.rollback()
            conn = get_db()
            c = conn.cursor()

    conn.commit()
    release_db(conn)
    flash(f'已建立 {created} 筆測試週報', 'success')
    return redirect(url_for('report_calendar', year=2026, month=4))

def _get_user_scope(c, user_id):
    """Returns (is_manager, dept_filter).
    is_manager: bool
    dept_filter: None = see all, string = specific dept, '' = own only
    """
    c.execute('SELECT managed_dept, is_manager FROM users WHERE id=%s', (user_id,))
    u = c.fetchone()
    if not u:
        return False, ''
    raw = u.get('managed_dept')
    if raw == 'ALL':
        return True, None
    if raw:
        return True, raw
    if u.get('is_manager'):
        return True, None
    return False, ''


def _build_report_prefill(c, user_id, report_date_str, is_mgr, dept_filter):
    """Generate pre-fill text: undone projects + 2-week activity."""
    try:
        report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        report_date = date.today()

    two_weeks_ago = datetime.combine(report_date - timedelta(days=14), datetime.min.time())
    report_dt = datetime.combine(report_date + timedelta(days=1), datetime.min.time())

    status_cond = '''COALESCE((
        SELECT status FROM project_pulse WHERE project_id=p.id ORDER BY created_at DESC LIMIT 1
    ), 'new')'''

    if not is_mgr:
        c.execute(f'''
            SELECT p.id, p.title,
                   ua.name AS assignee_name, ua.department AS assignee_dept,
                   {status_cond} AS last_status
            FROM projects p
            LEFT JOIN users ua ON p.assignee_id = ua.id
            WHERE p.parent_id IS NULL AND p.assignee_id = %s
              AND {status_cond} != 'done'
            ORDER BY p.id
        ''', (user_id, user_id))
    elif dept_filter:
        c.execute(f'''
            SELECT p.id, p.title,
                   ua.name AS assignee_name, ua.department AS assignee_dept,
                   {status_cond} AS last_status
            FROM projects p
            JOIN users ua ON p.assignee_id = ua.id
            WHERE p.parent_id IS NULL AND ua.department = %s
              AND {status_cond} != 'done'
            ORDER BY p.id
        ''', (dept_filter, dept_filter))
    else:
        c.execute(f'''
            SELECT p.id, p.title,
                   ua.name AS assignee_name, ua.department AS assignee_dept,
                   {status_cond} AS last_status
            FROM projects p
            LEFT JOIN users ua ON p.assignee_id = ua.id
            WHERE p.parent_id IS NULL
              AND {status_cond} != 'done'
            ORDER BY p.id
        ''')

    projects = c.fetchall()
    if not projects:
        return '（本週無待完成專案）'

    status_map = {'new': '未開始', 'process': '進行中'}
    lines = ['【待完成專案 ─ 系統自動帶入，請依實際狀況修改】', '']

    for proj in projects:
        slabel = status_map.get(proj['last_status'], proj['last_status'])
        assignee = f"{proj['assignee_name']}（{proj['assignee_dept']}）" if proj.get('assignee_name') else '未指派'
        lines.append(f"■ {proj['title']}")
        lines.append(f"  狀態：{slabel}　負責人：{assignee}")

        c.execute('''
            SELECT al.action_label, al.created_at
            FROM activity_log al
            WHERE al.project_id = %s
              AND al.created_at >= %s AND al.created_at < %s
            ORDER BY al.created_at DESC
            LIMIT 5
        ''', (proj['id'], two_weeks_ago, report_dt))
        acts = c.fetchall()

        if acts:
            lines.append('  近兩週異動：')
            for a in acts:
                ts = a['created_at'].strftime('%m/%d %H:%M') if a['created_at'] else ''
                lines.append(f'    {ts}　{a["action_label"]}')
        else:
            lines.append('  近兩週無異動')
        lines.append('')

    return '\n'.join(lines)


@app.route('/reports')
@login_required
def report_list():
    conn = get_db()
    c = conn.cursor()
    uid = session['user_id']
    is_mgr, dept_filter = _get_user_scope(c, uid)

    if not is_mgr:
        c.execute('''
            SELECT wr.id, wr.report_date, wr.content, wr.created_at, wr.user_id,
                   u.name AS user_name, u.department
            FROM weekly_reports wr
            JOIN users u ON wr.user_id = u.id
            WHERE wr.user_id = %s
            ORDER BY wr.report_date DESC
        ''', (uid,))
    elif dept_filter:
        c.execute('''
            SELECT wr.id, wr.report_date, wr.content, wr.created_at, wr.user_id,
                   u.name AS user_name, u.department
            FROM weekly_reports wr
            JOIN users u ON wr.user_id = u.id
            WHERE u.department = %s
            ORDER BY wr.report_date DESC
        ''', (dept_filter,))
    else:
        c.execute('''
            SELECT wr.id, wr.report_date, wr.content, wr.created_at, wr.user_id,
                   u.name AS user_name, u.department
            FROM weekly_reports wr
            JOIN users u ON wr.user_id = u.id
            ORDER BY wr.report_date DESC
        ''')
    reports = c.fetchall()
    release_db(conn)
    return render_template('report_list.html', reports=reports,
                           is_mgr=is_mgr, dept_filter=dept_filter,
                           current_uid=uid)


@app.route('/reports/new', methods=['GET', 'POST'])
@login_required
def report_new():
    conn = get_db()
    c = conn.cursor()
    uid = session['user_id']
    is_mgr, dept_filter = _get_user_scope(c, uid)

    if request.method == 'POST':
        report_date_str = request.form.get('report_date', '').strip()
        content = request.form.get('content', '').strip()
        if not report_date_str or not content:
            flash('請填寫報告日期與報告內容', 'danger')
            release_db(conn)
            return redirect(url_for('report_new'))
        try:
            report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
        except ValueError:
            flash('日期格式錯誤', 'danger')
            release_db(conn)
            return redirect(url_for('report_new'))
        try:
            c.execute('''
                INSERT INTO weekly_reports (report_date, user_id, content)
                VALUES (%s, %s, %s) RETURNING id
            ''', (report_date, uid, content))
            new_id = c.fetchone()['id']
            conn.commit()
            release_db(conn)
            flash('週報已建立', 'success')
            return redirect(url_for('report_detail', report_id=new_id))
        except pg_errors.UniqueViolation:
            conn.rollback()
            release_db(conn)
            flash('本週已有一份週報，請編輯現有週報', 'warning')
            return redirect(url_for('report_list'))

    report_date_str = request.args.get('report_date', date.today().isoformat())
    c.execute('SELECT name, department FROM users WHERE id=%s', (uid,))
    me = c.fetchone()
    prefill = _build_report_prefill(c, uid, report_date_str, is_mgr, dept_filter)
    release_db(conn)
    return render_template('report_form.html', mode='new', report=None,
                           me=me, report_date=report_date_str, prefill=prefill)


@app.route('/reports/<int:report_id>')
@login_required
def report_detail(report_id):
    conn = get_db()
    c = conn.cursor()
    uid = session['user_id']
    is_mgr, dept_filter = _get_user_scope(c, uid)

    c.execute('''
        SELECT wr.id, wr.report_date, wr.content, wr.created_at, wr.updated_at, wr.user_id,
               u.name AS user_name, u.department
        FROM weekly_reports wr
        JOIN users u ON wr.user_id = u.id
        WHERE wr.id = %s
    ''', (report_id,))
    report = c.fetchone()
    release_db(conn)

    if not report:
        abort(404)
    if report['user_id'] != uid:
        if not is_mgr:
            abort(403)
        if dept_filter and report['department'] != dept_filter:
            abort(403)

    return render_template('report_detail.html', report=report,
                           can_edit=(report['user_id'] == uid))


@app.route('/reports/<int:report_id>/edit', methods=['GET', 'POST'])
@login_required
def report_edit(report_id):
    conn = get_db()
    c = conn.cursor()
    uid = session['user_id']

    c.execute('''
        SELECT wr.*, u.name AS user_name, u.department
        FROM weekly_reports wr
        JOIN users u ON wr.user_id = u.id
        WHERE wr.id = %s
    ''', (report_id,))
    report = c.fetchone()

    if not report or report['user_id'] != uid:
        release_db(conn)
        abort(403)

    if request.method == 'POST':
        content = request.form.get('content', '').strip()
        report_date_str = request.form.get('report_date', '').strip()
        if not content or not report_date_str:
            flash('請填寫完整資訊', 'danger')
            release_db(conn)
            return redirect(url_for('report_edit', report_id=report_id))
        try:
            report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()
        except ValueError:
            flash('日期格式錯誤', 'danger')
            release_db(conn)
            return redirect(url_for('report_edit', report_id=report_id))
        try:
            c.execute('''
                UPDATE weekly_reports SET content=%s, report_date=%s, updated_at=NOW()
                WHERE id=%s AND user_id=%s
            ''', (content, report_date, report_id, uid))
            conn.commit()
            release_db(conn)
            flash('週報已更新', 'success')
            return redirect(url_for('report_detail', report_id=report_id))
        except pg_errors.UniqueViolation:
            conn.rollback()
            release_db(conn)
            flash('該週已有其他週報，無法更改至同一週', 'warning')
            return redirect(url_for('report_edit', report_id=report_id))

    me = {'name': report['user_name'], 'department': report['department']}
    release_db(conn)
    return render_template('report_form.html', mode='edit', report=report,
                           me=me, report_date=report['report_date'].isoformat(),
                           prefill=report['content'])


@app.route('/reports/<int:report_id>/delete', methods=['POST'])
@login_required
def report_delete(report_id):
    conn = get_db()
    c = conn.cursor()
    uid = session['user_id']
    c.execute('SELECT user_id FROM weekly_reports WHERE id=%s', (report_id,))
    row = c.fetchone()
    if not row or row['user_id'] != uid:
        release_db(conn)
        abort(403)
    c.execute('DELETE FROM weekly_reports WHERE id=%s', (report_id,))
    conn.commit()
    release_db(conn)
    flash('週報已刪除', 'success')
    return redirect(url_for('report_list'))


@app.route('/reports/calendar')
@login_required
def report_calendar():
    conn = get_db()
    c = conn.cursor()
    uid = session['user_id']
    is_mgr, dept_filter = _get_user_scope(c, uid)

    if not is_mgr:
        release_db(conn)
        abort(403)

    today = date.today()
    try:
        year  = int(request.args.get('year',  today.year))
        month = int(request.args.get('month', today.month))
        if not (1 <= month <= 12):
            month = today.month
    except ValueError:
        year, month = today.year, today.month

    first_day = date(year, month, 1)
    last_day  = date(year + (month // 12), month % 12 + 1, 1) - timedelta(days=1)

    if dept_filter:
        c.execute('''
            SELECT wr.id, wr.report_date, u.name AS user_name, u.department
            FROM weekly_reports wr
            JOIN users u ON wr.user_id = u.id
            WHERE u.department = %s
              AND wr.report_date >= %s AND wr.report_date <= %s
            ORDER BY wr.report_date
        ''', (dept_filter, first_day, last_day))
    else:
        c.execute('''
            SELECT wr.id, wr.report_date, u.name AS user_name, u.department
            FROM weekly_reports wr
            JOIN users u ON wr.user_id = u.id
            WHERE wr.report_date >= %s AND wr.report_date <= %s
            ORDER BY wr.report_date
        ''', (first_day, last_day))
    reports = c.fetchall()
    release_db(conn)

    by_date = {}
    for r in reports:
        by_date.setdefault(r['report_date'], []).append(r)

    # Build full 7-day weeks (Mon–Sun)
    weeks = []
    cur = first_day - timedelta(days=first_day.weekday())
    while cur <= last_day:
        week = []
        for i in range(7):
            d = cur + timedelta(days=i)
            week.append({
                'date':     d,
                'in_month': d.month == month,
                'reports':  by_date.get(d, []),
                'is_today': d == today,
            })
        weeks.append(week)
        cur += timedelta(days=7)

    prev_month = month - 1 or 12
    prev_year  = year - (1 if month == 1 else 0)
    next_month = month % 12 + 1
    next_year  = year + (1 if month == 12 else 0)

    month_names = ['', '一月', '二月', '三月', '四月', '五月', '六月',
                   '七月', '八月', '九月', '十月', '十一月', '十二月']

    return render_template('report_calendar.html',
                           weeks=weeks, year=year, month=month,
                           month_name=month_names[month],
                           prev_year=prev_year, prev_month=prev_month,
                           next_year=next_year, next_month=next_month,
                           dept_filter=dept_filter, is_mgr=is_mgr)


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
