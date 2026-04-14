require('dotenv').config();
const express = require('express');
const mysql   = require('mysql2/promise');
const bcrypt  = require('bcryptjs');
const jwt     = require('jsonwebtoken');

const app = express();
app.use(express.json());

const JWT_SECRET  = process.env.JWT_SECRET || 'changeme';
const JWT_EXPIRES = '8h';

// ─── DB POOL ──────────────────────────────────────────────────────────────────
let pool;
async function getPool() {
  if (!pool) pool = mysql.createPool({
    host: process.env.DB_HOST, user: process.env.DB_USER,
    password: process.env.DB_PASS, database: process.env.DB_NAME,
    waitForConnections: true, connectionLimit: 10, charset: 'utf8mb4',
  });
  return pool;
}

// ─── INIT DB ──────────────────────────────────────────────────────────────────
async function initDB() {
  const db = await getPool();

  await db.execute(`CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    role ENUM('admin','user','requester') DEFAULT 'user',
    full_name VARCHAR(255),
    job_title VARCHAR(255),
    permissions TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) CHARACTER SET utf8mb4`);
  await db.execute(`ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name VARCHAR(255)`).catch(()=>{});
  await db.execute(`ALTER TABLE users ADD COLUMN IF NOT EXISTS job_title VARCHAR(255)`).catch(()=>{});
  await db.execute(`ALTER TABLE users ADD COLUMN IF NOT EXISTS permissions TEXT`).catch(()=>{});

  await db.execute(`CREATE TABLE IF NOT EXISTS lookups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    type ENUM('client_name','region','feed_type') NOT NULL,
    value VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_lookup (type, value)
  ) CHARACTER SET utf8mb4`);

  await db.execute(`CREATE TABLE IF NOT EXISTS suppliers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) CHARACTER SET utf8mb4`);

  await db.execute(`CREATE TABLE IF NOT EXISTS supplier_items (
    id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_supplier_item (supplier_id, name),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id) ON DELETE CASCADE
  ) CHARACTER SET utf8mb4`);

  await db.execute(`CREATE TABLE IF NOT EXISTS clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    activity VARCHAR(255),
    feed_type VARCHAR(100),
    phone VARCHAR(50),
    area VARCHAR(100),
    cons_type ENUM('daily','weekly') DEFAULT 'daily',
    cons DECIMAL(10,2) DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) CHARACTER SET utf8mb4`);

  await db.execute(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS created_by INT NULL`).catch(()=>{});

  await db.execute(`CREATE TABLE IF NOT EXISTS contacts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    phone VARCHAR(50),
    activity VARCHAR(255),
    area VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) CHARACTER SET utf8mb4`);

  await db.execute(`CREATE TABLE IF NOT EXISTS pending_contacts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    activity VARCHAR(255),
    area VARCHAR(100),
    client_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_pc (name)
  ) CHARACTER SET utf8mb4`);

  await db.execute(`CREATE TABLE IF NOT EXISTS pending_lookups (
    id INT AUTO_INCREMENT PRIMARY KEY,
    type ENUM('client_name','region','feed_type') NOT NULL,
    value VARCHAR(255) NOT NULL,
    client_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_pending (type, value)
  ) CHARACTER SET utf8mb4`);


  await db.execute(`ALTER TABLE users MODIFY COLUMN role ENUM('admin','user','requester') DEFAULT 'user'`).catch(()=>{});

  await db.execute(`CREATE TABLE IF NOT EXISTS order_requests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    client_name VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    area VARCHAR(100),
    employee_name VARCHAR(255),
    feed_type VARCHAR(100),
    qty DECIMAL(10,2) NOT NULL,
    requested_date DATE,
    order_time TIME,
    notes TEXT,
    status ENUM('pending','approved','rejected') DEFAULT 'pending',
    delivery_status ENUM('loading_wait','delivery_wait','in_delivery','delivered') DEFAULT NULL,
    price DECIMAL(12,2) DEFAULT NULL,
    admin_note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  ) CHARACTER SET utf8mb4`);
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS phone VARCHAR(50)`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS area VARCHAR(100)`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS employee_name VARCHAR(255)`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS order_time TIME`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS responsible_name VARCHAR(255)`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS match_status ENUM('matched','non_matched','under_review') DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests MODIFY COLUMN status ENUM('pending','approved','rejected') DEFAULT 'pending'`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS order_items JSON DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS invoice_number VARCHAR(100) DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS supplier VARCHAR(255) DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS purchase_date DATE DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS purchase_qty DECIMAL(10,2) DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS purchase_invoice VARCHAR(100) DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS purchase_price DECIMAL(12,2) DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS supplier_payment_status VARCHAR(30) DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS sale_date DATE DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS client_payment_status VARCHAR(30) DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS purchase_feed_type VARCHAR(255) DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS purchase_items JSON DEFAULT NULL`).catch(()=>{});
  await db.execute(`ALTER TABLE order_requests ADD COLUMN IF NOT EXISTS seq_no INT DEFAULT NULL`).catch(()=>{});
  // backfill any rows missing seq_no, preserving chronological order
  try {
    const [[{cnt}]] = await db.execute('SELECT COUNT(*) AS cnt FROM order_requests WHERE seq_no IS NULL');
    if (cnt > 0) {
      await db.execute('SET @n := (SELECT COALESCE(MAX(seq_no),0) FROM order_requests)');
      await db.execute('UPDATE order_requests SET seq_no = (@n := @n + 1) WHERE seq_no IS NULL ORDER BY created_at, id');
    }
  } catch(e) {}
  await db.execute(`CREATE TABLE IF NOT EXISTS order_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    user_id INT NOT NULL,
    action VARCHAR(50) NOT NULL,
    changes JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES order_requests(id) ON DELETE CASCADE
  ) CHARACTER SET utf8mb4`);

  await db.execute(`CREATE TABLE IF NOT EXISTS order_chats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    user_id INT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES order_requests(id) ON DELETE CASCADE,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
  ) CHARACTER SET utf8mb4`);

  // seed default admin
  const [[{cnt}]] = await db.execute('SELECT COUNT(*) AS cnt FROM users');
  if (cnt === 0) {
    const hash = await bcrypt.hash('admin123', 10);
    await db.execute("INSERT INTO users (username,password_hash,role) VALUES ('admin',?,'admin')", [hash]);
    console.log('Default admin: admin / admin123');
  }
  console.log('DB ready');
}

// ─── AUTH MIDDLEWARE ──────────────────────────────────────────────────────────
function auth(req, res, next) {
  const token = (req.headers.authorization || '').replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'غير مصرح' });
  try { req.user = jwt.verify(token, JWT_SECRET); next(); }
  catch { res.status(401).json({ error: 'رمز غير صالح' }); }
}
function adminOnly(req, res, next) {
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'للمديرين فقط' });
  next();
}
function hasPerm(perm) {
  return (req, res, next) => {
    if (req.user.role === 'admin') return next();
    const perms = req.user.permissions || [];
    if (perms.includes(perm)) return next();
    res.status(403).json({ error: 'ليس لديك صلاحية' });
  };
}

// ─── AUTH ROUTES ─────────────────────────────────────────────────────────────
app.post('/auth/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'أدخل اسم المستخدم وكلمة المرور' });
  try {
    const db = await getPool();
    const [[user]] = await db.execute('SELECT * FROM users WHERE username = ?', [username]);
    if (!user || !(await bcrypt.compare(password, user.password_hash)))
      return res.status(401).json({ error: 'بيانات الدخول غير صحيحة' });
    const perms = user.permissions ? JSON.parse(user.permissions) : [];
    const token = jwt.sign({ id: user.id, username: user.username, role: user.role, full_name: user.full_name, job_title: user.job_title, permissions: perms }, JWT_SECRET, { expiresIn: JWT_EXPIRES });
    const permsData = user.permissions ? JSON.parse(user.permissions) : [];
    res.json({ token, user: { id: user.id, username: user.username, role: user.role, full_name: user.full_name, job_title: user.job_title, permissions: permsData } });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.get('/auth/me', auth, async (req, res) => {
  try {
    const db = await getPool();
    const [[u]] = await db.execute('SELECT id,username,role,full_name,job_title,permissions FROM users WHERE id=?', [req.user.id]);
    if (!u) return res.status(401).json({ error: 'غير موجود' });
    const perms = u.permissions ? JSON.parse(u.permissions) : [];
    res.json({ ...req.user, full_name: u.full_name, job_title: u.job_title, permissions: perms });
  } catch (err) { res.json(req.user); }
});

// ─── USERS ROUTES ─────────────────────────────────────────────────────────────
app.get('/users', auth, hasPerm('manage_users'), async (req, res) => {
  const db = await getPool();
  const [rows] = await db.execute('SELECT id,username,role,full_name,job_title,permissions,created_at FROM users ORDER BY created_at');
  res.json(rows);
});
app.post('/users', auth, hasPerm('manage_users'), async (req, res) => {
  const { username, password, role, full_name, job_title, permissions } = req.body;
  if (!username || !password) return res.status(400).json({ error: 'اسم المستخدم وكلمة المرور مطلوبان' });
  try {
    const db = await getPool();
    const hash = await bcrypt.hash(password, 10);
    const permsJson = permissions ? JSON.stringify(permissions) : null;
    const [r] = await db.execute('INSERT INTO users (username,password_hash,role,full_name,job_title,permissions) VALUES (?,?,?,?,?,?)', [username, hash, ['admin','user','requester'].includes(role) ? role : 'user', full_name||null, job_title||null, permsJson]);
    const [[u]] = await db.execute('SELECT id,username,role,full_name,job_title,permissions,created_at FROM users WHERE id=?', [r.insertId]);
    res.status(201).json(u);
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'اسم المستخدم موجود مسبقاً' });
    res.status(500).json({ error: err.message });
  }
});
app.put('/users/:id', auth, hasPerm('manage_users'), async (req, res) => {
  let { username, password, role, full_name, job_title, permissions } = req.body;
  try {
    const db = await getPool();
    // prevent admin from changing their own role or permissions
    if (parseInt(req.params.id) === req.user.id) {
      const [[self]] = await db.execute('SELECT role,permissions FROM users WHERE id=?', [req.user.id]);
      role = self.role;
      permissions = self.permissions ? JSON.parse(self.permissions) : [];
    }
    const permsJson = permissions ? JSON.stringify(permissions) : null;
    const safeRole = ['admin','user','requester'].includes(role) ? role : 'user';
    if (password) {
      const hash = await bcrypt.hash(password, 10);
      await db.execute('UPDATE users SET username=?,password_hash=?,role=?,full_name=?,job_title=?,permissions=? WHERE id=?', [username, hash, safeRole, full_name||null, job_title||null, permsJson, req.params.id]);
    } else {
      await db.execute('UPDATE users SET username=?,role=?,full_name=?,job_title=?,permissions=? WHERE id=?', [username, safeRole, full_name||null, job_title||null, permsJson, req.params.id]);
    }
    const [[u]] = await db.execute('SELECT id,username,role,full_name,job_title,permissions,created_at FROM users WHERE id=?', [req.params.id]);
    res.json(u);
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'اسم المستخدم موجود مسبقاً' });
    res.status(500).json({ error: err.message });
  }
});
app.delete('/users/:id', auth, hasPerm('manage_users'), async (req, res) => {
  if (parseInt(req.params.id) === req.user.id) return res.status(400).json({ error: 'لا يمكنك حذف حسابك الخاص' });
  const db = await getPool();
  await db.execute('DELETE FROM users WHERE id=?', [req.params.id]);
  res.json({ success: true });
});

// ─── LOOKUPS ROUTES ───────────────────────────────────────────────────────────
app.get('/lookups', auth, async (req, res) => {
  const db = await getPool();
  const where = req.query.type ? 'WHERE type=?' : '';
  const params = req.query.type ? [req.query.type] : [];
  const [rows] = await db.execute(`SELECT * FROM lookups ${where} ORDER BY type, value`, params);
  res.json(rows);
});
function adminOrLookups(req, res, next) {
  if (req.user.role === 'admin' || (req.user.permissions||[]).includes('lookups')) return next();
  return res.status(403).json({ error: 'ليس لديك صلاحية' });
}

app.post('/lookups', auth, adminOrLookups, async (req, res) => {
  const { type, value } = req.body;
  if (!type || !value) return res.status(400).json({ error: 'النوع والقيمة مطلوبان' });
  try {
    const db = await getPool();
    const [r] = await db.execute('INSERT INTO lookups (type,value) VALUES (?,?)', [type, value.trim()]);
    const [[row]] = await db.execute('SELECT * FROM lookups WHERE id=?', [r.insertId]);
    res.status(201).json(row);
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'القيمة موجودة مسبقاً' });
    res.status(500).json({ error: err.message });
  }
});
app.delete('/lookups/:id', auth, adminOrLookups, async (req, res) => {
  const db = await getPool();
  await db.execute('DELETE FROM lookups WHERE id=?', [req.params.id]);
  res.json({ success: true });
});

// ─── SUPPLIERS ROUTES ─────────────────────────────────────────────────────────
app.get('/suppliers', auth, async (req, res) => {
  try {
    const db = await getPool();
    const [sups] = await db.execute('SELECT id,name FROM suppliers ORDER BY name');
    const [items] = await db.execute('SELECT id,supplier_id,name FROM supplier_items ORDER BY name');
    const byId = {};
    sups.forEach(s => { s.items = []; byId[s.id] = s; });
    items.forEach(it => { if (byId[it.supplier_id]) byId[it.supplier_id].items.push({ id: it.id, name: it.name }); });
    res.json(sups);
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/suppliers', auth, adminOrLookups, async (req, res) => {
  const { name } = req.body;
  if (!name || !name.trim()) return res.status(400).json({ error: 'اسم المورد مطلوب' });
  try {
    const db = await getPool();
    const [r] = await db.execute('INSERT INTO suppliers (name) VALUES (?)', [name.trim()]);
    res.status(201).json({ id: r.insertId, name: name.trim(), items: [] });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'المورد موجود مسبقاً' });
    res.status(500).json({ error: err.message });
  }
});
app.delete('/suppliers/:id', auth, adminOrLookups, async (req, res) => {
  try {
    const db = await getPool();
    await db.execute('DELETE FROM suppliers WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/suppliers/:id/items', auth, adminOrLookups, async (req, res) => {
  const { name } = req.body;
  if (!name || !name.trim()) return res.status(400).json({ error: 'اسم الصنف مطلوب' });
  try {
    const db = await getPool();
    const [[sup]] = await db.execute('SELECT id FROM suppliers WHERE id=?', [req.params.id]);
    if (!sup) return res.status(404).json({ error: 'المورد غير موجود' });
    const [r] = await db.execute('INSERT INTO supplier_items (supplier_id,name) VALUES (?,?)', [req.params.id, name.trim()]);
    res.status(201).json({ id: r.insertId, supplier_id: parseInt(req.params.id), name: name.trim() });
  } catch (err) {
    if (err.code === 'ER_DUP_ENTRY') return res.status(400).json({ error: 'الصنف موجود مسبقاً لهذا المورد' });
    res.status(500).json({ error: err.message });
  }
});
app.delete('/suppliers/:id/items/:itemId', auth, adminOrLookups, async (req, res) => {
  try {
    const db = await getPool();
    await db.execute('DELETE FROM supplier_items WHERE id=? AND supplier_id=?', [req.params.itemId, req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── PENDING LOOKUPS ROUTES ──────────────────────────────────────────────────
app.get('/lookups/pending', auth, adminOnly, async (req, res) => {
  const db = await getPool();
  const [rows] = await db.execute(
    'SELECT p.*, c.name AS client_name FROM pending_lookups p LEFT JOIN clients c ON p.client_id=c.id ORDER BY p.created_at DESC'
  );
  res.json(rows);
});
app.post('/lookups/pending/:id/accept', auth, adminOnly, async (req, res) => {
  const db = await getPool();
  const [[row]] = await db.execute('SELECT * FROM pending_lookups WHERE id=?', [req.params.id]);
  if (!row) return res.status(404).json({ error: 'غير موجود' });
  try {
    await db.execute('INSERT INTO lookups (type,value) VALUES (?,?)', [row.type, row.value]);
  } catch (e) { if (e.code !== 'ER_DUP_ENTRY') return res.status(500).json({ error: e.message }); }
  await db.execute('DELETE FROM pending_lookups WHERE id=?', [req.params.id]);
  res.json({ success: true });
});
app.delete('/lookups/pending/:id', auth, adminOnly, async (req, res) => {
  const db = await getPool();
  await db.execute('DELETE FROM pending_lookups WHERE id=?', [req.params.id]);
  res.json({ success: true });
});

// ─── CONTACTS ROUTES ──────────────────────────────────────────────────────────
app.get('/contacts', auth, async (req, res) => {
  const db = await getPool();
  const [rows] = await db.execute('SELECT * FROM contacts ORDER BY name');
  res.json(rows);
});
app.post('/contacts', auth, adminOrLookups, async (req, res) => {
  const { name, phone, activity, area } = req.body;
  if (!name) return res.status(400).json({ error: 'الاسم مطلوب' });
  try {
    const db = await getPool();
    const [r] = await db.execute(
      'INSERT INTO contacts (name,phone,activity,area) VALUES (?,?,?,?)',
      [name.trim(), phone||null, activity||null, area||null]
    );
    const [[row]] = await db.execute('SELECT * FROM contacts WHERE id=?', [r.insertId]);
    res.status(201).json(row);
  } catch(e) {
    if (e.code==='ER_DUP_ENTRY') return res.status(400).json({ error: 'الاسم موجود مسبقاً' });
    res.status(500).json({ error: e.message });
  }
});
app.put('/contacts/:id', auth, adminOrLookups, async (req, res) => {
  const { name, phone, activity, area } = req.body;
  if (!name) return res.status(400).json({ error: 'الاسم مطلوب' });
  try {
    const db = await getPool();
    await db.execute('UPDATE contacts SET name=?,phone=?,activity=?,area=? WHERE id=?',
      [name.trim(), phone||null, activity||null, area||null, req.params.id]);
    const [[row]] = await db.execute('SELECT * FROM contacts WHERE id=?', [req.params.id]);
    res.json(row);
  } catch(e) {
    if (e.code==='ER_DUP_ENTRY') return res.status(400).json({ error: 'الاسم موجود مسبقاً' });
    res.status(500).json({ error: e.message });
  }
});
app.delete('/contacts/:id', auth, adminOrLookups, async (req, res) => {
  const db = await getPool();
  await db.execute('DELETE FROM contacts WHERE id=?', [req.params.id]);
  res.json({ success: true });
});

// ─── PENDING CONTACTS ROUTES ──────────────────────────────────────────────────
app.get('/contacts/pending', auth, adminOnly, async (req, res) => {
  const db = await getPool();
  const [rows] = await db.execute(
    'SELECT pc.*, c.name AS client_ref FROM pending_contacts pc LEFT JOIN clients c ON pc.client_id=c.id ORDER BY pc.created_at DESC'
  );
  res.json(rows);
});
app.post('/contacts/pending/:id/accept', auth, adminOnly, async (req, res) => {
  const db = await getPool();
  const [[row]] = await db.execute('SELECT * FROM pending_contacts WHERE id=?', [req.params.id]);
  if (!row) return res.status(404).json({ error: 'غير موجود' });
  try {
    await db.execute('INSERT INTO contacts (name,phone,activity,area) VALUES (?,?,?,?)',
      [row.name, row.phone, row.activity, row.area]);
  } catch(e) {
    if (e.code !== 'ER_DUP_ENTRY') return res.status(500).json({ error: e.message });
    // already exists — just remove from pending
  }
  await db.execute('DELETE FROM pending_contacts WHERE id=?', [req.params.id]);
  res.json({ success: true });
});
app.delete('/contacts/pending/:id', auth, adminOnly, async (req, res) => {
  const db = await getPool();
  await db.execute('DELETE FROM pending_contacts WHERE id=?', [req.params.id]);
  res.json({ success: true });
});

// helper: queue values not already in lookups
async function queuePending(db, type, value, clientId) {
  if (!value || !value.trim()) return;
  const [[existing]] = await db.execute('SELECT id FROM lookups WHERE type=? AND value=?', [type, value.trim()]);
  if (existing) return; // already in lookups, skip
  await db.execute(
    'INSERT IGNORE INTO pending_lookups (type,value,client_id) VALUES (?,?,?)',
    [type, value.trim(), clientId]
  );
}

// helper: queue contact if not already saved
async function queuePendingContact(db, name, phone, activity, area, clientId) {
  if (!name || !name.trim()) return;
  const [[existing]] = await db.execute('SELECT id FROM contacts WHERE name=?', [name.trim()]);
  if (existing) return;
  await db.execute(
    'INSERT IGNORE INTO pending_contacts (name,phone,activity,area,client_id) VALUES (?,?,?,?,?)',
    [name.trim(), phone||null, activity||null, area||null, clientId]
  );
}

// ─── CLIENTS ROUTES ───────────────────────────────────────────────────────────
app.get('/clients', auth, async (req, res) => {
  try {
    const db = await getPool();
    const [clients] = await db.execute('SELECT c.*, u.username AS created_by_name FROM clients c LEFT JOIN users u ON c.created_by = u.id ORDER BY c.created_at DESC');
    res.json(clients);
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/clients', auth, async (req, res) => {
  try {
    const { name, activity, feed_type, phone, area, cons_type, cons, notes } = req.body;
    const db = await getPool();
    const [r] = await db.execute(
      'INSERT INTO clients (name,activity,feed_type,phone,area,cons_type,cons,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?)',
      [name, activity||null, feed_type||null, phone||null, area||null, cons_type||'daily', cons||0, notes||null, req.user.id]
    );
    const clientId = r.insertId;
    await queuePendingContact(db, name, phone, activity, area, clientId);
    await queuePending(db, 'region', area, clientId);
    await queuePending(db, 'feed_type', feed_type, clientId);

    const [[client]] = await db.execute('SELECT * FROM clients WHERE id=?', [clientId]);
    res.status(201).json(client);
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.delete('/clients/:id', auth, async (req, res) => {
  try {
    const db = await getPool();
    await db.execute('DELETE FROM clients WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ─── CONSUMPTION & ORDER RATE ANALYTICS ──────────────────────────────────────
// GET /analytics/consumption-rates?days=90
app.get('/analytics/consumption-rates', auth, async (req, res) => {
  try {
    const db = await getPool();
    const daysInt = Math.max(1, parseInt(req.query.days) || 90);

    const [rows] = await db.execute(`
      SELECT
        client_name AS name,
        MAX(feed_type) AS feed_type,
        MAX(area) AS area,
        COUNT(*) AS order_count,
        COALESCE(SUM(qty),0) AS total_qty,
        MIN(created_at) AS first_order,
        MAX(created_at) AS last_order,
        DATEDIFF(CURDATE(), MIN(created_at)) AS days_since_first
      FROM order_requests
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
      GROUP BY client_name
      ORDER BY total_qty DESC
    `, [daysInt]);

    const clients = rows.map(row => {
      const activeDays  = Math.max(1, Math.min(Number(row.days_since_first) || daysInt, daysInt));
      const totalQty    = parseFloat(row.total_qty)  || 0;
      const orderCount  = parseInt(row.order_count)  || 0;
      const dqty        = totalQty   / activeDays;
      const dord        = orderCount / activeDays;
      return {
        name: row.name, feed_type: row.feed_type, area: row.area,
        period_days: daysInt, order_count: orderCount, total_qty: +totalQty.toFixed(2),
        first_purchase: row.first_order, last_purchase: row.last_order,
        consumption_rate: {
          daily:   +(dqty      ).toFixed(2),
          weekly:  +(dqty *  7 ).toFixed(2),
          monthly: +(dqty * 30 ).toFixed(2),
        },
        order_rate: {
          daily:   +(dord      ).toFixed(3),
          weekly:  +(dord *  7 ).toFixed(2),
          monthly: +(dord * 30 ).toFixed(2),
        },
      };
    });

    const summary = { client_count: clients.length, total_qty: 0, order_count: 0 };
    for (const c of clients) {
      summary.total_qty += c.total_qty;
      summary.order_count += c.order_count;
    }
    const sdq = summary.total_qty / daysInt;
    const sdo = summary.order_count / daysInt;
    summary.total_qty = +summary.total_qty.toFixed(2);
    summary.consumption_rate = {
      daily:   +(sdq).toFixed(2),
      weekly:  +(sdq * 7).toFixed(2),
      monthly: +(sdq * 30).toFixed(2),
    };
    summary.order_rate = {
      daily:   +(sdo).toFixed(3),
      weekly:  +(sdo * 7).toFixed(2),
      monthly: +(sdo * 30).toFixed(2),
    };

    res.json({ period_days: daysInt, summary, clients });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── ORDER REQUESTS ANALYTICS ────────────────────────────────────────────────
// GET /analytics/orders?days=90
app.get('/analytics/orders', auth, async (req, res) => {
  try {
    const db = await getPool();
    const daysInt = Math.max(1, parseInt(req.query.days) || 90);

    const [[orSummary]] = await db.execute(`
      SELECT
        COUNT(*)                                                          AS total_orders,
        SUM(CASE WHEN status='approved'  THEN 1 ELSE 0 END)              AS approved,
        SUM(CASE WHEN status='rejected'  THEN 1 ELSE 0 END)              AS rejected,
        SUM(CASE WHEN status='pending'   THEN 1 ELSE 0 END)              AS pending,
        COALESCE(SUM(qty),0)                                              AS total_qty,
        COALESCE(SUM(CASE WHEN status='approved' THEN qty*price ELSE 0 END),0) AS total_revenue
      FROM order_requests
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
    `, [daysInt]);

    const [orClients] = await db.execute(`
      SELECT
        client_name,
        COUNT(*) AS order_count,
        COALESCE(SUM(qty),0) AS total_qty,
        COALESCE(SUM(CASE WHEN status='approved' THEN qty*price ELSE 0 END),0) AS total_revenue,
        MIN(created_at) AS first_order,
        MAX(created_at) AS last_order,
        DATEDIFF(CURDATE(), MIN(created_at)) AS days_since_first
      FROM order_requests
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
      GROUP BY client_name
      ORDER BY total_qty DESC
    `, [daysInt]);

    const [orFeeds] = await db.execute(`
      SELECT client_name, feed_type, COALESCE(SUM(qty),0) AS qty
      FROM order_requests
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
      GROUP BY client_name, feed_type
      ORDER BY client_name, qty DESC
    `, [daysInt]);

    const [orPrev] = await db.execute(`
      SELECT client_name, COALESCE(SUM(qty),0) AS total_qty, COUNT(*) AS order_count
      FROM order_requests
      WHERE created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
        AND created_at < DATE_SUB(NOW(), INTERVAL ? DAY)
      GROUP BY client_name
    `, [daysInt * 2, daysInt]);

    const prevMap = {};
    for (const r of orPrev) prevMap[r.client_name] = { qty: parseFloat(r.total_qty)||0, orders: +r.order_count };

    const feedMap = {};
    for (const f of orFeeds) {
      if (!feedMap[f.client_name]) feedMap[f.client_name] = [];
      feedMap[f.client_name].push({ feed_type: f.feed_type || '-', qty: +parseFloat(f.qty).toFixed(0) });
    }

    const BAGS_PER_TRUCK = 600;
    const clientsData = orClients.map(c => {
      const totalBags  = parseFloat(c.total_qty) || 0;
      const totalTrucks = +(totalBags / BAGS_PER_TRUCK).toFixed(2);
      const orderCount = parseInt(c.order_count) || 0;
      const activeDays = Math.max(1, Math.min(Number(c.days_since_first) || daysInt, daysInt));
      const dqty = totalBags / activeDays;
      const dord = orderCount / activeDays;
      return {
        client_name:    c.client_name,
        order_count:    orderCount,
        total_bags:     +totalBags.toFixed(0),
        total_trucks:   totalTrucks,
        total_revenue:  +parseFloat(c.total_revenue).toFixed(2),
        feed_breakdown: feedMap[c.client_name] || [],
        prev_qty:       (prevMap[c.client_name]||{}).qty || 0,
        prev_orders:    (prevMap[c.client_name]||{}).orders || 0,
        change_pct:     (prevMap[c.client_name]||{}).qty ? +(((totalBags - (prevMap[c.client_name].qty)) / prevMap[c.client_name].qty) * 100).toFixed(1) : null,
        first_order:    c.first_order,
        last_order:     c.last_order,
        consumption_rate: {
          daily:   +(dqty).toFixed(2),
          weekly:  +(dqty * 7).toFixed(2),
          monthly: +(dqty * 30).toFixed(0),
          yearly:  +(dqty * 365).toFixed(0),
        },
        order_rate: {
          daily:   +(dord).toFixed(3),
          weekly:  +(dord * 7).toFixed(2),
          monthly: +(dord * 30).toFixed(2),
        },
      };
    });

    const totalBagsAll  = clientsData.reduce((s,c) => s + c.total_bags, 0);
    const totalOrderAll = clientsData.reduce((s,c) => s + c.order_count, 0);
    const dqtyAll = totalBagsAll / daysInt;
    const dordAll = totalOrderAll / daysInt;

    res.json({
      period_days: daysInt,
      summary: {
        total_orders:    +orSummary.total_orders,
        approved:        +orSummary.approved || 0,
        rejected:        +orSummary.rejected || 0,
        pending:         +orSummary.pending || 0,
        total_qty:       +parseFloat(orSummary.total_qty).toFixed(2),
        total_revenue:   +parseFloat(orSummary.total_revenue).toFixed(2),
        client_count:    clientsData.length,
        total_bags:      +totalBagsAll.toFixed(0),
        consumption_rate: {
          daily:   +(dqtyAll).toFixed(2),
          weekly:  +(dqtyAll * 7).toFixed(2),
          monthly: +(dqtyAll * 30).toFixed(0),
          yearly:  +(dqtyAll * 365).toFixed(0),
        },
        order_rate: {
          weekly:  +(dordAll * 7).toFixed(2),
        },
      },
      clients: clientsData,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /clients/:id/consumption-rate?days=90
app.get('/clients/:id/consumption-rate', auth, async (req, res) => {
  try {
    const db = await getPool();
    const daysInt = Math.max(1, parseInt(req.query.days) || 90);

    const [[client]] = await db.execute('SELECT id,name,feed_type,area FROM clients WHERE id=?', [req.params.id]);
    if (!client) return res.status(404).json({ error: 'العميل غير موجود' });

    const [orders] = await db.execute(
      `SELECT DATE(created_at) AS date, qty FROM order_requests
       WHERE client_name=? AND created_at >= DATE_SUB(NOW(), INTERVAL ? DAY)
       ORDER BY created_at ASC`,
      [client.name, daysInt]
    );

    const totalQty   = orders.reduce((s, p) => s + parseFloat(p.qty), 0);
    const orderCount = orders.length;
    const activeDays = orders.length > 0
      ? Math.max(1, Math.min(daysInt,
          Math.ceil((Date.now() - new Date(orders[0].date)) / 86400000) || 1))
      : daysInt;

    const dqty = totalQty   / activeDays;
    const dord = orderCount / activeDays;

    res.json({
      client,
      period_days: daysInt,
      order_count: orderCount,
      total_qty: +totalQty.toFixed(2),
      consumption_rate: {
        daily:   +(dqty      ).toFixed(2),
        weekly:  +(dqty *  7 ).toFixed(2),
        monthly: +(dqty * 30 ).toFixed(2),
      },
      order_rate: {
        daily:   +(dord      ).toFixed(3),
        weekly:  +(dord *  7 ).toFixed(2),
        monthly: +(dord * 30 ).toFixed(2),
      },
      purchases: orders,
    });
  } catch (err) { res.status(500).json({ error: err.message }); }
});


// ─── ORDER REQUESTS ROUTES ────────────────────────────────────────────────────
function requesterOrAdmin(req, res, next) {
  if (!req.user || (req.user.role !== 'admin' && req.user.role !== 'requester' && req.user.role !== 'user'))
    return res.status(403).json({ error: 'غير مصرح' });
  next();
}

app.get('/orders', auth, async (req, res) => {
  try {
    const db = await getPool();
    let rows;
    const canSeeAll = req.user.role === 'admin' || req.user.role === 'requester' || (req.user.permissions||[]).some(p=>['order_requests','manage_orders'].includes(p));
    if (canSeeAll) {
      [rows] = await db.execute(
        `SELECT o.*, u.username FROM order_requests o
         LEFT JOIN users u ON o.user_id = u.id
         ORDER BY o.created_at DESC`
      );
    } else {
      [rows] = await db.execute(
        `SELECT o.*, u.username FROM order_requests o
         LEFT JOIN users u ON o.user_id = u.id
         WHERE o.user_id = ?
         ORDER BY o.created_at DESC`,
        [req.user.id]
      );
    }
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/orders', auth, async (req, res) => {
  try {
    const { client_name, phone, area, employee_name, responsible_name, feed_type, qty, price, requested_date, order_time, notes, items } = req.body;
    let resolvedFeedType = feed_type || null;
    let resolvedQty = qty;
    let resolvedPrice = price || null;
    let resolvedItems = null;
    if (Array.isArray(items) && items.length > 0) {
      resolvedItems = JSON.stringify(items);
      resolvedQty = items.reduce((s, i) => s + (parseFloat(i.qty) || 0), 0);
      resolvedFeedType = items.length === 1 ? (items[0].feed_type || null) : items.map(i => i.feed_type).filter(Boolean).join('، ');
      resolvedPrice = items.length === 1 ? (items[0].price || null) : null;
    }
    const db = await getPool();
    // resolve purchase items
    let pItems = req.body.purchase_items;
    let pItemsJson = null, pFeed = req.body.purchase_feed_type||null, pQty = req.body.purchase_qty||null, pPrice = req.body.purchase_price||null;
    if (Array.isArray(pItems) && pItems.length > 0) {
      pItemsJson = JSON.stringify(pItems);
      pQty = pItems.reduce((s,i)=>s+(parseFloat(i.qty)||0),0) || null;
      pFeed = pItems.length === 1 ? (pItems[0].feed_type||null) : pItems.map(i=>i.feed_type).filter(Boolean).join('، ');
      pPrice = pItems.length === 1 ? (pItems[0].price||null) : null;
    }
    const [[{next_seq}]] = await db.execute('SELECT COALESCE(MAX(seq_no),0)+1 AS next_seq FROM order_requests');
    const [r] = await db.execute(
      'INSERT INTO order_requests (user_id,client_name,phone,area,employee_name,responsible_name,feed_type,qty,price,requested_date,order_time,notes,order_items,invoice_number,supplier,purchase_date,purchase_qty,purchase_invoice,purchase_price,purchase_feed_type,purchase_items,supplier_payment_status,delivery_status,sale_date,client_payment_status,seq_no) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [req.user.id, (client_name||'').trim()||null, phone||null, area||null, employee_name||null, responsible_name||null, resolvedFeedType, resolvedQty||null, resolvedPrice, requested_date||null, order_time||null, notes||null, resolvedItems, req.body.invoice_number||null, req.body.supplier||null, req.body.purchase_date||null, pQty, req.body.purchase_invoice||null, pPrice, pFeed, pItemsJson, req.body.supplier_payment_status||null, req.body.delivery_status||null, req.body.sale_date||null, req.body.client_payment_status||null, next_seq]
    );
    const [[row]] = await db.execute(
      'SELECT o.*, u.username FROM order_requests o LEFT JOIN users u ON o.user_id=u.id WHERE o.id=?',
      [r.insertId]
    );
    await db.execute(
      'INSERT INTO order_history (order_id,user_id,action,changes) VALUES (?,?,?,?)',
      [r.insertId, req.user.id, 'created', JSON.stringify({client_name,phone,area,responsible_name,feed_type:resolvedFeedType,qty:resolvedQty,price:resolvedPrice,requested_date,notes})]
    );
    res.status(201).json(row);
  } catch (err) { res.status(500).json({ error: err.message }); }
});


app.get('/orders/:id/history', auth, async (req, res) => {
  try {
    const db = await getPool();
    const [rows] = await db.execute(
      `SELECT h.*, u.username FROM order_history h
       LEFT JOIN users u ON h.user_id = u.id
       WHERE h.order_id = ? ORDER BY h.created_at DESC`,
      [req.params.id]
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/orders/:id/status', auth, (req,res,next)=>{
  if(req.user.role==='admin'||req.user.role==='requester'||(req.user.permissions||[]).some(p=>['manage_orders','edit_all_orders','order_requests'].includes(p))) return next();
  res.status(403).json({error:'ليس لديك صلاحية'});
}, async (req, res) => {
  try {
    const canChangeStatus = req.user.role==='admin' || req.user.role==='requester' || (req.user.permissions||[]).includes('manage_orders');
    let { status, delivery_status, admin_note } = req.body;
    if(!canChangeStatus){ status = undefined; admin_note = undefined; }
    if (status && !['approved','rejected','pending'].includes(status))
      return res.status(400).json({ error: 'حالة غير صالحة' });
    const db = await getPool();
    const fields = ['admin_note=?'];
    const vals = [admin_note||null];
    if (status) { fields.unshift('status=?'); vals.unshift(status); }
    if (delivery_status !== undefined) { fields.push('delivery_status=?'); vals.push(delivery_status||null); }
    vals.push(req.params.id);
    await db.execute('UPDATE order_requests SET '+fields.join(',')+' WHERE id=?', vals);
    const [[row]] = await db.execute(
      'SELECT o.*, u.username FROM order_requests o LEFT JOIN users u ON o.user_id=u.id WHERE o.id=?',
      [req.params.id]
    );
    if (!row) return res.status(404).json({ error: 'غير موجود' });
    const statusLabels = {pending:'قيد الانتظار',approved:'موافق عليه',rejected:'مرفوض',loading_wait:'بانتظار التحميل',delivery_wait:'بانتظار التسليم',in_delivery:'قيد التوصيل',delivered:'تم التسليم'};
    const changes = {};
    if (status) changes['الحالة'] = statusLabels[status]||status;
    if (delivery_status !== undefined && delivery_status !== null) changes['حالة التوصيل'] = statusLabels[delivery_status]||delivery_status;
    if (admin_note) changes['ملاحظة الإدارة'] = admin_note;
    await db.execute(
      'INSERT INTO order_history (order_id,user_id,action,changes) VALUES (?,?,?,?)',
      [req.params.id, req.user.id, 'status_updated', JSON.stringify(changes)]
    );
    res.json(row);
  } catch (err) { res.status(500).json({ error: err.message }); }
});


app.patch('/orders/:id/match-status', auth, async (req, res) => {
  try {
    const { match_status } = req.body;
    const valid = [null, '', 'matched', 'non_matched', 'under_review'];
    if (!valid.includes(match_status)) return res.status(400).json({ error: 'قيمة غير صالحة' });
    const db = await getPool();
    const [[order]] = await db.execute('SELECT id,user_id FROM order_requests WHERE id=?', [req.params.id]);
    if (!order) return res.status(404).json({ error: 'غير موجود' });
    const canEdit = req.user.role === 'admin'
      || (req.user.permissions || []).includes('edit_match_status');
    if (!canEdit) return res.status(403).json({ error: 'ليس لديك صلاحية تعديل حالة المطابقة' });
    await db.execute('UPDATE order_requests SET match_status=? WHERE id=?', [match_status || null, req.params.id]);
    const [[row]] = await db.execute(
      'SELECT o.*, u.username FROM order_requests o LEFT JOIN users u ON o.user_id=u.id WHERE o.id=?',
      [req.params.id]
    );
    res.json(row);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/orders/:id', auth, async (req, res) => {
  try {
    const db = await getPool();
    const [[row]] = await db.execute('SELECT * FROM order_requests WHERE id=?', [req.params.id]);
    if (!row) return res.status(404).json({ error: 'غير موجود' });
    const canEditAny = req.user.role === 'admin' || (req.user.permissions||[]).includes('edit_all_orders');
    if (!canEditAny && row.user_id !== req.user.id)
      return res.status(403).json({ error: 'غير مصرح' });
    const { client_name, phone, area, responsible_name, feed_type, qty, price, requested_date, notes, items, invoice_number } = req.body;
    let resolvedFeedType = feed_type !== undefined ? (feed_type || null) : row.feed_type;
    let resolvedQty = qty || row.qty;
    let resolvedPrice = price !== undefined ? (price || null) : row.price;
    let resolvedItems = row.order_items;
    if (Array.isArray(items) && items.length > 0) {
      resolvedItems = JSON.stringify(items);
      resolvedQty = items.reduce((s, i) => s + (parseFloat(i.qty) || 0), 0);
      resolvedFeedType = items.length === 1 ? (items[0].feed_type || null) : items.map(i => i.feed_type).filter(Boolean).join('، ');
      resolvedPrice = items.length === 1 ? (items[0].price || null) : null;
    }
    const pick = (k) => req.body[k] !== undefined ? (req.body[k] || null) : row[k];
    // resolve purchase items
    let uPItems = req.body.purchase_items;
    let uPItemsJson = row.purchase_items, uPFeed = pick('purchase_feed_type'), uPQty = pick('purchase_qty'), uPPrice = pick('purchase_price');
    if (Array.isArray(uPItems) && uPItems.length > 0) {
      uPItemsJson = JSON.stringify(uPItems);
      uPQty = uPItems.reduce((s,i)=>s+(parseFloat(i.qty)||0),0) || null;
      uPFeed = uPItems.length === 1 ? (uPItems[0].feed_type||null) : uPItems.map(i=>i.feed_type).filter(Boolean).join('، ');
      uPPrice = uPItems.length === 1 ? (uPItems[0].price||null) : null;
    } else if (uPItems !== undefined && uPItems === null) {
      uPItemsJson = null;
    }
    await db.execute(
      'UPDATE order_requests SET client_name=?,phone=?,area=?,responsible_name=?,feed_type=?,qty=?,price=?,requested_date=?,notes=?,order_items=?,invoice_number=?,supplier=?,purchase_date=?,purchase_qty=?,purchase_invoice=?,purchase_price=?,purchase_feed_type=?,purchase_items=?,supplier_payment_status=?,delivery_status=?,sale_date=?,client_payment_status=? WHERE id=?',
      [client_name||row.client_name, phone||null, area||null, responsible_name||null, resolvedFeedType, resolvedQty, resolvedPrice, requested_date||null, notes||null, resolvedItems, invoice_number!==undefined?(invoice_number||null):row.invoice_number, pick('supplier'), pick('purchase_date'), uPQty, pick('purchase_invoice'), uPPrice, uPFeed, uPItemsJson, pick('supplier_payment_status'), pick('delivery_status'), pick('sale_date'), pick('client_payment_status'), req.params.id]
    );
    const [[updated]] = await db.execute(
      'SELECT o.*, u.username FROM order_requests o LEFT JOIN users u ON o.user_id=u.id WHERE o.id=?',
      [req.params.id]
    );
    const fieldLabels = {client_name:'اسم العميل',phone:'الهاتف',area:'المنطقة',responsible_name:'المسؤول',feed_type:'نوع العلف',qty:'الكمية',price:'السعر',requested_date:'وقت التسليم',notes:'ملاحظات'};
    const changes = {};
    const newVals = {client_name,phone,area,responsible_name,feed_type:resolvedFeedType,qty:resolvedQty,price:resolvedPrice,requested_date,notes};
    for(const k of Object.keys(newVals)){
      const oldVal = String(row[k]||''); const newVal = String(newVals[k]||'');
      if(oldVal !== newVal) changes[fieldLabels[k]||k] = {من: oldVal||'-', إلى: newVal||'-'};
    }
    if(Object.keys(changes).length){
      await db.execute(
        'INSERT INTO order_history (order_id,user_id,action,changes) VALUES (?,?,?,?)',
        [req.params.id, req.user.id, 'updated', JSON.stringify(changes)]
      );
    }
    res.json(updated);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/orders/:id', auth, async (req, res) => {
  try {
    const db = await getPool();
    const [[row]] = await db.execute('SELECT * FROM order_requests WHERE id=?', [req.params.id]);
    if (!row) return res.status(404).json({ error: 'غير موجود' });
    if (req.user.role !== 'admin' && row.user_id !== req.user.id)
      return res.status(403).json({ error: 'غير مصرح' });
    await db.execute('DELETE FROM order_requests WHERE id=?', [req.params.id]);
    res.json({ success: true });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── ORDER CHAT ROUTES ───────────────────────────────────────────────────────
app.get('/orders/chat-counts', auth, async (req, res) => {
  try {
    const db = await getPool();
    const canSeeAll = req.user.role === 'admin' || req.user.role === 'requester'
      || (req.user.permissions || []).some(p => ['order_requests','manage_orders'].includes(p));
    let orderIds;
    if (canSeeAll) {
      const [rows] = await db.execute('SELECT id FROM order_requests');
      orderIds = rows.map(r => r.id);
    } else {
      const [rows] = await db.execute('SELECT id FROM order_requests WHERE user_id=?', [req.user.id]);
      orderIds = rows.map(r => r.id);
    }
    if (!orderIds.length) return res.json({});
    const placeholders = orderIds.map(() => '?').join(',');
    const [rows] = await db.execute(
      `SELECT order_id, COUNT(*) AS cnt FROM order_chats WHERE order_id IN (${placeholders}) GROUP BY order_id`,
      orderIds
    );
    const result = {};
    rows.forEach(r => { result[r.order_id] = Number(r.cnt); });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/orders/:id/chat', auth, async (req, res) => {
  try {
    const db = await getPool();
    const [[order]] = await db.execute('SELECT id,user_id FROM order_requests WHERE id=?', [req.params.id]);
    if (!order) return res.status(404).json({ error: 'الطلب غير موجود' });
    const canSee = req.user.role === 'admin' || req.user.role === 'requester'
      || order.user_id === req.user.id
      || (req.user.permissions || []).some(p => ['order_requests','manage_orders'].includes(p));
    if (!canSee) return res.status(403).json({ error: 'غير مصرح' });
    const [rows] = await db.execute(
      `SELECT c.id, c.user_id, c.message, c.created_at, u.username, u.full_name
       FROM order_chats c
       LEFT JOIN users u ON c.user_id = u.id
       WHERE c.order_id = ?
       ORDER BY c.created_at ASC`,
      [req.params.id]
    );
    res.json(rows);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/orders/:id/chat', auth, async (req, res) => {
  try {
    const { message } = req.body;
    if (!message || !message.trim()) return res.status(400).json({ error: 'الرسالة فارغة' });
    const db = await getPool();
    const [[order]] = await db.execute('SELECT id,user_id FROM order_requests WHERE id=?', [req.params.id]);
    if (!order) return res.status(404).json({ error: 'الطلب غير موجود' });
    const canSee = req.user.role === 'admin' || req.user.role === 'requester'
      || order.user_id === req.user.id
      || (req.user.permissions || []).some(p => ['order_requests','manage_orders'].includes(p));
    if (!canSee) return res.status(403).json({ error: 'غير مصرح' });
    const [r] = await db.execute(
      'INSERT INTO order_chats (order_id, user_id, message) VALUES (?,?,?)',
      [req.params.id, req.user.id, message.trim()]
    );
    const [[row]] = await db.execute(
      `SELECT c.id, c.message, c.created_at, u.username, u.full_name
       FROM order_chats c LEFT JOIN users u ON c.user_id = u.id
       WHERE c.id = ?`,
      [r.insertId]
    );
    res.status(201).json(row);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── START ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
initDB().then(() => app.listen(PORT, () => console.log(`سنابل الجوف — المنفذ ${PORT}`))).catch(err => { console.error(err); process.exit(1); });
