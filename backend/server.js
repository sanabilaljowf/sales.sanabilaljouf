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

  await db.execute(`CREATE TABLE IF NOT EXISTS clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    activity VARCHAR(255),
    feed_type VARCHAR(100),
    phone VARCHAR(50),
    area VARCHAR(100),
    sale_type ENUM('wholesale','retail') DEFAULT 'wholesale',
    cons_type ENUM('daily','weekly') DEFAULT 'daily',
    cons DECIMAL(10,2) DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  ) CHARACTER SET utf8mb4`);

  // add sale_type if upgrading from old schema
  await db.execute(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS sale_type ENUM('wholesale','retail') DEFAULT 'wholesale'`).catch(()=>{});

  await db.execute(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS created_by INT NULL`).catch(()=>{});
  await db.execute(`CREATE TABLE IF NOT EXISTS purchases (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL,
    date DATE NOT NULL,
    qty DECIMAL(10,2) NOT NULL,
    cons DECIMAL(10,2),
    feed_type VARCHAR(100),
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
  ) CHARACTER SET utf8mb4`);

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
app.get('/users', auth, adminOnly, async (req, res) => {
  const db = await getPool();
  const [rows] = await db.execute('SELECT id,username,role,full_name,job_title,permissions,created_at FROM users ORDER BY created_at');
  res.json(rows);
});
app.post('/users', auth, adminOnly, async (req, res) => {
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
app.put('/users/:id', auth, adminOnly, async (req, res) => {
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
app.delete('/users/:id', auth, adminOnly, async (req, res) => {
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
    const [purchases] = await db.execute('SELECT * FROM purchases ORDER BY date DESC');
    res.json(clients.map(c => ({ ...c, purchases: purchases.filter(p => p.client_id === c.id) })));
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/clients', auth, async (req, res) => {
  try {
    const { name, activity, feed_type, phone, area, sale_type, cons_type, cons, notes, purchase_date, purchase_qty } = req.body;
    const db = await getPool();
    const [r] = await db.execute(
      'INSERT INTO clients (name,activity,feed_type,phone,area,sale_type,cons_type,cons,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?)',
      [name, activity||null, feed_type||null, phone||null, area||null, sale_type||'wholesale', cons_type||'daily', cons||0, notes||null, req.user.id]
    );
    const clientId = r.insertId;
    if (purchase_date && purchase_qty) {
      await db.execute('INSERT INTO purchases (client_id,date,qty,cons,feed_type) VALUES (?,?,?,?,?)',
        [clientId, purchase_date, purchase_qty, cons||0, feed_type||null]);
    }
    // queue new contact info + any new lookup values for admin review
    await queuePendingContact(db, name, phone, activity, area, clientId);
    await queuePending(db, 'region', area, clientId);
    await queuePending(db, 'feed_type', feed_type, clientId);

    const [[client]] = await db.execute('SELECT * FROM clients WHERE id=?', [clientId]);
    const [purchases] = await db.execute('SELECT * FROM purchases WHERE client_id=?', [clientId]);
    res.status(201).json({ ...client, purchases });
  } catch (err) { res.status(500).json({ error: err.message }); }
});
app.post('/clients/:id/purchases', auth, async (req, res) => {
  try {
    const { date, qty, cons, feed_type, note } = req.body;
    const db = await getPool();
    const [r] = await db.execute('INSERT INTO purchases (client_id,date,qty,cons,feed_type,note) VALUES (?,?,?,?,?,?)',
      [req.params.id, date, qty, cons||null, feed_type||null, note||null]);
    const [[purchase]] = await db.execute('SELECT * FROM purchases WHERE id=?', [r.insertId]);
    res.status(201).json(purchase);
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
// GET /analytics/consumption-rates?sale_type=wholesale|retail&days=90
// Returns daily/weekly/monthly consumption and order rates per client + summary
app.get('/analytics/consumption-rates', auth, async (req, res) => {
  try {
    const db = await getPool();
    const { sale_type, days = 90 } = req.query;
    const daysInt = Math.max(1, parseInt(days) || 90);

    const clientWhere = (sale_type === 'wholesale' || sale_type === 'retail')
      ? 'WHERE c.sale_type = ?' : '';
    const clientParams = clientWhere ? [daysInt, sale_type] : [daysInt];

    const [rows] = await db.execute(`
      SELECT
        c.id, c.name, c.sale_type, c.feed_type, c.area,
        COUNT(p.id)            AS order_count,
        COALESCE(SUM(p.qty),0) AS total_qty,
        MIN(p.date)            AS first_purchase,
        MAX(p.date)            AS last_purchase,
        DATEDIFF(CURDATE(), MIN(p.date)) AS days_since_first
      FROM clients c
      LEFT JOIN purchases p
        ON p.client_id = c.id AND p.date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
      ${clientWhere}
      GROUP BY c.id, c.name, c.sale_type, c.feed_type, c.area
      ORDER BY c.sale_type, total_qty DESC
    `, clientParams);

    const clients = rows.map(row => {
      const activeDays  = Math.max(1, Math.min(Number(row.days_since_first) || daysInt, daysInt));
      const totalQty    = parseFloat(row.total_qty)  || 0;
      const orderCount  = parseInt(row.order_count)  || 0;
      const dqty        = totalQty   / activeDays;
      const dord        = orderCount / activeDays;
      return {
        id: row.id, name: row.name, sale_type: row.sale_type,
        feed_type: row.feed_type, area: row.area,
        period_days: daysInt, order_count: orderCount, total_qty: +totalQty.toFixed(2),
        first_purchase: row.first_purchase, last_purchase: row.last_purchase,
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

    // Aggregate summary by sale_type
    const summary = {};
    for (const c of clients) {
      if (!summary[c.sale_type])
        summary[c.sale_type] = { client_count: 0, total_qty: 0, order_count: 0 };
      summary[c.sale_type].client_count++;
      summary[c.sale_type].total_qty   += c.total_qty;
      summary[c.sale_type].order_count += c.order_count;
    }
    for (const type of Object.keys(summary)) {
      const s   = summary[type];
      const dqty = s.total_qty   / daysInt;
      const dord = s.order_count / daysInt;
      s.total_qty = +s.total_qty.toFixed(2);
      s.consumption_rate = {
        daily:   +(dqty      ).toFixed(2),
        weekly:  +(dqty *  7 ).toFixed(2),
        monthly: +(dqty * 30 ).toFixed(2),
      };
      s.order_rate = {
        daily:   +(dord      ).toFixed(3),
        weekly:  +(dord *  7 ).toFixed(2),
        monthly: +(dord * 30 ).toFixed(2),
      };
    }

    res.json({ period_days: daysInt, summary, clients });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// GET /clients/:id/consumption-rate?days=90
// Returns detailed daily/weekly/monthly rates for a single client
app.get('/clients/:id/consumption-rate', auth, async (req, res) => {
  try {
    const db = await getPool();
    const daysInt = Math.max(1, parseInt(req.query.days) || 90);

    const [[client]] = await db.execute('SELECT id,name,sale_type,feed_type,area FROM clients WHERE id=?', [req.params.id]);
    if (!client) return res.status(404).json({ error: 'العميل غير موجود' });

    const [purchases] = await db.execute(
      `SELECT date, qty FROM purchases
       WHERE client_id=? AND date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
       ORDER BY date ASC`,
      [req.params.id, daysInt]
    );

    const totalQty   = purchases.reduce((s, p) => s + parseFloat(p.qty), 0);
    const orderCount = purchases.length;
    const activeDays = purchases.length > 0
      ? Math.max(1, Math.min(daysInt,
          Math.ceil((Date.now() - new Date(purchases[0].date)) / 86400000) || 1))
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
      purchases,
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
    if (!client_name || !resolvedQty) return res.status(400).json({ error: 'اسم العميل والكمية مطلوبان' });
    const db = await getPool();
    const [r] = await db.execute(
      'INSERT INTO order_requests (user_id,client_name,phone,area,employee_name,responsible_name,feed_type,qty,price,requested_date,order_time,notes,order_items) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
      [req.user.id, client_name.trim(), phone||null, area||null, employee_name||null, responsible_name||null, resolvedFeedType, resolvedQty, resolvedPrice, requested_date||null, order_time||null, notes||null, resolvedItems]
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
  if(req.user.role==='admin'||req.user.role==='requester'||(req.user.permissions||[]).some(p=>['manage_orders','order_requests','edit_all_orders'].includes(p))) return next();
  res.status(403).json({error:'ليس لديك صلاحية'});
}, async (req, res) => {
  try {
    const { status, delivery_status, admin_note } = req.body;
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
    await db.execute(
      'UPDATE order_requests SET client_name=?,phone=?,area=?,responsible_name=?,feed_type=?,qty=?,price=?,requested_date=?,notes=?,order_items=?,invoice_number=? WHERE id=?',
      [client_name||row.client_name, phone||null, area||null, responsible_name||null, resolvedFeedType, resolvedQty, resolvedPrice, requested_date||null, notes||null, resolvedItems, invoice_number!==undefined?(invoice_number||null):row.invoice_number, req.params.id]
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
