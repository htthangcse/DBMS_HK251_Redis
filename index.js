const XLSX = require("xlsx");
const mysql = require("mysql2/promise");
const redis = require("redis");

// =========================
// 1. READ EXCEL FILE
// =========================
const workbook = XLSX.readFile("./Radiologists Report.xlsx");
const sheet = workbook.Sheets[workbook.SheetNames[0]];
const rows = XLSX.utils.sheet_to_json(sheet);

console.log("Loaded Excel rows:", rows.length);
console.log("Columns in Excel:", Object.keys(rows[0]));

// =========================
// 2. MYSQL CONNECTION
// =========================
async function initMySQL() {
  const db = await mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "090604",
    database: "dbms"
  });

  // TABLE 1: Patients
  await db.execute(`
        CREATE TABLE IF NOT EXISTS patients (
            patient_id INT PRIMARY KEY,
            clinician_notes TEXT,
            FULLTEXT(clinician_notes)
        );
    `);

  // TABLE 2: Counter (for speed demo)
  await db.execute(`
        CREATE TABLE IF NOT EXISTS counter (
            id INT PRIMARY KEY,
            num INT
        );
    `);

  // Insert initial counter row
  await db.execute(`
        INSERT IGNORE INTO counter(id, num)
        VALUES (1, 0);
    `);

  console.log("MySQL ready!");
  return db;
}


// =========================
// 3. REDIS CONNECTION
// =========================
async function initRedis() {
  const client = redis.createClient();
  await client.connect();
  console.log("Redis ready!");
  return client;
}

// =========================
// 4. INSERT DATA TO BOTH DB
// =========================
async function insertData(db, redisClient) {
  for (let r of rows) {
    const id = r["Patient ID"] || null;
    let notes = r["Clinician's Notes"] || null;

    // Nếu row không có ID → bỏ qua để tránh lỗi
    if (!id) continue;

    // Nếu notes thiếu → gán chuỗi rỗng
    if (!notes) notes = "";


    // Insert MySQL
    await db.execute(
      "REPLACE INTO patients(patient_id, clinician_notes) VALUES (?, ?)",
      [id, notes]
    );

    // Insert Redis
    await redisClient.hSet(`patient:${id}`, "id", id);
    await redisClient.hSet(`patient:${id}`, "notes", notes);
  }

  console.log("Inserted data into MySQL & Redis");
}

// =========================
// 5. QUERY PROCESSING DEMO
// =========================
async function demoQueryProcessing(db, redisClient) {
  console.log("\n===== QUERY PROCESSING SPEED DEMO =====");

  const keyword = "stenosis";

  // -------------------------------
  // MySQL Query Speed
  // -------------------------------
  console.time("MySQL Query Time");
  const [mysqlResult] = await db.query(
    `SELECT patient_id FROM patients
    WHERE MATCH(clinician_notes) AGAINST(? IN NATURAL LANGUAGE MODE)
    LIMIT 20;`,
    [keyword]
  );
  console.timeEnd("MySQL Query Time");

  console.log("MySQL results:", mysqlResult.map(r => r.patient_id));


  // -------------------------------
  // Redis Query Speed (Scan + Filter)
  // -------------------------------
  console.time("Redis Query Time");
  const redisKeys = await redisClient.keys("patient:*");

  const matches = [];
  for (let key of redisKeys) {
    const notes = await redisClient.hGet(key, "notes");
    if (notes && notes.toLowerCase().includes(keyword)) {
      matches.push(key);
    }
  }
  console.timeEnd("Redis Query Time");

  console.log("Redis results:", matches.slice(0, 20));
}

async function demoFastKeyLookup(db, redisClient) {
  console.log("\n===== FAST KEY LOOKUP DEMO =====");

  const testId = 172;

  // --- MySQL ---
  console.time("MySQL GET by ID");
  const [mysqlResult] = await db.query("SELECT * FROM patients WHERE patient_id=?", [testId]);
  console.timeEnd("MySQL GET by ID");

  // --- Redis ---
  console.time("Redis HGET by ID");
  const redisResult = await redisClient.hGetAll(`patient:${testId}`);
  console.timeEnd("Redis HGET by ID");

  console.log("MySQL row:", mysqlResult[0]);
  console.log("Redis row:", redisResult);
}

async function demoCounter(db, redisClient) {
  console.log("\n===== COUNTER DEMO =====");

  console.time("MySQL counter");
  await db.query("UPDATE counter SET num = num + 1 WHERE id=1");
  console.timeEnd("MySQL counter");

  console.time("Redis counter");
  await redisClient.incr("counter");
  console.timeEnd("Redis counter");
}



// =========================
// 6. TRANSACTION DEMO
// =========================
async function demoTransaction(db, redisClient) {
  console.log("\n===== TRANSACTION DEMO =====");

  // === MySQL ACID Transaction ===
  try {
    await db.beginTransaction();
    await db.execute(`UPDATE patients SET clinician_notes=? WHERE patient_id=1`, [
      "Updated via MySQL transaction"
    ]);
    await db.commit();
    console.log("MySQL Transaction committed (ACID)");
  } catch (err) {
    await db.rollback();
    console.log("MySQL Transaction rolled back");
  }

  // === Redis Multi/Exec (no rollback) ===
  const transaction = redisClient.multi();
  transaction.hSet("patient:1", "notes", "Updated via Redis MULTI/EXEC");

  try {
    await transaction.exec();
    console.log("Redis MULTI/EXEC committed (NO rollback support)");
  } catch (err) {
    console.log("Redis transaction failed");
  }
}

// =========================
// 7. CONCURRENCY CONTROL DEMO
// =========================
async function demoConcurrency(db, redisClient) {
  console.log("\n===== CONCURRENCY CONTROL DEMO =====");

  // === MYSQL Row Locking ===
  const [locked] = await db.execute(`
        SELECT * FROM patients WHERE id=1 FOR UPDATE;
    `);
  console.log("MySQL locked row:", locked[0]);

  // === REDIS WATCH (Optimistic Locking) ===
  await redisClient.watch("patient:1");

  const notes = await redisClient.hGet("patient:1", "notes");
  const updated = notes + " (edited)";

  const tx = redisClient.multi();
  tx.hSet("patient:1", "notes", updated);

  const result = await tx.exec();
  if (result === null) {
    console.log("Redis WATCH failed (someone modified the key)");
  } else {
    console.log("Redis WATCH success");
  }
}

// =========================
// MAIN EXECUTION
// =========================
async function main() {
  const db = await initMySQL();
  const redisClient = await initRedis();

  await insertData(db, redisClient);
  await demoQueryProcessing(db, redisClient);
  // await demoTransaction(db, redisClient);
  // await demoConcurrency(db, redisClient);

  await demoFastKeyLookup(db, redisClient);
  await demoCounter(db, redisClient);

  process.exit(0);
}

main();