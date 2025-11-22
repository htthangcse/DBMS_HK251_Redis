const XLSX = require("xlsx");
const mysql = require("mysql2/promise");
const redis = require("redis");
const readline = require("readline");

// =========================
// 1. READ EXCEL FILE
// =========================
const workbook = XLSX.readFile("./Radiologists Report.xlsx");
const sheet = workbook.Sheets[workbook.SheetNames[0]];
const rows = XLSX.utils.sheet_to_json(sheet);

console.log("=".repeat(60));
console.log("DEMO SO SÁNH REDIS vs MySQL");
console.log("=".repeat(60));
console.log(`Loaded Excel rows: ${rows.length}`);
console.log(`Columns: ${Object.keys(rows[0]).join(", ")}\n`);

// =========================
// 2. MYSQL CONNECTION
// =========================
async function initMySQL() {
  const db = await mysql.createConnection({
    host: "127.0.0.1",
    user: "root",
    password: "090604",
    database: "dbms",
    port: 3306
  });

  // TABLE 1: Patients
  await db.execute(`
    CREATE TABLE IF NOT EXISTS patients (
      patient_id INT PRIMARY KEY,
      clinician_notes TEXT,
      FULLTEXT(clinician_notes)
    );
  `);

  // TABLE 2: Counter (for performance demo)
  await db.execute(`
    CREATE TABLE IF NOT EXISTS counter (
      id INT PRIMARY KEY,
      num INT
    );
  `);

  await db.execute(`
    INSERT IGNORE INTO counter(id, num) VALUES (1, 0);
  `);

  console.log("MySQL connected");
  return db;
}

// =========================
// 3. REDIS CONNECTION
// =========================
async function initRedis() {
  const client = redis.createClient();
  await client.connect();
  console.log("Redis connected\n");
  return client;
}

// =========================
// 4. INSERT DATA TO BOTH DB
// =========================
async function insertData(db, redisClient) {
  console.log("Inserting data...");

  for (let r of rows) {
    const id = r["Patient ID"] || null;
    let notes = r["Clinician's Notes"] || null;

    if (!id) continue;
    if (!notes) notes = "";

    // Insert MySQL
    await db.execute(
      "REPLACE INTO patients(patient_id, clinician_notes) VALUES (?, ?)",
      [id, notes]
    );

    // Insert Redis
    await redisClient.hSet(`patient:${id}`, "id", String(id));
    await redisClient.hSet(`patient:${id}`, "notes", notes);
  }

  console.log(`Inserted ${rows.length} records into MySQL & Redis\n`);
}

// =========================
// 5. QUERY PROCESSING DEMO
// =========================
// demo search keyword
async function demoQueryProcessing(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 1: QUERY KEYWORD PROCESSING");
  console.log("=".repeat(60));

  const keyword = "stenosis";
  console.log(`Searching for keyword: "${keyword}"\n`);

  // --- MySQL FULLTEXT Search ---
  const mysqlStart = performance.now();
  const [mysqlResult] = await db.query(
    `SELECT patient_id FROM patients 
     WHERE MATCH(clinician_notes) AGAINST(? IN NATURAL LANGUAGE MODE);`,
    [keyword]
  );
  const mysqlTime = (performance.now() - mysqlStart).toFixed(3);

  console.log(`MySQL Query Time: ${mysqlTime}ms`);
  console.log(`MySQL found: ${mysqlResult.length} records`);
  console.log(`Sample IDs: ${mysqlResult.map(r => r.patient_id).join(", ")}\n`);

  // --- Redis Scan + Filter ---
  const redisStart = performance.now();
  const redisKeys = await redisClient.keys("patient:*");

  const matches = [];
  for (let key of redisKeys) {
    const notes = await redisClient.hGet(key, "notes");
    if (notes && notes.toLowerCase().includes(keyword)) {
      matches.push(key);
    }
  }
  const redisTime = (performance.now() - redisStart).toFixed(3);

  console.log(`Redis Query Time: ${redisTime}ms`);
  console.log(`Redis found: ${matches.length} records`);
  console.log(`Sample keys: ${matches.join(", ")}\n`);

  // --- MySQL FULLTEXT Search Redis Scan + Filter ---
  const speedup = (redisTime / mysqlTime).toFixed(2);
  if (mysqlTime < redisTime) {
    console.log(`MySQL is ${speedup}x FASTER for full-text search`);
  } else {
    console.log(`Redis is ${(mysqlTime / redisTime).toFixed(2)}x FASTER\n`);
  }
}

// =========================
// 6. KEY LOOKUP DEMO
// =========================
async function demoFastKeyLookup(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 2: KEY-VALUE LOOKUP SPEED");
  console.log("=".repeat(60));

  const testId = 250;
  console.log(`Looking up Patient ID: ${testId}\n`);

  // --- MySQL ---
  const mysqlStart = performance.now();
  const [mysqlResult] = await db.query(
    "SELECT * FROM patients WHERE patient_id=?",
    [testId]
  );
  const mysqlTime = (performance.now() - mysqlStart).toFixed(3);

  console.log(`MySQL GET by ID: ${mysqlTime}ms`);
  console.log(`Found: ${mysqlResult.length} record\n`);

  // --- Redis ---
  const redisStart = performance.now();
  const redisResult = await redisClient.hGetAll(`patient:${testId}`);
  const redisTime = (performance.now() - redisStart).toFixed(3);

  console.log(`Redis HGET by ID: ${redisTime}ms`);
  console.log(`Found: ${redisResult.length} record\n`);

  // --- Redis stores data in RAM (O(1) access) MySQL must read from disk + B-tree traversal ---
  const speedup = (mysqlTime / redisTime).toFixed(2);
  console.log(`Redis is ${speedup}x FASTER for key lookup!`);
}

// =========================
// 7. COUNTER DEMO
// =========================
async function demoCounter(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 3: ATOMIC COUNTER OPERATIONS");
  console.log("=".repeat(60));
  console.log(`Incrementing counter 100 times...\n`);

  // --- MySQL ---
  const mysqlStart = performance.now();
  for (let i = 0; i < 100; i++) {
    await db.query("UPDATE counter SET num = num + 1 WHERE id=1");
  }
  const mysqlTime = (performance.now() - mysqlStart).toFixed(3);

  console.log(`MySQL (100 UPDATEs): ${mysqlTime}ms`);
  console.log(`Average per operation: ${(mysqlTime / 100).toFixed(3)}ms\n`);

  // --- Redis ---
  const redisStart = performance.now();
  for (let i = 0; i < 100; i++) {
    await redisClient.incr("counter");
  }
  const redisTime = (performance.now() - redisStart).toFixed(3);

  console.log(`Redis (100 INCRs): ${redisTime}ms`);
  console.log(`Average per operation: ${(redisTime / 100).toFixed(3)}ms\n`);

  // --- Counter      Redis: view counts, like buttons, rate limiting ---
  const speedup = (mysqlTime / redisTime).toFixed(2);
  console.log(`Redis is ${speedup}x FASTER for counters!`);
}

// =========================
// 8. TRANSACTION DEMO
// =========================
async function demoTransactionSuccess(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 4: TRANSACTION SUPPORT");
  console.log("=".repeat(60));

  // --- MySQL ACID Transaction ---
  console.log("MySQL Transaction (ACID compliant):\n");

  try {
    await db.beginTransaction();
    console.log("BEGIN TRANSACTION");
    const [beforeMySQL] = await db.execute(
      "SELECT clinician_notes FROM patients WHERE patient_id=1"
    );
    console.log("Row before MySQL UPDATE:", beforeMySQL[0].clinician_notes);

    await db.execute(
      `UPDATE patients SET clinician_notes=? WHERE patient_id=1`,
      ["Updated via MySQL transaction"]
    );
    console.log("UPDATE executed");

    const [afterMySQL] = await db.execute(
      "SELECT clinician_notes FROM patients WHERE patient_id=1"
    );
    console.log("Row after MySQL UPDATE:", afterMySQL[0].clinician_notes);

    await db.commit();
    console.log("COMMIT successful\n");
  } catch (err) {
    await db.rollback(); // ho tro rollback
    console.log("ROLLBACK executed");
  }

  // --- Redis MULTI/EXEC ---
  console.log("Redis Transaction (MULTI/EXEC):\n");

  const transaction = redisClient.multi();
  const beforeRedis = await redisClient.hGet("patient:1", "notes");
  console.log("Row after Redis UPDATE:", beforeRedis);

  transaction.hSet("patient:1", "notes", "Updated via Redis MULTI/EXEC");
  console.log("HSET queued");

  try {
    await transaction.exec();

    const afterRedis = await redisClient.hGet("patient:1", "notes");
    console.log("Row after Redis UPDATE:", afterRedis);
    console.log("EXEC successful");
  } catch (err) {
    console.log("EXEC failed");
  }
}


// =========================
// 5. TRANSACTION ERROR HANDLING DEMO
// =========================
async function demoTransactionError(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 5: TRANSACTION ERROR HANDLING");
  console.log("=".repeat(60));
  console.log("Scenario: Update patient_id=1, then intentionally cause error\n");

  // MYSQL - ROLLBACK ON ERROR
  console.log("MySQL Transaction with Error:\n");

  // Đọc giá trị ban đầu
  console.log("Step 1: Update value note")
  const [beforeMySQL] = await db.query(
    "SELECT clinician_notes FROM patients WHERE patient_id=1"
  );
  console.log(`Initial value: "${beforeMySQL[0]?.clinician_notes || 'N/A'}..."`);

  try {
    await db.beginTransaction();
    console.log("BEGIN TRANSACTION");

    // Step 1: Update thành công
    await db.execute(
      `UPDATE patients SET clinician_notes=? WHERE patient_id=1`,
      ["MYSQL UPDATE - This should be rolled back"]
    );
    const [afterMySQLSuccess] = await db.query(
      "SELECT clinician_notes FROM patients WHERE patient_id=1"
    );
    console.log(`afterMySQLSuccess: "${afterMySQLSuccess[0]?.clinician_notes || 'N/A'}"`);

    // Step 2: gây lỗi - INSERT duplicate PRIMARY KEY
    console.log("Step 2: Attempting to insert duplicate PRIMARY KEY...");
    await db.execute(
      `INSERT INTO patients (patient_id, clinician_notes) VALUES (1, 'This will fail')`
    );
    // Không bao giờ đến dòng này
    console.log("Step 2: INSERT successful");

    await db.commit();
    console.log("COMMIT successful");
  } catch (err) {
    console.log(`Error occurred: ${err.message}`);
    console.log("Executing ROLLBACK...");
    await db.rollback();
    console.log("ROLLBACK successful - all changes reverted!\n");
  }

  // check sau rollback
  const [afterMySQL] = await db.query(
    "SELECT clinician_notes FROM patients WHERE patient_id=1"
  );
  console.log(`Final value: "${afterMySQL[0]?.clinician_notes || 'N/A'}" \n`);

  // REDIS - NO ROLLBACK (Commands still execute)
  console.log("Redis Transaction with Error:\n");
  // Đọc giá trị ban đầu
  const beforeRedis = await redisClient.hGet("patient:1", "notes");
  console.log(`Initial value: "${beforeRedis || 'N/A'}"`);

  const transaction = redisClient.multi();
  console.log("MULTI started");

  // Step 1: Update thành công
  transaction.hSet("patient:1", "notes", "REDIS UPDATE - This WILL stay");
  console.log("Step 1: HSET queued");

  // Step 2: gây lỗi - sử dụng command sai kiểu dữ liệu
  console.log("Step 2: Attempting INVALID command...");
  transaction.incr("patient:1"); // Error: không thể INCR trên hash
  console.log("Step 2: INCR queued (will fail on execution)");

  try {
    const results = await transaction.exec();
    console.log("EXEC completed (but with errors)");

    // Kiểm tra kết quả từng command
    console.log("\nCommand Results:");
    results.forEach((result, index) => {
      if (result instanceof Error) {
        console.log(`Step ${index + 1}: Error - ${result.message}`);
      } else {
        console.log(`Step ${index + 1}: Success`);
      }
    });
    console.log();
  } catch (err) {
    console.log(`EXEC failed: ${err.message}\n`);
  }

  // Kiểm tra giá trị sau "rollback"
  const afterRedis = await redisClient.hGet("patient:1", "notes");
  console.log(`Final value: "${afterRedis || 'N/A'}"\n`);
}

// =========================
// 9. CONCURRENCY CONTROL DEMO
// =========================
async function demoConcurrency(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 6: CONCURRENCY CONTROL");
  console.log("=".repeat(60));

  // --- MySQL Row Locking (Pessimistic) ---
  console.log("MySQL Concurrency Control (Row Locking):\n");

  try {
    await db.beginTransaction();
    console.log("BEGIN TRANSACTION");

    const [locked] = await db.execute(`
      SELECT * FROM patients WHERE patient_id=1 FOR UPDATE;
    `);
    console.log("Row LOCKED with FOR UPDATE");
    console.log(`Locked row ID: ${locked[0]?.patient_id || 'N/A'}`);
    console.log("Other transactions must WAIT");

    await db.commit();
    console.log("COMMIT - lock released");
  } catch (err) {
    await db.rollback();
  }

  // --- Redis Optimistic Locking (WATCH) ---
  console.log("Redis Concurrency Control (Optimistic Locking):\n");

  await redisClient.watch("patient:1");
  console.log("WATCH patient:1");
  console.log("Monitoring for changes");

  const notes = await redisClient.hGet("patient:1", "notes");
  const updated = notes + " (edited)";

  const tx = redisClient.multi();
  tx.hSet("patient:1", "notes", updated);
  console.log("HSET queued");

  const result = await tx.exec();
  if (result === null) {
    console.log("EXEC FAILED - key was modified by another client");
    console.log("Transaction must retry");
  } else {
    console.log("EXEC successful");
    console.log("Uses Optimistic Locking");
    console.log("No deadlocks (single-threaded)\n");
  }
}

// =========================
// 9A. REAL MYSQL CONCURRENCY DEMO
// =========================

async function demoRealConcurrencyMySQL(db) {
  console.log("=".repeat(60));
  console.log("DEMO 7: REAL MYSQL CONCURRENCY");
  console.log("=".repeat(60));

  console.log("Simulating two clients A & B...\n");

  // --- CLIENT A ---
  const clientA = await mysql.createConnection({
    host: "127.0.0.1",
    user: "root",
    password: "090604",
    database: "dbms"
  });

  // --- CLIENT B ---
  const clientB = await mysql.createConnection({
    host: "127.0.0.1",
    user: "root",
    password: "090604",
    database: "dbms"
  });

  // CLIENT A: BEGIN + LOCK
  console.log("CLIENT A: BEGIN + SELECT ... FOR UPDATE");
  await clientA.beginTransaction();

  const [rowA] = await clientA.execute(
    "SELECT clinician_notes FROM patients WHERE patient_id=1 FOR UPDATE"
  );
  console.log("CLIENT A locked row:", rowA[0].clinician_notes);

  // CLIENT B: TRY TO UPDATE (WILL BLOCK)
  console.log("\nCLIENT B: trying UPDATE (should block until A commits)");

  const start = Date.now();

  const B_promise = clientB
    .execute(
      "UPDATE patients SET clinician_notes=? WHERE patient_id=1",
      ["UPDATE BY CLIENT B"]
    )
    .then(async () => {
      const end = Date.now();

      console.log(
        `CLIENT B update DONE — waited ${(end - start) / 1000} seconds for lock`
      );

      const [afterB] = await clientB.execute(
        "SELECT clinician_notes FROM patients WHERE patient_id=1"
      );

      console.log("CLIENT B sees updated row:", afterB[0].clinician_notes);
    });


  console.log("\nCLIENT A: now COMMIT (releasing lock)");
  await clientA.commit();

  await B_promise;

  const [afterA] = await clientA.execute(
    "SELECT clinician_notes FROM patients WHERE patient_id=1"
  );
  console.log("CLIENT A sees row AFTER B update:", afterA[0].clinician_notes);

  console.log("\nMySQL Concurrency Successful: B waited until A committed!\n");

  await clientA.end();
  await clientB.end();
}



// =========================
// 9B. REAL REDIS OPTIMISTIC LOCKING DEMO
// =========================
async function demoRealConcurrencyRedis(redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 8: REAL REDIS CONCURRENCY (WATCH)");
  console.log("=".repeat(60));

  console.log("Simulating clients A (transaction) and B (modifier)...\n");

  // CLIENT A: WATCH + MULTI
  console.log("CLIENT A: WATCH patient:1");
  await redisClient.watch("patient:1");

  const oldValue = await redisClient.hGet("patient:1", "notes");
  console.log("CLIENT A reads:", oldValue);

  // CLIENT B CHANGES VALUE BEFORE A EXEC
  console.log("\nCLIENT B: modifying value BEFORE CLIENT A EXEC");
  await redisClient.hSet("patient:1", "notes", oldValue + " [MODIFIED BY B]");
  console.log("Final value:", await redisClient.hGet("patient:1", "notes"));

  // CLIENT A TRY EXEC
  console.log("\nCLIENT A: preparing MULTI/EXEC");
  const tx = redisClient.multi();
  tx.hSet("patient:1", "notes", oldValue + " [UPDATE BY A]");

  const result = await tx.exec();

  if (result === null) {
    console.log("EXEC FAILED → Key modified by another client!");
    console.log("CLIENT A must retry transaction\n");
  } else {
    console.log("EXEC SUCCESS");
  }

  console.log("Final value:", await redisClient.hGet("patient:1", "notes"));
}


// =========================
// MAIN EXECUTION
// =========================
async function main() {
  try {
    const db = await initMySQL();
    const redisClient = await initRedis();
    await insertData(db, redisClient);

    while (true) {
      const choice = await showMenu();

      if (choice === "1") await demoQueryProcessing(db, redisClient);
      else if (choice === "2") await demoFastKeyLookup(db, redisClient);
      else if (choice === "3") await demoCounter(db, redisClient);
      else if (choice === "4") await demoTransactionSuccess(db, redisClient);
      else if (choice === "5") await demoTransactionError(db, redisClient);
      else if (choice === "6") await demoConcurrency(db, redisClient);
      else if (choice === "7") await demoRealConcurrencyMySQL(db);
      else if (choice === "8") await demoRealConcurrencyRedis(redisClient);
      else if (choice === "0") {
        console.log("\nThoát chương trình...");
        break;
      } else {
        console.log("Lựa chọn không hợp lệ, vui lòng nhập lại!");
      }
    }

    await db.end();
    await redisClient.quit();
    process.exit(0);

  } catch (error) {
    console.error("Error:", error.message);
    process.exit(1);
  }
}

function ask(question) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  return new Promise(resolve =>
    rl.question(question, ans => {
      rl.close();
      resolve(ans);
    })
  );
}

async function showMenu() {
  console.log("==============================");
  console.log("CHỌN DEMO MUỐN CHẠY:");
  console.log("1. Query Processing");
  console.log("2. Key Lookup Speed");
  console.log("3. Atomic Counter");
  console.log("4. Transaction Success");
  console.log("5. Transaction Error Handling");
  console.log("6. Concurrency Control");
  console.log("7. MySQL Real Concurrency");
  console.log("8. Redis Real Concurrency");
  console.log("0. Thoát");
  console.log("==============================");

  const choice = await ask("Nhập lựa chọn: ");
  return choice.trim();
}



main();