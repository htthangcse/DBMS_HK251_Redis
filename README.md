# Redis vs MySQL

## Overview
Dự án này trình bày và so sánh hiệu suất và hành vi của Redis và MySQL trên một số hoạt động cơ sở dữ liệu bao gồm full-text search, key lookup speed, transactions, error handling, and concurrency control.

## Features
- Import data from `Radiologists Report.xlsx` into both Redis and MySQL.
- Compare performance across multiple operations:
  - Query processing
  - Key-value lookup
  - Atomic counters
  - Transaction behavior
  - Error handling
  - Concurrency control
  - Real-world concurrency simulations

## Requirements
- Node.js >= 16
- MySQL 8+
- Redis 6 or 7
- npm

## Setup

### 1. Install dependencies
```bash
npm install
```

### 2. MySQL Setup
Create the database:
```sql
CREATE DATABASE dbms;
```

Configure MySQL credentials in `index.js`:
```js
host: "127.0.0.1",
user: "root",
password: "YOUR_PASSWORD",
database: "dbms",
port: 3306
```

### 3. Redis Setup
Start Redis server:
```bash
redis-server
```

### 4. Place Excel File
Ensure that `Radiologists Report.xlsx` is in the project root.

### 5. Run the project
```bash
node index.js
```

## Menu Options
Upon running, the CLI menu will appear:

```
1. Query Processing
2. Key Lookup Speed
3. Atomic Counter
4. Transaction Success
5. Transaction Error Handling
6. Concurrency Control
7. MySQL Real Concurrency
8. Redis Real Concurrency
0. Exit
```

## Project Structure
```
/project
|── index.js
|── package.json
|── Radiologists Report.xlsx
|── node_modules/
```
