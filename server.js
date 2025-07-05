require('dotenv').config();
const express = require('express');
const multer = require('multer');
const csvParser = require('csv-parser');
const { MongoClient } = require('mongodb');
const { Readable } = require('stream');
const cors = require('cors');
const { randomUUID } = require('crypto');

const app = express();
app.use(cors({ origin: '*' }));

app.use(express.json());

const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 100 * 1024 * 1024 // 100MB limit
  }
});

const mongoUrl = 'mongodb+srv://pandeyrahul1564:jyW1r4T9Y84kGWoJ@torchmap.4js5lq6.mongodb.net/?retryWrites=true&w=majority&ssl=true';
const dbName = 'csv_manager';
let db, csvFiles, csvRows;

// Connect to MongoDB
MongoClient.connect(mongoUrl)
  .then(client => {
    db = client.db(dbName);
    csvFiles = db.collection('csv_files');
    csvRows = db.collection('csv_rows');
    console.log('Connected to MongoDB');
  })
  .catch(err => {
    console.error('Failed to connect to MongoDB', err);
    process.exit(1);
  });

// ----------------------------------------
// 📤 Upload CSV
app.post('/api/upload-csv', upload.single('file'), async (req, res) => {
  try {
    if (!req.file || !req.file.originalname.endsWith('.csv')) {
      return res.status(400).json({ detail: 'File must be a CSV' });
    }

    const rows = [];
    const headers = [];

    const stream = Readable.from(req.file.buffer);
    stream
      .pipe(csvParser())
      .on('headers', h => headers.push(...h))
      .on('data', row => rows.push(row))
      .on('end', async () => {
        try {
          const csvId = randomUUID();
          const csvDoc = {
            id: csvId,
            filename: req.file.originalname,
            headers,
            upload_timestamp: new Date(),
          };
          await csvFiles.insertOne(csvDoc);

          const rowDocs = rows.map(r => ({
            id: randomUUID(),
            csv_id: csvId,
            row_data: r,
            is_reviewed: false,
            review_timestamp: null,
          }));
          
          if (rowDocs.length) {
            await csvRows.insertMany(rowDocs);
          }

          res.json({
            message: 'CSV uploaded successfully',
            csv_id: csvId,
            filename: req.file.originalname,
            headers,
            row_count: rowDocs.length,
          });
        } catch (dbError) {
          console.error('Database error:', dbError);
          res.status(500).json({ detail: 'Error saving to database' });
        }
      })
      .on('error', err => {
        console.error('CSV parsing error:', err);
        res.status(500).json({ detail: 'Error parsing CSV' });
      });
  } catch (error) {
    console.error('Upload error:', error);
    res.status(500).json({ detail: 'Internal server error' });
  }
});

// ----------------------------------------
// 📂 List CSV files
app.get('/api/csv-files', async (req, res) => {
  try {
    const files = await csvFiles.find().toArray();
    res.json(files.map(f => ({ ...f, _id: f._id.toString() })));
  } catch (error) {
    console.error('Error fetching CSV files:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ----------------------------------------
// 🧾 Get CSV data with filters
app.get('/api/csv-data/:csv_id', async (req, res) => {
  try {
    const { csv_id } = req.params;
    const { reviewed_only, search_term, column_filters } = req.query;

    const csvFile = await csvFiles.findOne({ id: csv_id });
    if (!csvFile) return res.status(404).json({ detail: 'CSV file not found' });

    const query = { csv_id };
    if (reviewed_only !== undefined) query.is_reviewed = reviewed_only === 'true';

    let rows = await csvRows.find(query).toArray();

    if (search_term) {
      rows = rows.filter(r =>
        Object.values(r.row_data).some(v =>
          v.toString().toLowerCase().includes(search_term.toLowerCase())
        )
      );
    }

    if (column_filters) {
      try {
        const filters = JSON.parse(column_filters);
        rows = rows.filter(r =>
          Object.entries(filters).every(([col, val]) =>
            !val ||
            (r.row_data[col] || '')
              .toString()
              .toLowerCase()
              .includes(val.toLowerCase())
        )
        );
      } catch (e) {
        console.error('Error parsing column filters:', e);
      }
    }

    res.json({ csv_file: csvFile, rows });
  } catch (error) {
    console.error('Error fetching CSV data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ----------------------------------------
// � Get unique column values
app.get('/api/csv-columns/:csv_id', async (req, res) => {
  try {
    const { csv_id } = req.params;
    const csvFile = await csvFiles.findOne({ id: csv_id });
    if (!csvFile) return res.status(404).json({ detail: 'CSV file not found' });

    const rows = await csvRows.find({ csv_id }).toArray();
    const unique = {};

    csvFile.headers.forEach(h => {
      unique[h] = [
        ...new Set(
          rows
            .map(r => r.row_data[h])
            .filter(val => val != null && val !== '')
            .map(String)
        ),
      ].sort();
    });

    res.json(unique);
  } catch (error) {
    console.error('Error fetching column data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ----------------------------------------
// ✔ Update review status
app.post('/api/update-review-status', async (req, res) => {
  try {
    const { row_ids, is_reviewed } = req.body;
    if (!Array.isArray(row_ids) || typeof is_reviewed !== 'boolean') {
      return res.status(400).json({ detail: 'Invalid payload' });
    }

    const result = await csvRows.updateMany(
      { id: { $in: row_ids } },
      {
        $set: {
          is_reviewed,
          review_timestamp: is_reviewed ? new Date() : null,
        },
      }
    );
    res.json({
      message: `Updated ${result.modifiedCount} rows`,
      modified_count: result.modifiedCount,
    });
  } catch (error) {
    console.error('Error updating review status:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ----------------------------------------
// 📥 Export reviewed rows as CSV
app.get('/api/export-reviewed/:csv_id', async (req, res) => {
  try {
    const { csv_id } = req.params;
    const file = await csvFiles.findOne({ id: csv_id });
    if (!file) return res.status(404).json({ detail: 'CSV file not found' });

    const rows = await csvRows.find({ csv_id, is_reviewed: true }).toArray();
    if (!rows.length) {
      return res.status(404).json({ detail: 'No reviewed rows found' });
    }

    res.setHeader(
      'Content-Disposition',
      `attachment; filename=reviewed_${file.filename}`
    );
    res.setHeader('Content-Type', 'text/csv');

    const headerLine = file.headers.join(',') + '\n';
    res.write(headerLine);

    rows.forEach(r => {
      const line = file.headers
        .map(h => (r.row_data[h] || '').toString().replace(/"/g, '""'))
        .map(cell =>
          cell.includes(',') || cell.includes('\n') ? `"${cell}"` : cell
        )
        .join(',') + '\n';
      res.write(line);
    });
    res.end();
  } catch (error) {
    console.error('Error exporting CSV:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ----------------------------------------
// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({
    status: 'OK',
    dbConnected: !!db,
    uptime: process.uptime()
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({ error: 'Internal server error' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));