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
    fileSize: 100 * 1024 * 1024 // 100MB
  },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Only CSV files are allowed'), false);
    }
  }
});

const mongoUrl = 'mongodb+srv://pandeyrahul1564:jyW1r4T9Y84kGWoJ@torchmap.4js5lq6.mongodb.net/?retryWrites=true&w=majority&ssl=true';
const dbName = 'csv_manager';

async function init() {
  try {
    const client = await MongoClient.connect(mongoUrl, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      tls: true,
      retryWrites: true,
      w: 'majority'
    });
    
    const db = client.db(dbName);
    const csvFiles = db.collection('csv_files');
    const csvRows = db.collection('csv_rows');
    
    app.locals.db = db;
    app.locals.csvFiles = csvFiles;
    app.locals.csvRows = csvRows;
    app.locals.dbConnected = true;

    console.log('✅ Connected to MongoDB');
    registerRoutes();

    const PORT = process.env.PORT || 10000;
    app.listen(PORT, () => console.log(`🚀 Server running on port ${PORT}`));
  } catch (err) {
    console.error('❌ Failed to connect to MongoDB', err);
    process.exit(1);
  }
}

function registerRoutes() {
  // Health check
  app.get('/api/health', (req, res) => {
    res.json({
      status: 'OK',
      dbConnected: !!app.locals.dbConnected,
      uptime: process.uptime()
    });
  });

  // Upload CSV
  app.post('/api/upload-csv', upload.single('CSV'), async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ detail: 'No file uploaded' });
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
            await app.locals.csvFiles.insertOne(csvDoc);

            const rowDocs = rows.map(r => ({
              id: randomUUID(),
              csv_id: csvId,
              row_data: r,
              is_reviewed: false,
              review_timestamp: null,
            }));

            if (rowDocs.length) {
              await app.locals.csvRows.insertMany(rowDocs);
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

  // Get CSV files list
  app.get('/api/csv-files', async (req, res) => {
    if (!app.locals.dbConnected) {
      return res.status(503).json({ error: 'Database not connected' });
    }
    try {
      const files = await app.locals.csvFiles.find().toArray();
      res.json(files.map(f => ({ ...f, _id: f._id.toString() })));
    } catch (error) {
      console.error('Error fetching CSV files:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // ... (keep all other routes with similar db connection checks)

  // Error handling middleware
  app.use((err, req, res, next) => {
    console.error('Unhandled error:', err);
    
    if (err instanceof multer.MulterError) {
      return res.status(400).json({ 
        error: 'File upload error',
        details: err.message 
      });
    }
    
    res.status(500).json({ 
      error: 'Internal server error',
      details: err.message 
    });
  });
}

init();