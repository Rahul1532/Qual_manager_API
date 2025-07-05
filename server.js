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
    
    // Create indexes for better performance
    await csvRows.createIndex({ csv_id: 1 });
    await csvRows.createIndex({ is_reviewed: 1 });
    await csvFiles.createIndex({ id: 1 });
    await csvRows.createIndex({ "row_data.$**": 1 }); // Wildcard index for all fields in row_data
    
    app.locals.db = db;
    app.locals.csvFiles = csvFiles;
    app.locals.csvRows = csvRows;
    app.locals.dbConnected = true;

    console.log('✅ Connected to MongoDB with indexes created');
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
              row_count: rows.length
            };
            
            await app.locals.csvFiles.insertOne(csvDoc);

            const rowDocs = rows.map(r => ({
              id: randomUUID(),
              csv_id: csvId,
              row_data: r,
              is_reviewed: false,
              review_timestamp: null
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
      const files = await app.locals.csvFiles.find({}, {
        projection: {
          _id: 0,
          id: 1,
          filename: 1,
          headers: 1,
          upload_timestamp: 1,
          row_count: 1
        }
      }).toArray();
      res.json(files);
    } catch (error) {
      console.error('Error fetching CSV files:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // Get filtered CSV data
  app.get('/api/csv-data/:csvId', async (req, res) => {
    try {
      const { csvId } = req.params;
      const { reviewed_only, search_term, column_filters } = req.query;
      
      let query = { csv_id: csvId };
      
      // Handle review status filter
      if (reviewed_only !== undefined) {
        query.is_reviewed = reviewed_only === 'true';
      }
      
      // Handle global search
      if (search_term) {
        const csvFile = await app.locals.csvFiles.findOne({ id: csvId });
        if (csvFile && csvFile.headers) {
          query.$or = csvFile.headers.map(header => ({
            [`row_data.${header}`]: { $regex: search_term, $options: 'i' }
          }));
        }
      }
      
      // Handle column-specific filters
      if (column_filters) {
        try {
          const filters = JSON.parse(column_filters);
          Object.entries(filters).forEach(([column, value]) => {
            if (value) {
              query[`row_data.${column}`] = value;
            }
          });
        } catch (e) {
          console.error('Error parsing column filters:', e);
        }
      }
      
      const rows = await app.locals.csvRows.find(query, {
        projection: {
          _id: 0,
          id: 1,
          csv_id: 1,
          row_data: 1,
          is_reviewed: 1,
          review_timestamp: 1
        }
      }).toArray();
      
      const csvFile = await app.locals.csvFiles.findOne({ id: csvId }, {
        projection: {
          _id: 0,
          id: 1,
          filename: 1,
          headers: 1,
          upload_timestamp: 1,
          row_count: 1
        }
      });
      
      res.json({
        csv_file: csvFile,
        rows: rows
      });
    } catch (error) {
      console.error('Error fetching CSV data:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // Get column data for filters
  app.get('/api/csv-columns/:csvId', async (req, res) => {
    try {
      const { csvId } = req.params;
      const csvFile = await app.locals.csvFiles.findOne({ id: csvId });
      
      if (!csvFile) {
        return res.status(404).json({ error: 'CSV file not found' });
      }
      
      // Get unique values for each column
      const columnData = {};
      for (const header of csvFile.headers) {
        const values = await app.locals.csvRows.distinct(`row_data.${header}`, { 
          csv_id: csvId 
        });
        columnData[header] = values.filter(v => v !== null && v !== undefined);
      }
      
      res.json(columnData);
    } catch (error) {
      console.error('Error fetching column data:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // Update review status
  app.post('/api/update-review-status', async (req, res) => {
    try {
      const { row_ids, is_reviewed } = req.body;
      
      if (!row_ids || !Array.isArray(row_ids)) {
        return res.status(400).json({ error: 'Invalid row_ids' });
      }
      
      const result = await app.locals.csvRows.updateMany(
        { id: { $in: row_ids } },
        { 
          $set: { 
            is_reviewed: is_reviewed,
            review_timestamp: is_reviewed ? new Date() : null
          } 
        }
      );
      
      res.json({ 
        success: true, 
        updated_count: result.modifiedCount 
      });
    } catch (error) {
      console.error('Error updating review status:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

  // Export reviewed data as CSV
  app.get('/api/export-reviewed/:csvId', async (req, res) => {
    try {
      const { csvId } = req.params;
      const csvFile = await app.locals.csvFiles.findOne({ id: csvId });
      
      if (!csvFile) {
        return res.status(404).json({ error: 'CSV file not found' });
      }
      
      const rows = await app.locals.csvRows.find({ 
        csv_id: csvId,
        is_reviewed: true 
      }).toArray();
      
      // Create CSV content
      const headers = csvFile.headers;
      let csvContent = headers.join(',') + '\n';
      
      rows.forEach(row => {
        const rowValues = headers.map(header => {
          const value = row.row_data[header] || '';
          // Properly escape CSV values
          if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
            return `"${value.replace(/"/g, '""')}"`;
          }
          return value;
        });
        csvContent += rowValues.join(',') + '\n';
      });
      
      // Set response headers for file download
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename=reviewed_${csvFile.filename}`);
      res.send(csvContent);
    } catch (error) {
      console.error('Error exporting reviewed data:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  });

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