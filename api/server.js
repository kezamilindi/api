const express = require('express');
const path = require('path');
const { router } = require('./api.js'); // Make sure api.js is in the same directory

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Mount API routes
app.use('/api', router);

// Serve the test page at the root URL
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({
        error: 'Internal Server Error',
        message: err.message
    });
});

// Start server
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
    console.log('Available endpoints:');
    console.log(`- Test UI: http://localhost:${PORT}`);
    console.log(`- GET: http://localhost:${PORT}/api/recommendations?userId=YOUR_USER_ID&contentType=video`);
    console.log(`- POST: http://localhost:${PORT}/api/recommendations`);
    console.log(`- Health Check: http://localhost:${PORT}/api/health`);
});