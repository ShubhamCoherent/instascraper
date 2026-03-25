require('dotenv').config();
const https = require('https');

const url = 'https://www.instagram.com/virat.kohli/';
const req = https.request(url, { headers: {
  'cookie': process.env.IG_COOKIES,
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
}}, (res) => {
  let data = '';
  res.on('data', c => data += c);
  res.on('end', () => {
    const bioMatch = data.match(/"biography":"((?:[^"\\]|\\.)*)"/);
    const catMatch = data.match(/"category_name":"((?:[^"\\]|\\.)*)"/);
    const extMatch = data.match(/"external_url":"((?:[^"\\]|\\.)*)"/);
    console.log('Bio:', bioMatch ? JSON.parse('"' + bioMatch[1] + '"') : 'NOT FOUND');
    console.log('Category:', catMatch ? catMatch[1] : 'NOT FOUND');
    console.log('External URL:', extMatch && extMatch[1] ? extMatch[1] : 'NONE');
  });
});
req.end();
