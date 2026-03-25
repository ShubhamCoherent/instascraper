require('dotenv').config();
const https = require('https');

const url = 'https://www.instagram.com/api/v1/users/web_profile_info/?username=virat.kohli';
const req = https.request(url, { headers: {
  'cookie': process.env.IG_COOKIES,
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
  'x-ig-app-id': '936619743392459',
  'x-csrftoken': process.env.IG_CSRF_TOKEN,
  'x-ig-www-claim': 'hmac.AR0si6YYQcCivXubfm_ml_WZ_kREfaxkrMM2Q2UsZFtgRW5R',
  'referer': 'https://www.instagram.com/',
}}, (res) => {
  let data = '';
  res.on('data', c => data += c);
  res.on('end', () => {
    console.log('Status:', res.statusCode);
    if (res.statusCode === 200) {
      const json = JSON.parse(data);
      const u = json.data?.user;
      console.log('Bio:', u?.biography);
      console.log('Category:', u?.category_name);
      console.log('External URL:', u?.external_url);
      console.log('Followers:', u?.edge_followed_by?.count);
      console.log('Full name:', u?.full_name);
      console.log('Is verified:', u?.is_verified);
    } else {
      console.log(data.substring(0, 300));
    }
  });
});
req.end();
