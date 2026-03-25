require('dotenv').config();
const https = require('https');

// The chaining/suggestions API returns full user objects with bio
const url = 'https://i.instagram.com/api/v1/discover/chaining/?target_id=2880775926';
const req = https.request(url, { headers: {
  'cookie': process.env.IG_COOKIES,
  'user-agent': 'Instagram 317.0.0.34.109 Android (30/11; 420dpi; 1080x2220; Google; Pixel 5; redfin; redfin; en_US; 562804463)',
  'x-ig-app-id': '936619743392459',
}}, (res) => {
  let data = '';
  res.on('data', c => data += c);
  res.on('end', () => {
    console.log('Status:', res.statusCode);
    if (res.statusCode === 200) {
      const json = JSON.parse(data);
      const u = json.users?.[0];
      if (u) {
        console.log('\nFirst suggested user:');
        console.log('Username:', u.username);
        console.log('Bio:', u.biography);
        console.log('Full name:', u.full_name);
        console.log('Followers:', u.follower_count);
        console.log('Category:', u.category);
        console.log('External URL:', u.external_url);
        console.log('Is verified:', u.is_verified);
        console.log('\nAll available fields:', Object.keys(u).join(', '));
      }
    } else {
      console.log(data.substring(0, 500));
    }
  });
});
req.end();
