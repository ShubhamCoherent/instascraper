require('dotenv').config();
const https = require('https');

// First get pk from search, then get full info
const pk = '2880775926'; // virat.kohli's pk
const url = 'https://i.instagram.com/api/v1/users/' + pk + '/info/';
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
      const u = json.user;
      if (u) {
        console.log('Username:', u.username);
        console.log('Bio:', u.biography);
        console.log('Full name:', u.full_name);
        console.log('Followers:', u.follower_count);
        console.log('Following:', u.following_count);
        console.log('Category:', u.category);
        console.log('External URL:', u.external_url);
        console.log('Is verified:', u.is_verified);
        console.log('Is business:', u.is_business);
      }
    } else {
      console.log(data.substring(0, 500));
    }
  });
});
req.end();
