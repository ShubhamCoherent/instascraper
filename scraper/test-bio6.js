require('dotenv').config();
const https = require('https');
const querystring = require('querystring');

const body = querystring.stringify({
  __d: 'www', __user: '0', __a: '1', __req: '1', dpr: '1', __ccg: 'EXCELLENT',
  fb_dtsg: process.env.IG_FB_DTSG,
  jazoest: process.env.IG_JAZOEST,
  lsd: process.env.IG_LSD,
  doc_id: process.env.IG_DOC_ID,
  fb_api_caller_class: 'RelayModern',
  fb_api_req_friendly_name: 'PolarisProfilePostsTabContentQuery_connection',
  server_timestamps: 'true',
  variables: JSON.stringify({ after: null, before: null, data: { count: 1 }, first: 1, last: null, username: 'foodholic.travels' }),
});

const options = {
  hostname: 'www.instagram.com', path: '/graphql/query', method: 'POST',
  headers: {
    'content-type': 'application/x-www-form-urlencoded',
    'content-length': Buffer.byteLength(body),
    'cookie': process.env.IG_COOKIES,
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
    'x-csrftoken': process.env.IG_CSRF_TOKEN,
    'x-fb-lsd': process.env.IG_LSD,
    'x-ig-app-id': '936619743392459',
    'origin': 'https://www.instagram.com',
    'referer': 'https://www.instagram.com/',
  },
};

const req = https.request(options, (res) => {
  let data = '';
  res.on('data', c => data += c);
  res.on('end', () => {
    console.log('Status:', res.statusCode);
    if (res.statusCode === 200) {
      const json = JSON.parse(data);
      const edge = json?.data?.xdt_api__v1__feed__user_timeline_graphql_connection?.edges?.[0];
      if (edge) {
        const u = edge.node.user;
        console.log('\nUser fields from GraphQL:', Object.keys(u).join(', '));
        console.log('Username:', u.username);
        console.log('Full name:', u.full_name);
        console.log('Biography:', u.biography);
        console.log('Bio links:', u.bio_links);
      }
    }
  });
});
req.write(body);
req.end();
