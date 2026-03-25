/**
 * Backend API Server
 *
 * Serves pre-scraped influencer data from JSON files.
 * No scraping happens here — data is produced by the scraper (../scraper/).
 */

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const https = require('https');

const app = express();
const PORT = process.env.PORT || 3001;
const DATA_DIR = path.join(__dirname, '..', 'data');

app.use(cors());
app.use(express.json());

// ---------- HELPERS ----------

function loadProfileData(username) {
  const filePath = path.join(DATA_DIR, `${username}_complete.json`);
  if (!fs.existsSync(filePath)) {
    return null;
  }
  const raw = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(raw);
}

function computeMedian(arr) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function computeMonthlyStats(posts) {
  if (!posts || posts.length === 0) return [];

  const buckets = {};
  posts.forEach(post => {
    const date = new Date(post.timestamp * 1000);
    const key = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
    if (!buckets[key]) {
      buckets[key] = { month: key, posts: [], totalLikes: 0, totalComments: 0, totalViews: 0, engagementRates: [] };
    }
    buckets[key].posts.push(post);
    buckets[key].totalLikes += post.likes || 0;
    buckets[key].totalComments += post.comments || 0;
    buckets[key].totalViews += post.views || 0;
    if (post.engagement_rate > 0) {
      buckets[key].engagementRates.push(post.engagement_rate);
    }
  });

  return Object.values(buckets)
    .sort((a, b) => a.month.localeCompare(b.month))
    .map(b => ({
      month: b.month,
      label: new Date(b.month + '-15').toLocaleDateString('en-US', { month: 'short', year: '2-digit' }),
      post_count: b.posts.length,
      total_likes: b.totalLikes,
      total_comments: b.totalComments,
      total_views: b.totalViews,
      total_engagements: b.totalLikes + b.totalComments,
      avg_likes: Math.round(b.totalLikes / b.posts.length),
      avg_comments: Math.round(b.totalComments / b.posts.length),
      avg_engagement_rate: b.engagementRates.length > 0
        ? parseFloat((b.engagementRates.reduce((s, r) => s + r, 0) / b.engagementRates.length).toFixed(4))
        : 0,
    }));
}

function computePartnershipDetails(posts, influencerUsername) {
  const paidPosts = (posts || []).filter(p => p.is_paid_partnership);
  if (paidPosts.length === 0) return [];

  const brandMap = {};

  paidPosts.forEach(post => {
    // Identify brand from: sponsor_tags > mentions > hashtags
    const brands = [];

    // 1. Sponsor tags (most reliable, from Instagram's API)
    if (post.sponsor_tags && post.sponsor_tags.length > 0) {
      brands.push(...post.sponsor_tags);
    }

    // 2. Coauthors
    if (post.coauthors && post.coauthors.length > 0) {
      brands.push(...post.coauthors);
    }

    // 3. Mentions (exclude self)
    if (post.mentions && post.mentions.length > 0) {
      const filtered = post.mentions
        .map(m => m.replace(/\.$/, ''))  // remove trailing dots
        .filter(m => m.toLowerCase() !== influencerUsername.toLowerCase());
      brands.push(...filtered);
    }

    // 4. If no brand found from above, extract from hashtags
    if (brands.length === 0 && post.hashtags && post.hashtags.length > 0) {
      brands.push(post.hashtags[0]);
    }

    // Deduplicate and normalize
    const uniqueBrands = [...new Set(brands.map(b => b.toLowerCase()))];
    if (uniqueBrands.length === 0) uniqueBrands.push('unknown');

    uniqueBrands.forEach(brand => {
      if (!brandMap[brand]) {
        brandMap[brand] = { brand, posts: [], total_likes: 0, total_comments: 0, hashtags: new Set() };
      }
      brandMap[brand].posts.push({
        code: post.code,
        url: post.url,
        date: post.date,
        likes: post.likes,
        comments: post.comments,
        caption: post.caption ? post.caption.substring(0, 120) : '',
        post_type: post.post_type,
        image_url: post.image_url,
      });
      brandMap[brand].total_likes += post.likes || 0;
      brandMap[brand].total_comments += post.comments || 0;

      // Collect hashtags from this post
      const postHashtags = (post.caption || '').match(/#[\w]+/g) || [];
      postHashtags.forEach(h => brandMap[brand].hashtags.add(h));
    });
  });

  return Object.values(brandMap)
    .map(b => ({
      brand: b.brand,
      post_count: b.posts.length,
      total_likes: b.total_likes,
      total_comments: b.total_comments,
      avg_likes: Math.round(b.total_likes / b.posts.length),
      avg_engagement: b.total_likes + b.total_comments,
      hashtags: [...b.hashtags],
      posts: b.posts,
    }))
    .sort((a, b) => b.post_count - a.post_count || b.avg_engagement - a.avg_engagement);
}

// ---------- ROUTES ----------

/**
 * GET /api/profile/:username
 * Returns profile info + engagement metrics (without posts)
 */
app.get('/api/profile/:username', (req, res) => {
  const { username } = req.params;
  const scraped = loadProfileData(username);

  if (!scraped || !scraped.data) {
    return res.status(404).json({
      error: 'Profile not found',
      message: `No data available for @${username}. Run the scraper first.`,
    });
  }

  const data = scraped.data;
  const posts = data.recent_posts || [];
  const metrics = data.engagement_metrics || {};

  res.json({
    username: data.username,
    full_name: data.full_name,
    profile_pic: data.profile_pic,
    is_verified: data.is_verified,
    followers: data.followers,
    following: data.following,
    posts_count: data.posts_count,
    bio: data.bio || '',
    engagement_metrics: {
      ...metrics,
      median_likes: computeMedian(posts.map(p => p.likes || 0)),
      median_comments: computeMedian(posts.map(p => p.comments || 0)),
      median_engagements: computeMedian(posts.map(p => (p.likes || 0) + (p.comments || 0))),
      median_engagement_rate: computeMedian(posts.filter(p => p.engagement_rate > 0).map(p => p.engagement_rate)),
    },
    post_types: data.post_types,
    monthly_stats: computeMonthlyStats(posts),
    partnership_details: computePartnershipDetails(posts, data.username),
    scraped_at: scraped.scraped_at,
    methods: scraped.methods || [],
    total_posts_available: posts.length,
  });
});

/**
 * GET /api/profile/:username/posts
 * Returns paginated posts
 *
 * Query params:
 *   sort   - "date" (default), "likes", "comments"
 *   size   - posts per page (default 8)
 *   offset - starting index (default 0)
 */
app.get('/api/profile/:username/posts', (req, res) => {
  const { username } = req.params;
  const sort = req.query.sort || 'date';
  const size = Math.min(parseInt(req.query.size) || 8, 50);
  const offset = parseInt(req.query.offset) || 0;

  const scraped = loadProfileData(username);

  if (!scraped || !scraped.data) {
    return res.status(404).json({
      error: 'Profile not found',
      message: `No data available for @${username}. Run the scraper first.`,
    });
  }

  let posts = [...(scraped.data.recent_posts || [])];

  // Sort
  switch (sort) {
    case 'likes':
      posts.sort((a, b) => (b.likes || 0) - (a.likes || 0));
      break;
    case 'comments':
      posts.sort((a, b) => (b.comments || 0) - (a.comments || 0));
      break;
    case 'date':
    default:
      posts.sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
      break;
  }

  const total = posts.length;
  const paginatedPosts = posts.slice(offset, offset + size);

  res.json({
    posts: paginatedPosts,
    total,
    offset,
    size,
    sort,
    has_more: offset + size < total,
  });
});

/**
 * GET /api/profiles
 * Lists all available scraped profiles
 */
app.get('/api/profiles', (req, res) => {
  if (!fs.existsSync(DATA_DIR)) {
    return res.json({ profiles: [] });
  }

  const files = fs.readdirSync(DATA_DIR)
    .filter(f => f.endsWith('_complete.json'));

  const profiles = files.map(f => {
    const username = f.replace('_complete.json', '');
    try {
      const data = loadProfileData(username);
      return {
        username: data?.data?.username || username,
        full_name: data?.data?.full_name || username,
        profile_pic: data?.data?.profile_pic || '',
        is_verified: data?.data?.is_verified || false,
        followers: data?.data?.followers || 0,
        posts_available: (data?.data?.recent_posts || []).length,
        scraped_at: data?.scraped_at || null,
      };
    } catch {
      return { username, error: 'Failed to load' };
    }
  });

  res.json({ profiles });
});

/**
 * GET /api/image-proxy
 * Proxies Instagram CDN images to avoid referrer-based blocking.
 * Query: ?url=<encoded instagram CDN url>
 */
app.get('/api/image-proxy', (req, res) => {
  const imageUrl = req.query.url;
  if (!imageUrl) {
    return res.status(400).json({ error: 'Missing url parameter' });
  }

  try {
    const parsed = new URL(imageUrl);
    // Only allow Instagram CDN domains
    if (!parsed.hostname.includes('instagram') && !parsed.hostname.includes('fbcdn')) {
      return res.status(403).json({ error: 'Only Instagram image URLs allowed' });
    }

    https.get(imageUrl, { headers: { 'User-Agent': 'Mozilla/5.0' } }, (proxyRes) => {
      if (proxyRes.statusCode !== 200) {
        return res.status(proxyRes.statusCode).end();
      }
      res.setHeader('Content-Type', proxyRes.headers['content-type'] || 'image/jpeg');
      res.setHeader('Cache-Control', 'public, max-age=86400');
      proxyRes.pipe(res);
    }).on('error', () => {
      res.status(502).json({ error: 'Failed to fetch image' });
    });
  } catch {
    res.status(400).json({ error: 'Invalid URL' });
  }
});

// ---------- START ----------

app.listen(PORT, () => {
  console.log(`\n  Backend API running on http://localhost:${PORT}`);
  console.log(`  Data directory: ${DATA_DIR}\n`);
  console.log('  Endpoints:');
  console.log('    GET /api/profiles                     - List all profiles');
  console.log('    GET /api/profile/:username             - Profile info + metrics');
  console.log('    GET /api/profile/:username/posts       - Paginated posts');
  console.log('        ?sort=date|likes|comments');
  console.log('        &size=8&offset=0\n');
});
