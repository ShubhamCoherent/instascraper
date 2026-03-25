/**
 * Instagram Influencer Crawler — BullMQ + Redis powered
 *
 * Uses Redis queue for unlimited discovery. Every suggestion gets queued.
 * No limits on queue size — discover as many profiles as possible.
 *
 * Architecture:
 *   Redis SET "discovered"  → all usernames ever seen (dedup)
 *   Redis SET "scraped"     → successfully scraped usernames
 *   BullMQ Queue "scrape"   → jobs to scrape profiles
 *   BullMQ Queue "suggest"  → jobs to fetch suggestions
 *   File: all_profiles.json → combined output
 *
 * Usage:
 *   node queue-crawler.js seed                         # Seed queue from file + start
 *   node queue-crawler.js seed --file seeds.json       # Seed from custom file
 *   node queue-crawler.js start                        # Start worker (processes queue)
 *   node queue-crawler.js start --concurrency 2        # Parallel workers
 *   node queue-crawler.js status                       # Show queue stats
 *   node queue-crawler.js export                       # Export all_profiles.json
 *   node queue-crawler.js flush                        # Clear all queues + redis sets
 */

const { Queue, Worker, QueueEvents } = require('bullmq');
const Redis = require('ioredis');
const mongoose = require('mongoose');
const https = require('https');
const fs = require('fs');
const path = require('path');

// =============================================
// CONFIGURATION
// =============================================

require('dotenv').config({ path: path.join(__dirname, '.env') });

const CONFIG = {
  cookies: process.env.IG_COOKIES,
  csrfToken: process.env.IG_CSRF_TOKEN,
  userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
  igAppId: process.env.IG_APP_ID || '936619743392459',
};

const LIMITS = {
  minFollowers: 5000,
  maxFollowers: 200000,
  maxPostsPerProfile: 50,
  maxDepth: 4,
};

// Delays (ms) — safe but fast
const DELAYS = {
  betweenJobs: [5000, 8000],       // 5-8s between profile scrapes
  betweenPages: [2000, 4000],      // 2-4s between post pagination
  afterSuggestions: [3000, 6000],   // 3-6s after fetching suggestions
  onRateLimit: [90000, 120000],    // 1.5-2min on 429
  cooldownEvery: 25,               // cooldown every N jobs
  cooldownMs: [40000, 60000],      // 40-60s cooldown
};

// =============================================
// REDIS + BULLMQ SETUP
// =============================================

const DATA_DIR = path.join(__dirname, '..', 'backend', 'data');
const ALL_PROFILES_FILE = path.join(__dirname, '..', 'backend', 'all_profiles.json');

const redisConnection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
};

const scrapeQueue = new Queue('scrape', { connection: redisConnection });
const redis = new Redis(redisConnection);

// Redis keys
const DISCOVERED_KEY = 'ig:discovered';  // SET of all usernames ever seen
const SCRAPED_KEY = 'ig:scraped';        // SET of successfully scraped usernames
const STATS_KEY = 'ig:stats';            // HASH of stats

// =============================================
// MONGODB SETUP
// =============================================

const profileSchema = new mongoose.Schema({
  scraped_at: String,
  crawl_info: {
    depth: Number,
    source: String,
  },
  data: {
    username: { type: String, unique: true, index: true },
    full_name: String,
    profile_pic: String,
    is_verified: Boolean,
    pk: String,
    followers: { type: Number, index: true },
    following: Number,
    posts_count: Number,
    bio: String,
    category: String,
    external_url: String,
    is_business: Boolean,
    is_professional: Boolean,
    city: String,
    engagement_metrics: mongoose.Schema.Types.Mixed,
    post_types: mongoose.Schema.Types.Mixed,
    recent_posts: [mongoose.Schema.Types.Mixed],
  },
}, { timestamps: true });

const collectionName = process.env.MONGO_COLLECTION || 'insta_Profiles';
const Profile = mongoose.model('Profile', profileSchema, collectionName);

async function connectMongo() {
  const uri = process.env.MONGO_URI;
  if (!uri) {
    log('MONGO_URI not set — skipping MongoDB, using JSON file only');
    return false;
  }
  try {
    await mongoose.connect(uri, { dbName: process.env.MONGO_DB || 'coherent2026_db' });
    log(`Connected to MongoDB (collection: ${collectionName})`);
    return true;
  } catch (err) {
    log(`MongoDB connection failed: ${err.message}`);
    return false;
  }
}

let mongoConnected = false;

async function saveToMongo(result) {
  if (!mongoConnected) return;
  try {
    await Profile.findOneAndUpdate(
      { 'data.username': result.data.username },
      result,
      { upsert: true, returnDocument: 'after' }
    );
  } catch (err) {
    log(`  MongoDB save failed for @${result.data.username}: ${err.message}`);
  }
}

// =============================================
// HELPERS
// =============================================

function randomDelay(range) {
  const [min, max] = range;
  return new Promise(r => setTimeout(r, min + Math.random() * (max - min)));
}

function log(msg) {
  const ts = new Date().toLocaleTimeString();
  console.log(`  [${ts}] ${msg}`);
}

function decodeHtmlEntities(str) {
  return str
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, dec) => String.fromCharCode(parseInt(dec, 10)))
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#x27;/g, "'");
}

function parseMetaCount(str) {
  if (!str) return 0;
  str = str.replace(/,/g, '');
  const num = parseFloat(str);
  if (str.includes('B')) return Math.round(num * 1000000000);
  if (str.includes('M')) return Math.round(num * 1000000);
  if (str.includes('K')) return Math.round(num * 1000);
  return parseInt(str) || 0;
}

// =============================================
// HTTP
// =============================================

function httpGet(url, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
      headers: {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': CONFIG.cookies,
        'user-agent': CONFIG.userAgent,
        'sec-fetch-mode': 'navigate',
        ...extraHeaders,
      },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.end();
  });
}

function httpGetJson(url, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: 'GET',
      headers: {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cookie': CONFIG.cookies,
        'user-agent': CONFIG.userAgent,
        'x-csrftoken': CONFIG.csrfToken,
        'x-ig-app-id': CONFIG.igAppId,
        'x-ig-www-claim': 'hmac.AR0si6YYQcCivXubfm_ml_WZ_kREfaxkrMM2Q2UsZFtgRW5R',
        'x-requested-with': 'XMLHttpRequest',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'referer': 'https://www.instagram.com/',
        ...extraHeaders,
      },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode === 429) return reject(new Error('RATE_LIMITED'));
        if (res.statusCode === 401 || res.statusCode === 403) return reject(new Error('AUTH_ERROR'));
        if (res.statusCode !== 200) return reject(new Error(`HTTP_${res.statusCode}`));
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON_PARSE_ERROR')); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// =============================================
// INSTAGRAM FUNCTIONS
// =============================================

async function scrapeProfile(username) {
  const response = await httpGet(`https://www.instagram.com/${username}/`);
  if (response.status === 404) return null;
  if (response.status === 429) throw new Error('RATE_LIMITED');
  if (response.status !== 200) throw new Error(`HTTP_${response.status}`);

  const html = response.body;
  const profile = { username };

  const pkMatch = html.match(/"profilePage_(\d+)"/) ||
                  html.match(/"user_id":"(\d+)"/) ||
                  html.match(/"id":"(\d+)".*?"username":"[^"]*"/);
  if (pkMatch) profile.pk = pkMatch[1];

  const ogTitle = html.match(/<meta\s+property="og:title"\s+content="([^"]+)"/);
  const ogDesc = html.match(/<meta\s+property="og:description"\s+content="([^"]+)"/);
  const ogImage = html.match(/<meta\s+property="og:image"\s+content="([^"]+)"/);

  if (ogTitle) {
    const decoded = decodeHtmlEntities(ogTitle[1]);
    const nameMatch = decoded.match(/^(.+?)\s*\(@/);
    profile.full_name = nameMatch ? nameMatch[1].trim() : decoded;
  }

  if (ogDesc) {
    const desc = ogDesc[1].replace(/&amp;/g, '&').replace(/&#x27;/g, "'").replace(/&quot;/g, '"');
    const followersMatch = desc.match(/([\d,.]+[MKB]?)\s*Followers/i);
    const followingMatch = desc.match(/([\d,.]+[MKB]?)\s*Following/i);
    const postsMatch = desc.match(/([\d,.]+[MKB]?)\s*Posts/i);
    profile.followers = parseMetaCount(followersMatch?.[1]);
    profile.following = parseMetaCount(followingMatch?.[1]);
    profile.posts_count = parseMetaCount(postsMatch?.[1]);
  }

  // Extract real bio from JSON embedded in HTML
  const bioJsonMatch = html.match(/"biography":"((?:[^"\\]|\\.)*)"/);
  if (bioJsonMatch) {
    try {
      profile.bio = JSON.parse('"' + bioJsonMatch[1] + '"');
    } catch (e) {
      profile.bio = bioJsonMatch[1].replace(/\\n/g, '\n').replace(/\\"/g, '"');
    }
  }

  // Extract external URL if present
  const extUrlMatch = html.match(/"external_url":"((?:[^"\\]|\\.)*)"/);
  if (extUrlMatch && extUrlMatch[1] !== '') {
    try {
      profile.external_url = JSON.parse('"' + extUrlMatch[1] + '"');
    } catch (e) {
      profile.external_url = extUrlMatch[1];
    }
  }

  // Extract category from JSON
  const categoryMatch = html.match(/"category_name":"((?:[^"\\]|\\.)*)"/);
  if (categoryMatch) profile.category = categoryMatch[1];

  if (ogImage) profile.profile_pic = ogImage[1].replace(/&amp;/g, '&');
  profile.is_verified = html.includes('is_verified":true') || html.includes('isVerified":true');
  profile.is_private = html.includes('is_private":true');

  return profile;
}

async function fetchUserInfo(pk) {
  const url = `https://i.instagram.com/api/v1/users/${pk}/info/`;
  const json = await httpGetJson(url, {
    'user-agent': 'Instagram 317.0.0.34.109 Android (30/11; 420dpi; 1080x2220; Google; Pixel 5; redfin; redfin; en_US; 562804463)',
  });
  const u = json?.user;
  if (!u) return null;
  return {
    biography: u.biography || '',
    category: u.category || '',
    external_url: u.external_url || '',
    follower_count: u.follower_count,
    following_count: u.following_count,
    media_count: u.media_count,
    is_business: u.is_business || false,
    is_professional: u.is_professional_account || false,
    public_email: u.public_email || '',
    contact_phone_number: u.contact_phone_number || '',
    city_name: u.city_name || '',
  };
}

async function getSuggestedUsers(userId) {
  const url = `https://i.instagram.com/api/v1/discover/chaining/?target_id=${userId}`;
  const json = await httpGetJson(url, {
    'user-agent': 'Instagram 317.0.0.34.109 Android (30/11; 420dpi; 1080x2220; Google; Pixel 5; redfin; redfin; en_US; 562804463)',
  });
  return json?.users || [];
}

function httpPostGraphQL(body) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'www.instagram.com',
      port: 443,
      path: '/graphql/query',
      method: 'POST',
      headers: {
        'accept': '*/*',
        'accept-encoding': 'identity',
        'content-type': 'application/x-www-form-urlencoded',
        'content-length': Buffer.byteLength(body),
        'cookie': CONFIG.cookies,
        'user-agent': CONFIG.userAgent,
        'origin': 'https://www.instagram.com',
        'referer': 'https://www.instagram.com/',
        'x-csrftoken': CONFIG.csrfToken,
        'x-fb-lsd': process.env.IG_LSD,
        'x-ig-app-id': CONFIG.igAppId,
      },
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode === 429) return reject(new Error('RATE_LIMITED'));
        if (res.statusCode !== 200) return reject(new Error(`HTTP_${res.statusCode}`));
        try { resolve(JSON.parse(data)); }
        catch (e) { reject(new Error('JSON_PARSE_ERROR')); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function buildPostsPayload(username, cursor = null) {
  const querystring = require('querystring');
  return querystring.stringify({
    __d: 'www',
    __user: '0',
    __a: '1',
    __req: '1',
    dpr: '1',
    __ccg: 'EXCELLENT',
    fb_dtsg: process.env.IG_FB_DTSG,
    jazoest: process.env.IG_JAZOEST,
    lsd: process.env.IG_LSD,
    doc_id: process.env.IG_DOC_ID,
    fb_api_caller_class: 'RelayModern',
    fb_api_req_friendly_name: 'PolarisProfilePostsTabContentQuery_connection',
    server_timestamps: 'true',
    variables: JSON.stringify({
      after: cursor,
      before: null,
      data: { count: 12 },
      first: 12,
      last: null,
      username: username,
    }),
  });
}

async function fetchUserPosts(username, maxPosts = 50) {
  const allPosts = [];
  let cursor = null;

  while (allPosts.length < maxPosts) {
    const body = buildPostsPayload(username, cursor);
    const json = await httpPostGraphQL(body);

    const connection = json?.data?.xdt_api__v1__feed__user_timeline_graphql_connection;
    if (!connection) break;

    const edges = connection.edges || [];
    if (edges.length === 0) break;

    for (const edge of edges) {
      if (allPosts.length >= maxPosts) break;
      allPosts.push(edge.node);
    }

    if (!connection.page_info?.has_next_page) break;
    cursor = connection.page_info.end_cursor;

    if (allPosts.length < maxPosts) {
      await randomDelay(DELAYS.betweenPages);
    }
  }

  return allPosts.slice(0, maxPosts);
}

function parsePost(node, followers) {
  const captionText = node.caption?.text || '';
  const userTags = node.usertags?.in?.map(t => t.user?.username).filter(Boolean) || [];
  const captionMentions = (captionText.match(/@[\w.]+/g) || []).map(m => m.slice(1));
  const allMentions = [...new Set([...userTags, ...captionMentions])];
  const hashtags = (captionText.match(/#[\w]+/g) || []).map(h => h.slice(1));
  const sponsorTags = (node.sponsor_tags || []).map(s => s.username || s.name).filter(Boolean);
  const coauthors = (node.coauthor_producers || []).map(c => c.username).filter(Boolean);

  const likeCount = node.like_count || 0;
  const commentCount = node.comment_count || 0;

  return {
    code: node.code,
    url: `https://www.instagram.com/p/${node.code}/`,
    caption: captionText,
    timestamp: node.taken_at,
    date: new Date(node.taken_at * 1000).toLocaleDateString('en-US', { day: 'numeric', month: 'short', year: '2-digit' }),
    likes: likeCount,
    comments: commentCount,
    views: node.view_count || node.play_count || 0,
    post_type: node.product_type === 'clips' ? 'Reel' : 'Post',
    media_type: node.media_type,
    is_paid_partnership: node.is_paid_partnership || false,
    mentions: allMentions,
    hashtags,
    sponsor_tags: sponsorTags,
    coauthors,
    carousel_count: node.carousel_media_count || null,
    image_url: node.image_versions2?.candidates?.[0]?.url || null,
    engagement_rate: followers > 0 ? parseFloat(((likeCount + commentCount) / followers * 100).toFixed(4)) : 0,
  };
}

function computeMetrics(posts, followers) {
  const count = posts.length;
  if (count === 0) return null;

  const totalLikes = posts.reduce((s, p) => s + p.likes, 0);
  const totalComments = posts.reduce((s, p) => s + p.comments, 0);
  const totalViews = posts.reduce((s, p) => s + p.views, 0);

  let postsPerWeek = 0;
  if (count >= 2) {
    const sorted = [...posts].sort((a, b) => a.timestamp - b.timestamp);
    const weeks = (sorted[count - 1].timestamp - sorted[0].timestamp) / (7 * 24 * 60 * 60);
    postsPerWeek = weeks > 0 ? parseFloat((count / weeks).toFixed(1)) : count;
  }

  const postTypes = {};
  posts.forEach(p => { postTypes[p.post_type] = (postTypes[p.post_type] || 0) + 1; });

  return {
    posts_analyzed: count,
    total_likes: totalLikes,
    total_comments: totalComments,
    total_views: totalViews,
    avg_likes: Math.round(totalLikes / count),
    avg_comments: Math.round(totalComments / count),
    avg_views: Math.round(totalViews / count),
    engagement_rate: followers > 0
      ? parseFloat(((totalLikes + totalComments) / count / followers * 100).toFixed(2))
      : 0,
    posts_per_week: postsPerWeek,
    partnership_posts: posts.filter(p => p.is_paid_partnership).length,
    post_types: postTypes,
  };
}

// =============================================
// SKIPPED PROFILES LOG
// =============================================

const SKIPPED_FILE = path.join(__dirname, '..', 'backend', 'skipped_profiles.json');

function logSkipped(username, followers, reason, depth, source) {
  let skipped = { total: 0, profiles: [] };
  if (fs.existsSync(SKIPPED_FILE)) {
    try { skipped = JSON.parse(fs.readFileSync(SKIPPED_FILE, 'utf8')); } catch (e) {}
  }

  // Avoid duplicates
  if (skipped.profiles.some(p => p.username === username)) return;

  skipped.profiles.push({
    username,
    followers,
    reason,
    depth,
    source,
    skipped_at: new Date().toISOString(),
  });

  skipped.total = skipped.profiles.length;
  skipped.updated_at = new Date().toISOString();
  fs.writeFileSync(SKIPPED_FILE, JSON.stringify(skipped, null, 2));
}

// =============================================
// JOB PROCESSOR
// =============================================

let jobCount = 0;

async function processJob(job) {
  const { username, depth, source } = job.data;
  jobCount++;

  // Check if already scraped
  const alreadyScraped = await redis.sismember(SCRAPED_KEY, username);
  if (alreadyScraped) return { status: 'skipped', reason: 'already_scraped' };

  log(`[${jobCount}] Scraping @${username} (depth: ${depth}, from: @${source || 'seed'})`);

  // Cooldown check
  if (jobCount > 0 && jobCount % DELAYS.cooldownEvery === 0) {
    const cooldown = DELAYS.cooldownMs[0] + Math.random() * (DELAYS.cooldownMs[1] - DELAYS.cooldownMs[0]);
    log(`  Cooldown: ${(cooldown / 1000).toFixed(0)}s after ${jobCount} jobs...`);
    await new Promise(r => setTimeout(r, cooldown));
  }

  // Step 1: Scrape profile
  let profile;
  try {
    profile = await scrapeProfile(username);
  } catch (err) {
    if (err.message === 'RATE_LIMITED') {
      log(`  Rate limited! Waiting...`);
      await randomDelay(DELAYS.onRateLimit);
      throw err; // BullMQ will retry
    }
    throw err;
  }

  if (!profile) return { status: 'skipped', reason: 'not_found' };
  if (profile.is_private) return { status: 'skipped', reason: 'private' };

  const followers = profile.followers || 0;

  if (followers < LIMITS.minFollowers) {
    log(`  @${username}: ${followers.toLocaleString()} followers (< ${LIMITS.minFollowers.toLocaleString()}). Skip.`);
    return { status: 'skipped', reason: 'too_few_followers' };
  }

  if (followers > LIMITS.maxFollowers) {
    log(`  @${username}: ${followers.toLocaleString()} followers (> ${LIMITS.maxFollowers.toLocaleString()}). Skip.`);
    logSkipped(username, followers, 'too_many_followers', depth, source);
    return { status: 'skipped', reason: 'too_many_followers' };
  }

  log(`  @${username}: ${followers.toLocaleString()} followers`);

  // Step 2: Fetch posts (with retry on 302)
  await randomDelay(DELAYS.betweenPages);
  let rawPosts = [];
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      rawPosts = await fetchUserPosts(username, LIMITS.maxPostsPerProfile);
      break;
    } catch (err) {
      if (err.message === 'HTTP_302' && attempt === 1) {
        log(`  Posts 302 for @${username}, retrying in 5s...`);
        await new Promise(r => setTimeout(r, 5000));
        continue;
      }
      log(`  Posts failed for @${username}: ${err.message}`);
      break;
    }
  }

  const parsedPosts = rawPosts.map(node => parsePost(node, profile.followers));
  const metrics = computeMetrics(parsedPosts, profile.followers);

  // Don't save profiles with 0 posts — re-queue them for later
  if (parsedPosts.length === 0) {
    log(`  @${username}: 0 posts fetched, skipping save (will retry later)`);
    await redis.srem(SCRAPED_KEY, username); // allow retry
    return { status: 'skipped', reason: 'no_posts_fetched' };
  }

  // Step 3: Fetch user info (bio, category, etc.) via mobile API
  let userInfo = {};
  if (profile.pk) {
    try {
      await randomDelay([1000, 2000]);
      userInfo = await fetchUserInfo(profile.pk) || {};
    } catch (err) {
      log(`  User info failed for @${username}: ${err.message}`);
    }
  }

  // Step 4: Build result & save
  const result = {
    scraped_at: new Date().toISOString(),
    crawl_info: { depth, source },
    data: {
      username: profile.username || username,
      full_name: profile.full_name || username,
      profile_pic: profile.profile_pic || '',
      is_verified: profile.is_verified || false,
      pk: profile.pk || '',
      followers: userInfo.follower_count || profile.followers,
      following: userInfo.following_count || profile.following || 0,
      posts_count: userInfo.media_count || profile.posts_count || 0,
      bio: userInfo.biography || profile.bio || '',
      category: userInfo.category || '',
      external_url: userInfo.external_url || '',
      is_business: userInfo.is_business || false,
      is_professional: userInfo.is_professional || false,
      city: userInfo.city_name || '',
      engagement_metrics: metrics,
      post_types: metrics?.post_types || {},
      recent_posts: parsedPosts,
    },
  };

  // Save to MongoDB + JSON file
  await saveToMongo(result);
  appendProfile(result);

  // Mark as scraped in Redis
  await redis.sadd(SCRAPED_KEY, username);
  await redis.hincrby(STATS_KEY, 'profiles_scraped', 1);

  log(`  Saved @${username} → ${parsedPosts.length} posts, ${metrics?.engagement_rate || 0}% eng`);

  // Step 4: Fetch suggestions and queue them (NO LIMIT on queue size)
  if (depth < LIMITS.maxDepth && profile.pk) {
    await randomDelay(DELAYS.afterSuggestions);
    try {
      const suggested = await getSuggestedUsers(profile.pk);
      let newCount = 0;

      for (const user of suggested) {
        if (user.is_private) continue;
        const added = await redis.sadd(DISCOVERED_KEY, user.username);
        if (added === 1) {
          // New username — add to queue
          await scrapeQueue.add('scrape', {
            username: user.username,
            pk: user.pk?.toString(),
            depth: depth + 1,
            source: username,
          }, {
            attempts: 3,
            backoff: { type: 'exponential', delay: 30000 },
            removeOnComplete: true,
            removeOnFail: 50,
          });
          newCount++;
        }
      }

      await redis.hincrby(STATS_KEY, 'suggestions_found', newCount);
      log(`  Suggestions: ${suggested.length} found, ${newCount} new queued`);
    } catch (err) {
      log(`  Suggestions failed: ${err.message}`);
    }
  }

  // Delay before next job
  await randomDelay(DELAYS.betweenJobs);

  return { status: 'scraped', username, followers: profile.followers };
}

// =============================================
// FILE OUTPUT
// =============================================

function appendProfile(result) {
  let existing = { total_profiles: 0, profiles: [] };

  if (fs.existsSync(ALL_PROFILES_FILE)) {
    try {
      existing = JSON.parse(fs.readFileSync(ALL_PROFILES_FILE, 'utf8'));
    } catch (e) {}
  }

  // Avoid duplicates in file
  const idx = existing.profiles.findIndex(p => p.data.username === result.data.username);
  if (idx >= 0) {
    existing.profiles[idx] = result; // Update existing
  } else {
    existing.profiles.push(result);
  }

  existing.total_profiles = existing.profiles.length;
  existing.generated_at = new Date().toISOString();

  // Sort by followers desc
  existing.profiles.sort((a, b) => b.data.followers - a.data.followers);

  if (!fs.existsSync(path.dirname(ALL_PROFILES_FILE))) {
    fs.mkdirSync(path.dirname(ALL_PROFILES_FILE), { recursive: true });
  }
  fs.writeFileSync(ALL_PROFILES_FILE, JSON.stringify(existing, null, 2));
}

// =============================================
// COMMANDS
// =============================================

async function seedQueue(seedFile) {
  const filePath = seedFile || path.join(__dirname, '..', 'seeds', 'micro_influencers_india.json');
  const seedData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  const usernames = seedData.usernames || seedData;

  let added = 0;
  for (const username of usernames) {
    const isNew = await redis.sadd(DISCOVERED_KEY, username);
    if (isNew === 1) {
      await scrapeQueue.add('scrape', {
        username,
        pk: null,
        depth: 0,
        source: null,
      }, {
        attempts: 3,
        backoff: { type: 'exponential', delay: 30000 },
        removeOnComplete: true,
        removeOnFail: 50,
      });
      added++;
    }
  }

  log(`Seeded ${added} new usernames (${usernames.length - added} already discovered)`);

  // Also seed from existing all_profiles.json (mark as already scraped)
  if (fs.existsSync(ALL_PROFILES_FILE)) {
    const existing = JSON.parse(fs.readFileSync(ALL_PROFILES_FILE, 'utf8'));
    let recovered = 0;
    for (const p of existing.profiles) {
      const username = p.data.username;
      await redis.sadd(DISCOVERED_KEY, username);
      const wasNew = await redis.sadd(SCRAPED_KEY, username);
      if (wasNew === 1) recovered++;
    }
    if (recovered > 0) log(`Marked ${recovered} existing profiles as already scraped`);
  }

  await showStatus();
}

async function startWorker(concurrency = 1) {
  log(`Starting worker (concurrency: ${concurrency})...`);

  const worker = new Worker('scrape', processJob, {
    connection: redisConnection,
    concurrency,
    limiter: {
      max: 1,
      duration: 5000, // Max 1 job per 5 seconds
    },
  });

  worker.on('completed', (job, result) => {
    if (result?.status === 'scraped') {
      redis.hget(STATS_KEY, 'profiles_scraped').then(count => {
        if (count % 10 === 0) showStatus();
      });
    }
  });

  worker.on('failed', (job, err) => {
    log(`  Job failed @${job?.data?.username}: ${err.message}`);
    redis.hincrby(STATS_KEY, 'failed', 1);
  });

  worker.on('error', (err) => {
    if (err.message === 'AUTH_ERROR') {
      log('\nSession expired! Update cookies and restart.');
      process.exit(1);
    }
  });

  log('Worker running. Press Ctrl+C to stop.\n');

  // Graceful shutdown
  process.on('SIGINT', async () => {
    log('\nShutting down gracefully...');
    await worker.close();
    await showStatus();
    process.exit(0);
  });
}

async function showStatus() {
  const waiting = await scrapeQueue.getWaitingCount();
  const active = await scrapeQueue.getActiveCount();
  const completed = await scrapeQueue.getCompletedCount();
  const failed = await scrapeQueue.getFailedCount();
  const delayed = await scrapeQueue.getDelayedCount();
  const discovered = await redis.scard(DISCOVERED_KEY);
  const scraped = await redis.scard(SCRAPED_KEY);
  const stats = await redis.hgetall(STATS_KEY);

  console.log('\n' + '='.repeat(50));
  console.log('  QUEUE STATUS');
  console.log('='.repeat(50));
  console.log(`  Waiting:     ${waiting}`);
  console.log(`  Active:      ${active}`);
  console.log(`  Delayed:     ${delayed}`);
  console.log(`  Completed:   ${completed}`);
  console.log(`  Failed:      ${failed}`);
  console.log('  ---');
  console.log(`  Discovered:  ${discovered}`);
  console.log(`  Scraped:     ${scraped}`);
  console.log(`  Suggestions: ${stats.suggestions_found || 0}`);
  console.log('='.repeat(50) + '\n');
}

async function exportProfiles() {
  if (fs.existsSync(ALL_PROFILES_FILE)) {
    const data = JSON.parse(fs.readFileSync(ALL_PROFILES_FILE, 'utf8'));
    log(`all_profiles.json has ${data.total_profiles} profiles`);
    log(`File: ${ALL_PROFILES_FILE}`);
  } else {
    log('No all_profiles.json found.');
  }
}

async function flushAll() {
  await scrapeQueue.drain();
  await scrapeQueue.clean(0, 0, 'completed');
  await scrapeQueue.clean(0, 0, 'failed');
  await redis.del(DISCOVERED_KEY, SCRAPED_KEY, STATS_KEY);
  log('All queues and Redis keys flushed.');
}

// =============================================
// CLI
// =============================================

async function main() {
  const args = process.argv.slice(2);
  const command = args[0];

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  // Connect to MongoDB
  mongoConnected = await connectMongo();

  switch (command) {
    case 'seed': {
      const fileIdx = args.indexOf('--file');
      const seedFile = fileIdx >= 0 ? args[fileIdx + 1] : null;
      await seedQueue(seedFile);
      break;
    }
    case 'start': {
      const concIdx = args.indexOf('--concurrency');
      const concurrency = concIdx >= 0 ? parseInt(args[concIdx + 1]) || 1 : 1;
      await startWorker(concurrency);
      // Keep process alive
      await new Promise(() => {});
      break;
    }
    case 'status':
      await showStatus();
      break;
    case 'export':
      await exportProfiles();
      break;
    case 'flush':
      await flushAll();
      break;
    case 'migrate': {
      if (!mongoConnected) { log('MongoDB not connected'); break; }
      if (fs.existsSync(ALL_PROFILES_FILE)) {
        const data = JSON.parse(fs.readFileSync(ALL_PROFILES_FILE, 'utf8'));
        let migrated = 0;
        for (const profile of data.profiles) {
          await saveToMongo(profile);
          migrated++;
          if (migrated % 50 === 0) log(`  Migrated ${migrated}/${data.profiles.length}`);
        }
        log(`Migration complete: ${migrated} profiles pushed to MongoDB`);
      } else {
        log('No all_profiles.json found');
      }
      break;
    }
    default:
      console.log(`
Usage:
  node queue-crawler.js seed                      Seed queue from default seeds file
  node queue-crawler.js seed --file <path>        Seed from custom file
  node queue-crawler.js start                     Start processing queue
  node queue-crawler.js start --concurrency 2     Start with parallel workers
  node queue-crawler.js status                    Show queue statistics
  node queue-crawler.js export                    Show export file info
  node queue-crawler.js flush                     Clear everything
      `);
  }

  // Close connections (except for 'start' which stays alive)
  if (command !== 'start') {
    await redis.quit();
    process.exit(0);
  }
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
