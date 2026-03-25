/**
 * Instagram Influencer Crawler - Recursive Suggestion Tree
 *
 * Strategy:
 *   1. Start from seed Indian influencer(s)
 *   2. Scrape their profile (followers, bio, posts)
 *   3. Fetch "suggested users" via chaining API
 *   4. Filter: followers >= 10k, not private, not already scraped
 *   5. Add to queue → repeat (BFS tree traversal)
 *
 * Anti-blocking measures:
 *   - Random delays between requests (5-12s for profiles, 3-6s for pages)
 *   - Longer cooldowns every N requests (60-90s every 20 requests)
 *   - Exponential backoff on 429/rate limit
 *   - Request jitter to avoid patterns
 *   - Session health checks
 *
 * Usage:
 *   node crawler.js                          # Start with default Indian seeds
 *   node crawler.js --seeds virat.kohli      # Custom seed(s), comma-separated
 *   node crawler.js --max-depth 3            # Max tree depth (default 3)
 *   node crawler.js --max-profiles 500       # Max profiles to scrape (default 500)
 *   node crawler.js --min-followers 10000    # Min follower threshold (default 10000)
 *   node crawler.js --resume                 # Resume from previous crawl state
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

// =============================================
// CONFIGURATION
// =============================================

const CONFIG = {
  // Updated cookies from user's session (March 24, 2026)
  cookies: [
    'csrftoken=4vK0pChSgDBgAPQN99F2ZZ',
    'datr=CzvCaXNUghuvAz6knPJGhn1P',
    'ig_did=F6424B2B-A43C-4127-A324-A6D0C7C028A1',
    'mid=acI7CwALAAHHOboXbKVGZa2BrbSP',
    'dpr=1.5',
    'ds_user_id=80121789867',
    'sessionid=80121789867%3AlGk4xoKyzbCVdC%3A29%3AAYjlUs-vfG4vTNTwTVIE4_ZKn_liz23h0gyhC8Tb_g',
    'rur="RVA\\05480121789867\\0541805872849:01fe011d0a202ceff7069ad82e702cc3d38c6b4ae09b326e8eed92afb3033bb529c87319"',
  ].join('; '),

  csrfToken: '4vK0pChSgDBgAPQN99F2ZZ',

  // GraphQL tokens — extract from DevTools Network tab on any graphql/query request
  fbDtsg: '',   // Fill from network tab if needed for posts
  lsd: '',      // Fill from network tab if needed for posts
  docId: '33944389991841132',
  jazoest: '',  // Fill from network tab if needed for posts

  // These are only needed for GraphQL post fetching (Phase 2 of each profile)
  // The chaining API and profile scrape don't need them
  av: '17841480096035223',
  __hsi: '',
  __rev: '',
  __s: '',
  __dyn: '',
  __csr: '',
  __spinR: '',
  __spinB: 'trunk',
  __spinT: '',

  // User agent matching the session
  userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',

  // Instagram app ID (constant)
  igAppId: '936619743392459',
};

// =============================================
// CRAWL SETTINGS
// =============================================

const DEFAULT_SEEDS = [
  'virat.kohli',
  'shraddhakapoor',
  'alaborig',
];

const SETTINGS = {
  maxDepth: 3,            // How deep to crawl the suggestion tree
  maxProfiles: 500,       // Max total profiles to scrape
  minFollowers: 5000,     // 5k minimum
  maxFollowers: 150000,   // 150k maximum (soft cap — keeps ~100K+ profiles that are growing)
  maxPostsPerProfile: 50, // Posts to fetch per profile
  requestsBeforeCooldown: 15,  // Cooldown after N requests
  cooldownMs: [60000, 90000],  // 60-90s cooldown range
  skipSuggestionsAfterQueue: 200, // Stop fetching suggestions once queue is big enough
  postsPerPage: 12,       // Posts per API call

  // Delay ranges (milliseconds)
  delays: {
    betweenProfiles: [8000, 15000],   // 8-15s between full profile scrapes
    betweenPages: [3000, 6000],       // 3-6s between post pagination
    betweenSuggestions: [5000, 10000], // 5-10s before fetching suggestions
    onRateLimit: [120000, 180000],    // 2-3min on 429
  },
};

// Fast mode — optimized for speed while staying safe
const FAST_SETTINGS = {
  maxPostsPerProfile: 12, // 1 page of posts (no pagination needed)
  requestsBeforeCooldown: 25,
  cooldownMs: [40000, 60000],
  skipSuggestionsAfterQueue: 150,

  delays: {
    betweenProfiles: [5000, 8000],
    betweenPages: [2000, 4000],
    betweenSuggestions: [3000, 6000],
    onRateLimit: [90000, 120000],
  },
};

// =============================================
// STATE MANAGEMENT
// =============================================

const STATE_FILE = path.join(__dirname, '..', 'backend', 'data', '_crawl_state.json');
const DATA_DIR = path.join(__dirname, '..', 'backend', 'data');

const state = {
  scraped: new Set(),       // usernames already fully scraped
  queued: [],               // BFS queue: [{ username, pk, depth, source }]
  failed: new Map(),        // username → { error, retries }
  discovered: new Set(),    // all usernames ever seen (scraped + queued + rejected)
  stats: {
    totalRequests: 0,
    profilesScraped: 0,
    profilesSkipped: 0,
    suggestionsFound: 0,
    startedAt: null,
    lastRequestAt: null,
  },
};

function saveState() {
  const serializable = {
    scraped: [...state.scraped],
    queued: state.queued,
    failed: Object.fromEntries(state.failed),
    discovered: [...state.discovered],
    stats: state.stats,
  };
  fs.writeFileSync(STATE_FILE, JSON.stringify(serializable, null, 2));
}

function loadState() {
  if (!fs.existsSync(STATE_FILE)) return false;
  try {
    const data = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    state.scraped = new Set(data.scraped || []);
    state.queued = data.queued || [];
    state.failed = new Map(Object.entries(data.failed || {}));
    state.discovered = new Set(data.discovered || []);
    state.stats = { ...state.stats, ...data.stats };
    return true;
  } catch (e) {
    console.log('  [!] Could not load state file, starting fresh.');
    return false;
  }
}

// =============================================
// HTTP HELPERS (with anti-blocking)
// =============================================

let requestCount = 0;

function randomDelay(range) {
  const [min, max] = range;
  const ms = min + Math.random() * (max - min);
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function log(msg) {
  const ts = new Date().toLocaleTimeString();
  console.log(`  [${ts}] ${msg}`);
}

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
        'sec-fetch-dest': 'document',
        'sec-ch-ua': '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        ...extraHeaders,
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        requestCount++;
        state.stats.totalRequests++;
        state.stats.lastRequestAt = new Date().toISOString();
        resolve({ status: res.statusCode, headers: res.headers, body: data });
      });
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
        requestCount++;
        state.stats.totalRequests++;
        state.stats.lastRequestAt = new Date().toISOString();

        if (res.statusCode === 429) {
          reject(new Error('RATE_LIMITED'));
          return;
        }
        if (res.statusCode === 401 || res.statusCode === 403) {
          reject(new Error(`AUTH_ERROR_${res.statusCode}`));
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP_${res.statusCode}: ${data.substring(0, 200)}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`JSON_PARSE_ERROR: ${data.substring(0, 200)}`));
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function cooldownCheck() {
  if (requestCount > 0 && requestCount % SETTINGS.requestsBeforeCooldown === 0) {
    const cooldownMs = SETTINGS.cooldownMs[0] + Math.random() * (SETTINGS.cooldownMs[1] - SETTINGS.cooldownMs[0]);
    log(`Cooldown: ${(cooldownMs / 1000).toFixed(0)}s after ${requestCount} requests...`);
    saveState();
    await new Promise(r => setTimeout(r, cooldownMs));
  }
}

async function withRetry(fn, label, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (err.message === 'RATE_LIMITED') {
        const waitMs = SETTINGS.delays.onRateLimit[0] + Math.random() * (SETTINGS.delays.onRateLimit[1] - SETTINGS.delays.onRateLimit[0]);
        log(`Rate limited on ${label}! Waiting ${(waitMs / 1000).toFixed(0)}s (attempt ${attempt}/${maxRetries})`);
        await new Promise(r => setTimeout(r, waitMs));
        continue;
      }
      if (err.message.startsWith('AUTH_ERROR')) {
        log(`Auth error on ${label}: ${err.message}. Session may have expired.`);
        throw err; // Don't retry auth errors
      }
      if (attempt === maxRetries) {
        log(`Failed ${label} after ${maxRetries} attempts: ${err.message}`);
        throw err;
      }
      const backoff = 10000 * attempt + Math.random() * 5000;
      log(`Error on ${label} (attempt ${attempt}): ${err.message}. Retrying in ${(backoff / 1000).toFixed(0)}s...`);
      await new Promise(r => setTimeout(r, backoff));
    }
  }
}

// =============================================
// INSTAGRAM API FUNCTIONS
// =============================================

/**
 * Primary: scrape profile from HTML page (reliable, same as original scraper)
 * Also extracts pk (user ID) from embedded JSON for chaining API
 */
async function scrapeProfile(username) {
  const response = await httpGet(`https://www.instagram.com/${username}/`);

  if (response.status === 404) return null;
  if (response.status === 429) throw new Error('RATE_LIMITED');
  if (response.status !== 200) throw new Error(`HTTP_${response.status}`);

  const html = response.body;
  const profile = { username };

  // Extract pk (user ID) from page — needed for chaining/suggestions API
  // Look for patterns like "profilePage_12345" or "\"id\":\"12345\""
  const pkMatch = html.match(/"profilePage_(\d+)"/) ||
                  html.match(/"user_id":"(\d+)"/) ||
                  html.match(/"id":"(\d+)".*?"username":"[^"]*"/);
  if (pkMatch) profile.pk = pkMatch[1];

  // Extract from meta tags (always available on profile pages)
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
    const bioMatch = desc.match(/Posts\s*[-–—]\s*(.*)/);
    if (bioMatch) profile.bio = decodeHtmlEntities(bioMatch[1]).trim();
  }

  if (ogImage) profile.profile_pic = ogImage[1].replace(/&amp;/g, '&');
  profile.is_verified = html.includes('is_verified":true') || html.includes('isVerified":true');
  profile.is_private = html.includes('is_private":true');

  return profile;
}

function decodeHtmlEntities(str) {
  return str
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)))
    .replace(/&#(\d+);/g, (_, dec) => String.fromCharCode(parseInt(dec, 10)))
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#x27;/g, "'");
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

/**
 * Fetch suggested/similar accounts for a user
 * Uses Instagram's chaining API via i.instagram.com (mobile API, avoids UA mismatch)
 */
async function getSuggestedUsers(userId) {
  const url = `https://i.instagram.com/api/v1/discover/chaining/?target_id=${userId}`;
  // The chaining endpoint requires mobile-compatible headers via i.instagram.com
  const json = await httpGetJson(url, {
    'user-agent': 'Instagram 317.0.0.34.109 Android (30/11; 420dpi; 1080x2220; Google; Pixel 5; redfin; redfin; en_US; 562804463)',
  });
  return json?.users || [];
}

/**
 * Fetch recent posts for a user via the web API
 * Uses /api/v1/feed/user/{username}/username/ endpoint (simpler than GraphQL)
 */
async function fetchUserPosts(username, userId, maxPosts = 50) {
  const allPosts = [];
  let maxId = null;

  while (allPosts.length < maxPosts) {
    let url = `https://www.instagram.com/api/v1/feed/user/${username}/username/?count=12`;
    if (maxId) url += `&max_id=${maxId}`;

    const json = await httpGetJson(url);
    const items = json?.items || [];
    if (items.length === 0) break;

    allPosts.push(...items);
    log(`  Posts: ${allPosts.length}/${maxPosts}`);

    if (!json.more_available) break;
    maxId = json.next_max_id;

    if (allPosts.length < maxPosts) {
      await randomDelay(SETTINGS.delays.betweenPages);
      await cooldownCheck();
    }
  }

  return allPosts.slice(0, maxPosts);
}

/**
 * Parse a raw post node into our standard format
 */
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
  const engagementRate = followers > 0
    ? parseFloat(((likeCount + commentCount) / followers * 100).toFixed(4))
    : 0;

  return {
    media_id: node.pk?.toString() || node.code,
    code: node.code,
    url: `https://www.instagram.com/p/${node.code}/`,
    caption: captionText,
    timestamp: node.taken_at,
    date: new Date(node.taken_at * 1000).toLocaleDateString('en-US', {
      day: 'numeric', month: 'short', year: '2-digit',
    }),
    likes: likeCount,
    comments: commentCount,
    views: node.view_count || node.play_count || 0,
    fb_likes: node.fb_like_count || 0,
    post_type: node.product_type === 'clips' ? 'Reel' : 'Post',
    media_type: node.media_type,
    product_type: node.product_type,
    is_paid_partnership: node.is_paid_partnership || false,
    has_mentions: allMentions.length > 0,
    mentions: allMentions,
    hashtags,
    sponsor_tags: sponsorTags,
    coauthors,
    carousel_count: node.carousel_media_count || null,
    image_url: node.image_versions2?.candidates?.[0]?.url || null,
    engagement_rate: engagementRate,
  };
}

/**
 * Compute engagement metrics from parsed posts
 */
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

  const partnershipPosts = posts.filter(p => p.is_paid_partnership);
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
    avg_engagement: Math.round((totalLikes + totalComments) / count),
    engagement_rate: followers > 0
      ? parseFloat(((totalLikes + totalComments) / count / followers * 100).toFixed(2))
      : 0,
    posts_per_week: postsPerWeek,
    partnership_posts: partnershipPosts.length,
    partnership_percentage: parseFloat((partnershipPosts.length / count * 100).toFixed(1)),
    posts_with_mentions: posts.filter(p => p.has_mentions).length,
    post_types: postTypes,
  };
}

// =============================================
// FULL PROFILE SCRAPE (profile + posts + save)
// =============================================

async function scrapeFullProfile(username, depth, source) {
  log(`Scraping @${username} (depth: ${depth}, source: @${source || 'seed'})...`);

  // Step 1: Get profile info via HTML scrape (reliable, avoids API rate limits)
  let profile = await withRetry(
    () => scrapeProfile(username),
    `profile:${username}`
  );

  if (!profile) {
    log(`  Could not fetch profile for @${username}. Skipping.`);
    return null;
  }

  // Check follower range
  if (profile.followers < SETTINGS.minFollowers) {
    log(`  @${username} has ${profile.followers.toLocaleString()} followers (< ${SETTINGS.minFollowers.toLocaleString()}). Skipping.`);
    state.stats.profilesSkipped++;
    return null;
  }
  if (SETTINGS.maxFollowers && profile.followers > SETTINGS.maxFollowers) {
    log(`  @${username} has ${profile.followers.toLocaleString()} followers (> ${SETTINGS.maxFollowers.toLocaleString()}). Skipping.`);
    state.stats.profilesSkipped++;
    return null;
  }

  // Skip private accounts
  if (profile.is_private) {
    log(`  @${username} is private. Skipping.`);
    state.stats.profilesSkipped++;
    return null;
  }

  log(`  @${username}: ${profile.followers.toLocaleString()} followers, ${profile.posts_count} posts`);

  // Step 2: Fetch posts
  await randomDelay(SETTINGS.delays.betweenPages);
  await cooldownCheck();

  let rawPosts = [];
  try {
    rawPosts = await withRetry(
      () => fetchUserPosts(username, profile.pk, SETTINGS.maxPostsPerProfile),
      `posts:${username}`
    );
  } catch (err) {
    log(`  Could not fetch posts for @${username}: ${err.message}`);
  }

  const parsedPosts = rawPosts.map(node => parsePost(node, profile.followers));
  const metrics = computeMetrics(parsedPosts, profile.followers);

  // Build result (same format as original scraper)
  const result = {
    success: true,
    scraped_at: new Date().toISOString(),
    crawl_info: { depth, source, seed: depth === 0 },
    data: {
      username: profile.username || username,
      full_name: profile.full_name || username,
      profile_pic: profile.profile_pic || '',
      is_verified: profile.is_verified || false,
      pk: profile.pk || '',
      followers: profile.followers,
      following: profile.following || 0,
      posts_count: profile.posts_count || 0,
      bio: profile.bio || '',
      category: profile.category || '',
      external_url: profile.external_url || '',
      engagement_metrics: metrics,
      post_types: metrics?.post_types || {},
      recent_posts: parsedPosts,
    },
  };

  // Append to single all_profiles.json
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  const allFile = path.join(DATA_DIR, '..', 'all_profiles.json');
  let existing = { total_profiles: 0, generated_at: '', profiles: [] };
  if (fs.existsSync(allFile)) {
    try { existing = JSON.parse(fs.readFileSync(allFile, 'utf8')); } catch (e) {}
  }
  const idx = existing.profiles.findIndex(p => p.data.username === result.data.username);
  if (idx >= 0) {
    existing.profiles[idx] = result;
  } else {
    existing.profiles.push(result);
  }
  existing.total_profiles = existing.profiles.length;
  existing.generated_at = new Date().toISOString();
  existing.profiles.sort((a, b) => b.data.followers - a.data.followers);
  fs.writeFileSync(allFile, JSON.stringify(existing, null, 2));

  log(`  Saved @${username} → ${parsedPosts.length} posts, ${metrics?.engagement_rate || 0}% engagement (total: ${existing.total_profiles})`);

  state.scraped.add(username);
  state.stats.profilesScraped++;

  return { profile, pk: profile.pk };
}

// =============================================
// SUGGESTION CRAWLER (BFS tree traversal)
// =============================================

async function discoverSuggestions(username, userId, depth) {
  if (depth >= SETTINGS.maxDepth) return;

  // Skip fetching more suggestions if queue is already big enough
  if (SETTINGS.skipSuggestionsAfterQueue && state.queued.length >= SETTINGS.skipSuggestionsAfterQueue) {
    log(`  Queue already has ${state.queued.length} profiles, skipping suggestions.`);
    return;
  }

  log(`Fetching suggestions for @${username}...`);
  await randomDelay(SETTINGS.delays.betweenSuggestions);
  await cooldownCheck();

  let suggested = [];
  try {
    suggested = await withRetry(
      () => getSuggestedUsers(userId),
      `suggestions:${username}`
    );
  } catch (err) {
    log(`  Could not fetch suggestions for @${username}: ${err.message}`);
    return;
  }

  let newCount = 0;
  for (const user of suggested) {
    const uname = user.username;
    if (state.discovered.has(uname)) continue;

    state.discovered.add(uname);

    // Quick filter: skip private accounts
    if (user.is_private) continue;

    // Add to queue for BFS processing
    state.queued.push({
      username: uname,
      pk: user.pk?.toString(),
      depth: depth + 1,
      source: username,
    });
    newCount++;
  }

  state.stats.suggestionsFound += newCount;
  log(`  Found ${suggested.length} suggestions, ${newCount} new → queue: ${state.queued.length}`);
}

// =============================================
// MAIN CRAWLER LOOP
// =============================================

async function runCrawler(options = {}) {
  const {
    seeds = DEFAULT_SEEDS,
    maxDepth = SETTINGS.maxDepth,
    maxProfiles = SETTINGS.maxProfiles,
    minFollowers = SETTINGS.minFollowers,
    maxFollowers = SETTINGS.maxFollowers,
    resume = false,
    fast = false,
  } = options;

  SETTINGS.maxDepth = maxDepth;
  SETTINGS.maxProfiles = maxProfiles;
  SETTINGS.minFollowers = minFollowers;
  SETTINGS.maxFollowers = maxFollowers;

  // Apply fast mode settings
  if (fast) {
    SETTINGS.maxPostsPerProfile = FAST_SETTINGS.maxPostsPerProfile;
    SETTINGS.requestsBeforeCooldown = FAST_SETTINGS.requestsBeforeCooldown;
    SETTINGS.cooldownMs = FAST_SETTINGS.cooldownMs;
    SETTINGS.skipSuggestionsAfterQueue = FAST_SETTINGS.skipSuggestionsAfterQueue;
    SETTINGS.delays = FAST_SETTINGS.delays;
  }

  console.log('\n' + '='.repeat(70));
  console.log('  INSTAGRAM INFLUENCER CRAWLER');
  console.log('='.repeat(70));
  console.log(`  Seeds:          ${seeds.join(', ')}`);
  console.log(`  Max depth:      ${maxDepth}`);
  console.log(`  Max profiles:   ${maxProfiles}`);
  console.log(`  Follower range: ${minFollowers.toLocaleString()} - ${maxFollowers.toLocaleString()}`);
  console.log(`  Fast mode:      ${fast}`);
  console.log(`  Resume:         ${resume}`);
  console.log('='.repeat(70) + '\n');

  // Resume or initialize
  if (resume && loadState()) {
    log(`Resumed crawl: ${state.scraped.size} scraped, ${state.queued.length} in queue`);
  } else {
    state.stats.startedAt = new Date().toISOString();
    // Seed the queue
    for (const seed of seeds) {
      if (!state.discovered.has(seed)) {
        state.discovered.add(seed);
        state.queued.push({ username: seed, pk: null, depth: 0, source: null });
      }
    }
  }

  // Auto-detect already scraped profiles from all_profiles.json (avoid re-scraping)
  const allFile = path.join(DATA_DIR, '..', 'all_profiles.json');
  if (fs.existsSync(allFile)) {
    try {
      const existing = JSON.parse(fs.readFileSync(allFile, 'utf8'));
      let recovered = 0;
      for (const p of existing.profiles) {
        const username = p.data.username;
        if (!state.scraped.has(username)) {
          state.scraped.add(username);
          state.discovered.add(username);
          recovered++;
        }
      }
      if (recovered > 0) {
        log(`Auto-detected ${recovered} already-scraped profiles from all_profiles.json`);
      }
    } catch (e) {}
  }

  // BFS crawl loop
  while (state.queued.length > 0 && state.stats.profilesScraped < maxProfiles) {
    const item = state.queued.shift();
    const { username, depth, source } = item;

    // Skip if already scraped
    if (state.scraped.has(username)) continue;

    // Progress report
    const progress = `[${state.stats.profilesScraped}/${maxProfiles}]`;
    const queueSize = `(queue: ${state.queued.length})`;
    log(`\n${progress} ${queueSize} Processing @${username} (depth ${depth})`);

    try {
      // Scrape the full profile
      const result = await scrapeFullProfile(username, depth, source);

      if (result) {
        // Discover suggestions from this profile (grows the tree)
        await discoverSuggestions(username, result.pk, depth);
      }

      // Save state periodically
      if (state.stats.profilesScraped % 5 === 0) {
        saveState();
      }

    } catch (err) {
      if (err.message.startsWith('AUTH_ERROR')) {
        log('\nSession expired! Please update cookies in CONFIG and restart with --resume');
        saveState();
        process.exit(1);
      }

      const retries = (state.failed.get(username)?.retries || 0) + 1;
      state.failed.set(username, { error: err.message, retries });

      if (retries < 3) {
        // Re-queue with same depth
        state.queued.push({ username, pk: item.pk, depth, source });
        log(`  Re-queued @${username} (retry ${retries}/3)`);
      } else {
        log(`  Giving up on @${username} after 3 retries`);
      }
    }

    // Delay between profiles
    if (state.queued.length > 0 && state.stats.profilesScraped < maxProfiles) {
      await randomDelay(SETTINGS.delays.betweenProfiles);
    }
  }

  // Final state save
  saveState();

  // Print summary
  console.log('\n' + '='.repeat(70));
  console.log('  CRAWL COMPLETE');
  console.log('='.repeat(70));
  console.log(`  Profiles scraped:  ${state.stats.profilesScraped}`);
  console.log(`  Profiles skipped:  ${state.stats.profilesSkipped}`);
  console.log(`  Suggestions found: ${state.stats.suggestionsFound}`);
  console.log(`  Total discovered:  ${state.discovered.size}`);
  console.log(`  Queue remaining:   ${state.queued.length}`);
  console.log(`  Failed:            ${state.failed.size}`);
  console.log(`  Total requests:    ${state.stats.totalRequests}`);
  console.log(`  Started:           ${state.stats.startedAt}`);
  console.log(`  Finished:          ${new Date().toISOString()}`);
  console.log(`  Output:            ${path.join(DATA_DIR, '..', 'all_profiles.json')}`);
  console.log('='.repeat(70) + '\n');
}

// =============================================
// CLI ENTRY POINT
// =============================================

function parseArgs() {
  const args = process.argv.slice(2);
  const options = {};

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--seeds':
        options.seeds = args[++i]?.split(',').map(s => s.trim());
        break;
      case '--max-depth':
        options.maxDepth = parseInt(args[++i]) || 3;
        break;
      case '--max-profiles':
        options.maxProfiles = parseInt(args[++i]) || 500;
        break;
      case '--min-followers':
        options.minFollowers = parseInt(args[++i]) || 5000;
        break;
      case '--max-followers':
        options.maxFollowers = parseInt(args[++i]) || 100000;
        break;
      case '--seeds-file':
        try {
          const seedData = JSON.parse(fs.readFileSync(args[++i], 'utf8'));
          options.seeds = seedData.usernames || seedData;
        } catch (e) {
          console.error('Could not read seeds file:', e.message);
        }
        break;
      case '--resume':
        options.resume = true;
        break;
      case '--fast':
        options.fast = true;
        break;
      default:
        // If first arg doesn't start with --, treat as seed username
        if (!args[i].startsWith('--') && !options.seeds) {
          options.seeds = [args[i]];
        }
    }
  }

  return options;
}

if (require.main === module) {
  const options = parseArgs();
  runCrawler(options)
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('\nCrawler failed:', err.message);
      saveState();
      process.exit(1);
    });
}

module.exports = { runCrawler, scrapeFullProfile, getSuggestedUsers };
