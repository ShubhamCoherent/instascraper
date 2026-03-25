/**
 * Instagram Scraper - Hybrid approach
 *
 * Method 1: Basic HTTP scrape of profile page for followers, bio, profile pic
 * Method 2: Direct GraphQL API calls for paginated post data (exact counts)
 *
 * Both methods use the same session cookies — no Puppeteer needed.
 *
 * Usage:
 *   node scrape.js                     # scrape cristiano (default)
 *   node scrape.js <username>          # scrape specific user
 *   node scrape.js <username> <count>  # scrape specific count of posts
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const querystring = require('querystring');

// ---------- CONFIGURATION ----------
// These values come from your logged-in Instagram browser session.
// To update: Open instagram.com > DevTools > Network tab > find any graphql/query request

const CONFIG = {
  cookies: [
    'csrftoken=liz2bc9mRTQjEbawWt1LEo',
    'datr=x--Cad6Sl8O1lzIH9M-LndfA',
    'ig_did=441A0341-98F2-4E93-A427-B459D8B9F0BD',
    'dpr=1.5',
    'ig_nrcb=1',
    'mid=aYLvxwALAAHxNzDkIbviYaUW-GsN',
    'ps_l=1',
    'ps_n=1',
    'ds_user_id=80121789867',
    'sessionid=80121789867%3AmEGZmUIVXK3Ctx%3A23%3AAYhUCq-im3vBLs_KRqnEUxzHnIeI8WpBZ-NSCKw-ag',
    'rur="RVA\\05480121789867\\0541801731247:01fef55ce6fd60b326d05dc642994d027d026b35d17304aa8deea15ab0540bc9a1d2007d"',
  ].join('; '),
  csrfToken: 'liz2bc9mRTQjEbawWt1LEo',
  fbDtsg: 'NAft4wI3DwOUXKbGnAWXo6Ix8upgGO_nJ86qaXW5KMgvFPo4KJeVccg:17854477105113577:1770195166',
  lsd: 'qEpaK_Ja1-vdaBZUPwKn8S',
  docId: '33944389991841132',
  jazoest: '26263',
  av: '17841480096035223',
  __hsi: '7602930381785067701',
  __rev: '1032899680',
  __s: 'jlenky:qiyjp6:jo4ttu',
  __dyn: '7xeUjG1mxu1syaxG4Vp41twpUnwgU7SbzEdF8aUco2qwJyEiw9-1DwUx609vCwjE1EEc87m0yE462mcw5Mx62G5UswoEcE7O2l0Fwqo31w9O1lwxwQzXw8W58jwGzEaE2iwNwmE7G4-5o4q3y261kx-0ma2-azqwt8d-2u2J0bS1LyUaUbGwmk0zU8oC1Iwqo5p0OwUQp6x6Ub9UKUnAwCAK6E5y4UrwlE2xyVrx60hK798pyFEaU4y16wAwj8',
  __csr: 'gN3QY6Ytdnbd95ONa24BsDlnlunqZkyjkasR9pbLBWyaFeGHEyA9WVqWSuENCAhQypvjBjAKALJKKiGiUyaKVkmVox6ypGGEFdV9JVO16CAGAVrAzXVDFqjyCirjBWFdWAzAmBAHBy-QaKGmQbxeh7mqeUWiCnyBVrHCGOd2FpVfCUiKh6njxeVa8XDJKFoCqqayGzUyaK9jiQdzA5UF4zQq7E1b85G00mFaiB87obXzEGagkwhonc9wKwf22a2S0DS10wba69U2dG04oofp2N2xh1v8m6EdEbFEMEV2GK5o4yPetyUy78gwfq6Ucog5Zu0KCfw4ow4Iw4ig1WrwdvB85dFw3i9Eig982UyE2Ux3ovwsBfrgnGpw9ipU6e5XxmEh84oaAdxZ7x60Am4E-sE2dCg1ho5e8gKgE5C0vuac3xCwRVEB2VK1ew2W228sAb41C2y4rxB0mEm5jw4QwtEOfxqloK2q1gBUG7-eg1cU0gJw19-1ImmngObwmo7m07qo0uXw1zjRzCnwZw67wJyp45o4OWtwba0l60Yqw1ix09-0P83mgdu0kN_gWbwm4223G0-BG0Jo',
  __spinR: '1032899680',
  __spinB: 'trunk',
  __spinT: '1770195174',
};

// ---------- HTTP HELPERS ----------

function httpGet(url) {
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
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'sec-fetch-mode': 'navigate',
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body: data }));
    });
    req.on('error', reject);
    req.end();
  });
}

function httpPost(body) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'www.instagram.com',
      port: 443,
      path: '/graphql/query',
      method: 'POST',
      headers: {
        'accept': '*/*',
        'accept-encoding': 'identity',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'content-length': Buffer.byteLength(body),
        'cookie': CONFIG.cookies,
        'origin': 'https://www.instagram.com',
        'referer': 'https://www.instagram.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'x-csrftoken': CONFIG.csrfToken,
        'x-fb-friendly-name': 'PolarisProfilePostsTabContentQuery_connection',
        'x-fb-lsd': CONFIG.lsd,
        'x-ig-app-id': '936619743392459',
      },
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        if (res.statusCode === 429) {
          reject(new Error('Rate limited (429). Wait a few minutes and retry.'));
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}: ${data.substring(0, 200)}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error(`JSON parse error: ${data.substring(0, 200)}`));
        }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// =============================================
// METHOD 1: Profile Scraping (basic HTTP)
// =============================================

async function scrapeProfile(username) {
  console.log('\n  [Method 1] Scraping profile page for @' + username + '...');

  const response = await httpGet(`https://www.instagram.com/${username}/`);

  if (response.status !== 200) {
    console.log(`  Profile page returned ${response.status}`);
    return null;
  }

  const html = response.body;
  const profile = {};

  // Extract from meta tags (always available on profile pages)
  const ogTitle = html.match(/<meta\s+property="og:title"\s+content="([^"]+)"/);
  const ogDesc = html.match(/<meta\s+property="og:description"\s+content="([^"]+)"/);
  const ogImage = html.match(/<meta\s+property="og:image"\s+content="([^"]+)"/);

  // Parse og:title → "Cristiano Ronaldo (@cristiano) • Instagram photos and videos"
  if (ogTitle) {
    const nameMatch = ogTitle[1].match(/^(.+?)\s*\(@/);
    profile.full_name = nameMatch ? nameMatch[1].trim() : ogTitle[1];
  }

  // Parse og:description → "670M Followers, 584 Following, 3,621 Posts - ..."
  if (ogDesc) {
    const desc = ogDesc[1].replace(/&amp;/g, '&').replace(/&#x27;/g, "'").replace(/&quot;/g, '"');

    const followersMatch = desc.match(/([\d,.]+[MKB]?)\s*Followers/i);
    const followingMatch = desc.match(/([\d,.]+[MKB]?)\s*Following/i);
    const postsMatch = desc.match(/([\d,.]+[MKB]?)\s*Posts/i);

    profile.followers = parseMetaCount(followersMatch?.[1]);
    profile.following = parseMetaCount(followingMatch?.[1]);
    profile.posts_count = parseMetaCount(postsMatch?.[1]);

    // Bio is after the dash
    const bioMatch = desc.match(/Posts\s*[-–—]\s*(.*)/);
    if (bioMatch) {
      profile.bio = bioMatch[1].replace(/&[^;]+;/g, ' ').trim();
    }
  }

  // Profile picture from og:image
  if (ogImage) {
    profile.profile_pic_meta = ogImage[1].replace(/&amp;/g, '&');
  }

  // Check for verified badge in page
  profile.is_verified = html.includes('is_verified":true') || html.includes('isVerified":true');

  console.log(`  Followers: ${profile.followers?.toLocaleString() || 'N/A'}`);
  console.log(`  Following: ${profile.following?.toLocaleString() || 'N/A'}`);
  console.log(`  Posts: ${profile.posts_count?.toLocaleString() || 'N/A'}`);
  if (profile.bio) console.log(`  Bio: ${profile.bio.substring(0, 80)}...`);

  return profile;
}

function parseMetaCount(str) {
  if (!str) return 0;
  str = str.replace(/,/g, '');
  const num = parseFloat(str);
  if (str.includes('M')) return Math.round(num * 1000000);
  if (str.includes('K')) return Math.round(num * 1000);
  if (str.includes('B')) return Math.round(num * 1000000000);
  return parseInt(str) || 0;
}

// =============================================
// METHOD 2: GraphQL API (post data)
// =============================================

function buildPostsPayload(username, cursor = null) {
  return querystring.stringify({
    av: CONFIG.av,
    __d: 'www',
    __user: '0',
    __a: '1',
    __req: '1',
    __hs: '20488.HYP:instagram_web_pkg.2.1...0',
    dpr: '1',
    __ccg: 'EXCELLENT',
    __rev: CONFIG.__rev,
    __s: CONFIG.__s,
    __hsi: CONFIG.__hsi,
    __dyn: CONFIG.__dyn,
    __csr: CONFIG.__csr,
    __comet_req: '7',
    fb_dtsg: CONFIG.fbDtsg,
    jazoest: CONFIG.jazoest,
    lsd: CONFIG.lsd,
    __spin_r: CONFIG.__spinR,
    __spin_b: CONFIG.__spinB,
    __spin_t: CONFIG.__spinT,
    fb_api_caller_class: 'RelayModern',
    fb_api_req_friendly_name: 'PolarisProfilePostsTabContentQuery_connection',
    server_timestamps: 'true',
    doc_id: CONFIG.docId,
    variables: JSON.stringify({
      after: cursor,
      before: null,
      data: {
        count: 12,
        include_reel_media_seen_timestamp: true,
        include_relationship_info: true,
        latest_besties_reel_media: true,
        latest_reel_media: true,
      },
      first: 12,
      last: null,
      username: username,
    }),
  });
}

function parsePostNode(node, followers) {
  const postType = node.product_type === 'clips' ? 'Reel' : 'Post';

  // Mentions: from usertags + caption @mentions
  const userTags = node.usertags?.in?.map(t => t.user?.username).filter(Boolean) || [];
  const captionText = node.caption?.text || '';
  const captionMentions = (captionText.match(/@[\w.]+/g) || []).map(m => m.slice(1));
  const allMentions = [...new Set([...userTags, ...captionMentions])];

  // Hashtags from caption
  const hashtags = (captionText.match(/#[\w]+/g) || []).map(h => h.slice(1));

  // Sponsor/brand info from Instagram's API fields
  const sponsorTags = (node.sponsor_tags || []).map(s => s.username || s.name).filter(Boolean);
  const coauthors = (node.coauthor_producers || []).map(c => c.username).filter(Boolean);

  const likeCount = node.like_count || 0;
  const commentCount = node.comment_count || 0;
  const engagementRate = followers > 0
    ? parseFloat(((likeCount + commentCount) / followers * 100).toFixed(4))
    : 0;

  return {
    media_id: node.code,
    code: node.code,
    url: `https://www.instagram.com/p/${node.code}/`,
    caption: captionText,
    timestamp: node.taken_at,
    date: new Date(node.taken_at * 1000).toLocaleDateString('en-US', {
      day: 'numeric', month: 'short', year: '2-digit',
    }),
    likes: likeCount,
    comments: commentCount,
    views: node.view_count || 0,
    fb_likes: node.fb_like_count || 0,
    post_type: postType,
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

async function scrapePosts(username, maxPosts = 50) {
  console.log(`\n  [Method 2] Fetching ${maxPosts} posts via GraphQL API...`);

  const allRawPosts = [];
  let cursor = null;
  let profileFromAPI = null;
  let pageNum = 0;

  while (allRawPosts.length < maxPosts) {
    pageNum++;
    console.log(`  Page ${pageNum}: fetching batch (have ${allRawPosts.length})...`);

    const body = buildPostsPayload(username, cursor);
    const json = await httpPost(body);

    if (json.errors) {
      console.error('  API Error:', JSON.stringify(json.errors));
      break;
    }

    const connection = json?.data?.xdt_api__v1__feed__user_timeline_graphql_connection;
    if (!connection) {
      console.error('  No data in response.');
      break;
    }

    const edges = connection.edges || [];
    if (edges.length === 0) {
      console.log('  No more posts.');
      break;
    }

    // Grab profile info from first post's user object (once)
    if (!profileFromAPI && edges[0]?.node?.user) {
      const u = edges[0].node.user;
      profileFromAPI = {
        username: u.username,
        full_name: u.full_name,
        profile_pic: u.hd_profile_pic_url_info?.url || u.profile_pic_url,
        is_verified: u.is_verified,
        pk: u.pk,
      };
    }

    for (const edge of edges) {
      if (allRawPosts.length >= maxPosts) break;
      allRawPosts.push(edge.node);
    }

    console.log(`  Got ${edges.length} posts (total: ${allRawPosts.length})`);

    if (!connection.page_info?.has_next_page) {
      console.log('  Reached last page.');
      break;
    }

    cursor = connection.page_info.end_cursor;

    const delayMs = 3000 + Math.random() * 2000;
    console.log(`  Waiting ${(delayMs / 1000).toFixed(1)}s...`);
    await delay(delayMs);
  }

  return { rawPosts: allRawPosts, profileFromAPI };
}

// =============================================
// MAIN: Combine both methods
// =============================================

async function scrapeInstagram(username, maxPosts = 50) {
  console.log('\n' + '='.repeat(70));
  console.log(`  INSTAGRAM SCRAPER - @${username.toUpperCase()}`);
  console.log('='.repeat(70));

  // Step 1: Scrape profile page (followers, bio, etc.)
  const profileData = await scrapeProfile(username);

  // Step 2: Fetch posts via GraphQL API
  const { rawPosts, profileFromAPI } = await scrapePosts(username, maxPosts);

  // Merge profile data from both methods
  const followers = profileData?.followers || 0;
  const following = profileData?.following || 0;
  const postsCount = profileData?.posts_count || 0;

  // Parse posts with follower count for engagement rate
  const parsedPosts = rawPosts.map(node => parsePostNode(node, followers));

  // ---------- Compute metrics ----------
  const totalLikes = parsedPosts.reduce((sum, p) => sum + p.likes, 0);
  const totalComments = parsedPosts.reduce((sum, p) => sum + p.comments, 0);
  const totalViews = parsedPosts.reduce((sum, p) => sum + p.views, 0);
  const count = parsedPosts.length;

  // Posts per week
  let postsPerWeek = 0;
  if (count >= 2) {
    const sorted = [...parsedPosts].sort((a, b) => a.timestamp - b.timestamp);
    const weeks = (sorted[count - 1].timestamp - sorted[0].timestamp) / (7 * 24 * 60 * 60);
    postsPerWeek = weeks > 0 ? parseFloat((count / weeks).toFixed(1)) : count;
  }

  // Partnership stats
  const partnershipPosts = parsedPosts.filter(p => p.is_paid_partnership);
  const postsWithMentions = parsedPosts.filter(p => p.has_mentions);

  // Post type breakdown
  const postTypes = {};
  parsedPosts.forEach(p => {
    postTypes[p.post_type] = (postTypes[p.post_type] || 0) + 1;
  });

  const metrics = {
    posts_analyzed: count,
    total_likes: totalLikes,
    total_comments: totalComments,
    total_views: totalViews,
    avg_likes: count > 0 ? Math.round(totalLikes / count) : 0,
    avg_comments: count > 0 ? Math.round(totalComments / count) : 0,
    avg_views: count > 0 ? Math.round(totalViews / count) : 0,
    avg_engagement: count > 0 ? Math.round((totalLikes + totalComments) / count) : 0,
    engagement_rate: followers > 0
      ? parseFloat(((totalLikes + totalComments) / count / followers * 100).toFixed(2))
      : 0,
    posts_per_week: postsPerWeek,
    partnership_posts: partnershipPosts.length,
    partnership_percentage: count > 0
      ? parseFloat((partnershipPosts.length / count * 100).toFixed(1))
      : 0,
    posts_with_mentions: postsWithMentions.length,
  };

  // Build final result
  const result = {
    success: true,
    scraped_at: new Date().toISOString(),
    methods: ['profile_scrape', 'graphql_api'],
    data: {
      username: profileFromAPI?.username || username,
      full_name: profileFromAPI?.full_name || profileData?.full_name || username,
      profile_pic: profileFromAPI?.profile_pic || profileData?.profile_pic_meta || '',
      is_verified: profileFromAPI?.is_verified || profileData?.is_verified || false,
      pk: profileFromAPI?.pk || '',
      followers: followers,
      following: following,
      posts_count: postsCount,
      bio: profileData?.bio || '',
      engagement_metrics: metrics,
      post_types: postTypes,
      recent_posts: parsedPosts,
    },
  };

  // ---------- Print summary ----------
  console.log('\n' + '='.repeat(70));
  console.log('  RESULTS');
  console.log('='.repeat(70));
  console.log(`  Profile:        @${result.data.username} (${result.data.full_name})`);
  console.log(`  Verified:       ${result.data.is_verified}`);
  console.log(`  Followers:      ${followers.toLocaleString()}`);
  console.log(`  Following:      ${following.toLocaleString()}`);
  console.log(`  Total Posts:    ${postsCount.toLocaleString()}`);
  if (result.data.bio) console.log(`  Bio:            ${result.data.bio.substring(0, 60)}...`);
  console.log('');
  console.log(`  Posts analyzed:  ${count}`);
  console.log(`  Post types:     ${Object.entries(postTypes).map(([k, v]) => `${v} ${k}s`).join(', ')}`);
  console.log(`  Posts/week:     ${postsPerWeek}`);
  console.log(`  Avg likes:      ${metrics.avg_likes.toLocaleString()}`);
  console.log(`  Avg comments:   ${metrics.avg_comments.toLocaleString()}`);
  console.log(`  Engagement:     ${metrics.engagement_rate}%`);
  console.log(`  Partnerships:   ${partnershipPosts.length} of ${count} (${metrics.partnership_percentage}%)`);
  console.log(`  With mentions:  ${postsWithMentions.length} of ${count}`);

  console.log('\n  Top 5 posts by likes:');
  [...parsedPosts].sort((a, b) => b.likes - a.likes).slice(0, 5).forEach((p, i) => {
    console.log(`    ${i + 1}. ${p.code} [${p.post_type}] ${p.likes.toLocaleString()} likes | ${p.comments.toLocaleString()} comments${p.is_paid_partnership ? ' | SPONSORED' : ''}`);
  });

  // Save to backend/data/
  const dataDir = path.join(__dirname, '..', 'backend', 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  const outputFile = path.join(dataDir, `${username}_complete.json`);
  fs.writeFileSync(outputFile, JSON.stringify(result, null, 2));
  console.log(`\n  Saved to: ${outputFile}`);
  console.log('='.repeat(70) + '\n');

  return result;
}

// ---------- RUN ----------

if (require.main === module) {
  const username = process.argv[2] || 'cristiano';
  const maxPosts = parseInt(process.argv[3]) || 50;

  scrapeInstagram(username, maxPosts)
    .then(() => process.exit(0))
    .catch((err) => {
      console.error('\nFailed:', err.message);
      process.exit(1);
    });
}

module.exports = { scrapeInstagram };
