/**
 * Suggestion-Only Worker
 *
 * Reads from a copy queue (suggestion:todo), fetches following lists
 * for each profile, and adds new usernames to the main scrape queue.
 *
 * Never touches the main scrape queue's existing jobs.
 * Only ADDS new profiles to it.
 *
 * Usage:
 *   node worker.js copy       # Copy main queue's scraped profiles to suggestion:todo
 *   node worker.js start      # Start fetching following lists
 *   node worker.js status     # Show stats
 */

const { Queue } = require('bullmq');
const Redis = require('ioredis');
const https = require('https');
const path = require('path');

require('dotenv').config({ path: path.join(__dirname, '.env') });

// =============================================
// CONFIG
// =============================================

const CONFIG = {
  cookies: process.env.IG_COOKIES,
  csrfToken: process.env.IG_CSRF_TOKEN,
  appId: process.env.IG_APP_ID || '936619743392459',
  userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
};

const DELAYS = {
  betweenProfiles: [3000, 6000],
  cooldownEvery: 30,
  cooldownTime: [30000, 50000],
};

const redisConnection = {
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: null,
};

const redis = new Redis(redisConnection);
const scrapeQueue = new Queue('scrape', { connection: redisConnection });

// Redis keys
const DISCOVERED_KEY = 'ig:discovered';
const SCRAPED_KEY = 'ig:scraped';
const SUGGESTION_TODO_KEY = 'ig:suggestion_todo';   // SET of profiles to fetch following for
const SUGGESTION_DONE_KEY = 'ig:suggestion_done';   // SET of profiles already processed
const SUGGESTION_STATS_KEY = 'ig:suggestion_stats'; // HASH of stats

// =============================================
// HELPERS
// =============================================

function log(msg) {
  const time = new Date().toLocaleTimeString();
  console.log(`  [${time}] ${msg}`);
}

function randomDelay(range) {
  const [min, max] = range;
  const ms = Math.floor(Math.random() * (max - min + 1)) + min;
  return new Promise(r => setTimeout(r, ms));
}

function httpGetJson(url, extraHeaders = {}) {
  return new Promise((resolve, reject) => {
    const headers = {
      'user-agent': CONFIG.userAgent,
      'cookie': CONFIG.cookies,
      'x-ig-app-id': CONFIG.appId,
      'x-csrftoken': CONFIG.csrfToken,
      'accept': '*/*',
      'accept-encoding': 'identity',
      'origin': 'https://www.instagram.com',
      'referer': 'https://www.instagram.com/',
      ...extraHeaders,
    };

    const req = https.get(url, { headers }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP_${res.statusCode}`));
          return;
        }
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(new Error('INVALID_JSON'));
        }
      });
    });
    req.on('error', reject);
  });
}

// Fetch profile HTML to get pk (user ID)
function fetchProfilePk(username) {
  return new Promise((resolve, reject) => {
    const req = https.get(`https://www.instagram.com/${username}/`, {
      headers: {
        'user-agent': CONFIG.userAgent,
        'cookie': CONFIG.cookies,
      },
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP_${res.statusCode}`));
          return;
        }
        const pkMatch = data.match(/"profilePage_(\d+)"/) ||
                        data.match(/"user_id":"(\d+)"/) ||
                        data.match(/"id":"(\d+)".*?"username"/);
        if (pkMatch) resolve(pkMatch[1]);
        else reject(new Error('NO_PK'));
      });
    });
    req.on('error', reject);
  });
}

// Fetch following list for a user
async function fetchFollowing(userId, count = 50) {
  const url = `https://www.instagram.com/api/v1/friendships/${userId}/following/?count=${count}`;
  const json = await httpGetJson(url);
  return json.users || [];
}

// =============================================
// COMMANDS
// =============================================

async function copyQueueToSuggestionTodo() {
  log('Copying scraped profiles to suggestion:todo...');

  // Get all scraped usernames
  const scraped = await redis.smembers(SCRAPED_KEY);
  log(`Found ${scraped.length} scraped profiles`);

  // Get already processed
  const done = await redis.scard(SUGGESTION_DONE_KEY);
  log(`Already processed: ${done}`);

  // Add all scraped to suggestion:todo (minus already done)
  let added = 0;
  for (const username of scraped) {
    const alreadyDone = await redis.sismember(SUGGESTION_DONE_KEY, username);
    if (!alreadyDone) {
      await redis.sadd(SUGGESTION_TODO_KEY, username);
      added++;
    }
  }

  log(`Added ${added} profiles to suggestion:todo (${scraped.length - added} already done)`);
  await showStatus();
}

async function startWorker() {
  if (!CONFIG.cookies) {
    console.error('ERROR: IG_COOKIES not set in .env');
    process.exit(1);
  }

  log('Starting suggestion worker...');
  log('Press Ctrl+C to stop.\n');

  let jobCount = 0;
  let totalNewDiscovered = 0;

  while (true) {
    // Pop a random username from the todo set
    const username = await redis.spop(SUGGESTION_TODO_KEY);
    if (!username) {
      log('Suggestion queue empty. Done!');
      break;
    }

    jobCount++;

    // Cooldown
    if (jobCount > 1 && jobCount % DELAYS.cooldownEvery === 0) {
      const cooldown = Math.floor(Math.random() * (DELAYS.cooldownTime[1] - DELAYS.cooldownTime[0] + 1)) + DELAYS.cooldownTime[0];
      log(`Cooldown: ${Math.round(cooldown / 1000)}s after ${jobCount} profiles...`);
      await new Promise(r => setTimeout(r, cooldown));
    }

    try {
      // Get pk for this username
      let pk = null;
      try {
        pk = await fetchProfilePk(username);
      } catch (err) {
        log(`[${jobCount}] @${username}: Failed to get pk (${err.message}), skipping`);
        await redis.sadd(SUGGESTION_DONE_KEY, username);
        await randomDelay(DELAYS.betweenProfiles);
        continue;
      }

      if (!pk) {
        log(`[${jobCount}] @${username}: No pk found, skipping`);
        await redis.sadd(SUGGESTION_DONE_KEY, username);
        await randomDelay(DELAYS.betweenProfiles);
        continue;
      }

      // Fetch following list
      await randomDelay([1000, 2000]);
      const following = await fetchFollowing(pk);

      let newCount = 0;
      for (const user of following) {
        if (user.is_private) continue;
        const isNew = await redis.sadd(DISCOVERED_KEY, user.username);
        if (isNew === 1) {
          // Add to main scrape queue
          await scrapeQueue.add('scrape', {
            username: user.username,
            pk: user.pk?.toString(),
            depth: 1,
            source: username,
          }, {
            removeOnComplete: true,
            removeOnFail: { count: 100 },
          });
          newCount++;
        }
      }

      totalNewDiscovered += newCount;

      // Mark as done
      await redis.sadd(SUGGESTION_DONE_KEY, username);
      await redis.hincrby(SUGGESTION_STATS_KEY, 'profiles_processed', 1);
      await redis.hincrby(SUGGESTION_STATS_KEY, 'new_discovered', newCount);

      log(`[${jobCount}] @${username}: ${following.length} following, ${newCount} new → queue`);

    } catch (err) {
      log(`[${jobCount}] @${username}: ERROR ${err.message}`);
      // Put it back if it's a temp error
      if (err.message.includes('429') || err.message.includes('ECONNRESET')) {
        await redis.sadd(SUGGESTION_TODO_KEY, username);
        log('  Rate limited — pausing 60s...');
        await new Promise(r => setTimeout(r, 60000));
      } else {
        await redis.sadd(SUGGESTION_DONE_KEY, username);
      }
    }

    await randomDelay(DELAYS.betweenProfiles);
  }

  log(`\nDone! Processed ${jobCount} profiles, discovered ${totalNewDiscovered} new accounts.`);
  await showStatus();
  process.exit(0);
}

async function showStatus() {
  const todoSize = await redis.scard(SUGGESTION_TODO_KEY);
  const doneSize = await redis.scard(SUGGESTION_DONE_KEY);
  const discovered = await redis.scard(DISCOVERED_KEY);
  const scraped = await redis.scard(SCRAPED_KEY);
  const stats = await redis.hgetall(SUGGESTION_STATS_KEY);
  const mainWaiting = await redis.llen('bull:scrape:wait');

  console.log(`
==================================================
  SUGGESTION WORKER STATUS
==================================================
  Todo:           ${todoSize}
  Done:           ${doneSize}
  ---
  Main queue:     ${mainWaiting} waiting
  Total discovered: ${discovered}
  Total scraped:    ${scraped}
  ---
  Profiles processed: ${stats.profiles_processed || 0}
  New discovered:     ${stats.new_discovered || 0}
==================================================
`);
}

// =============================================
// CLI
// =============================================

(async () => {
  const command = process.argv[2] || 'status';

  redis.on('error', (err) => {
    console.error('Redis error:', err.message);
    process.exit(1);
  });

  await new Promise(r => redis.once('connect', r));

  switch (command) {
    case 'copy':
      await copyQueueToSuggestionTodo();
      process.exit(0);
      break;
    case 'start':
      await startWorker();
      break;
    case 'status':
      await showStatus();
      process.exit(0);
      break;
    default:
      console.log(`
Usage:
  node worker.js copy      Copy scraped profiles to suggestion queue
  node worker.js start     Start fetching following lists
  node worker.js status    Show stats
      `);
      process.exit(0);
  }
})();
