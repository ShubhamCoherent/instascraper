# Influencer Analytics Platform

A Favikon-like matchmaking platform for brands and influencers. Scrapes Instagram data using a hybrid approach and presents analytics via a React dashboard.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SYSTEM ARCHITECTURE                         │
│                                                                     │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────────┐  │
│  │   SCRAPER    │      │   BACKEND    │      │    FRONTEND      │  │
│  │              │      │              │      │                  │  │
│  │  Method 1:   │      │  Express.js  │      │  React + Vite    │  │
│  │  HTTP GET    │─────▶│  REST API    │◀─────│  Tailwind CSS    │  │
│  │  (profile)   │ JSON │              │ HTTP │                  │  │
│  │              │ file │  Serves      │      │  Dashboard with  │  │
│  │  Method 2:   │      │  pre-scraped │      │  - Profile card  │  │
│  │  GraphQL API │      │  data from   │      │  - Stats grid    │  │
│  │  (posts)     │      │  JSON files  │      │  - Post grid     │  │
│  └──────────────┘      └──────────────┘      └──────────────────┘  │
│                                                                     │
│  /scraper/              /backend/              /frontend/            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
Email Platform/
├── scraper/                  # Standalone data collection (separate from backend)
│   └── scrape.js             # Hybrid scraper: HTTP profile + GraphQL posts
│
├── backend/                  # API server (serves pre-scraped data)
│   ├── data/                 # JSON data store (output from scraper)
│   │   └── cristiano_complete.json
│   ├── src/
│   │   └── server.js         # Express REST API
│   └── package.json
│
└── frontend/                 # React dashboard
    ├── src/
    │   ├── App.jsx           # Main app (calls backend API)
    │   └── components/
    │       ├── ProfileHeader.jsx   # Avatar, name, bio, followers
    │       ├── PlatformTabs.jsx    # Platform navigation
    │       ├── StatsGrid.jsx       # Analytics cards (8 metrics)
    │       └── PostsGrid.jsx       # Paginated post cards
    └── vite.config.js        # Dev proxy → backend:3001
```

---

## Data Collection: Hybrid Approach

We use **two methods** to collect different types of data from Instagram:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     DATA COLLECTION FLOW                            │
│                                                                     │
│  METHOD 1: HTTP Profile Scrape                                      │
│  ─────────────────────────────                                      │
│  GET https://instagram.com/{username}/                              │
│  ┌─────────────────────────────────────────────┐                    │
│  │ Extracts from HTML meta tags:               │                    │
│  │   • Followers count (671M)                  │                    │
│  │   • Following count (641)                   │                    │
│  │   • Total posts count (4,007)               │                    │
│  │   • Bio text                                │                    │
│  │   • Profile picture URL (from og:image)     │                    │
│  │   • Verified status                         │                    │
│  └─────────────────────────────────────────────┘                    │
│  Speed: 1 request, ~1 second                                        │
│  Needs: Session cookies only                                        │
│                                                                     │
│                                                                     │
│  METHOD 2: GraphQL API Posts                                        │
│  ───────────────────────────                                        │
│  POST https://instagram.com/graphql/query                           │
│  ┌─────────────────────────────────────────────┐                    │
│  │ Returns per post (12 per request):          │                    │
│  │   • Exact like count (6,606,787)            │                    │
│  │   • Exact comment count (133,180)           │                    │
│  │   • View count (Reels only)                 │                    │
│  │   • Post type (Post / Reel)                 │                    │
│  │   • Brand partnership flag                  │                    │
│  │   • User tags / mentions                    │                    │
│  │   • Caption text                            │                    │
│  │   • Timestamp                               │                    │
│  │   • Image/thumbnail URL                     │                    │
│  │   • Carousel count                          │                    │
│  │   • Post shortcode (for URL)                │                    │
│  └─────────────────────────────────────────────┘                    │
│  Speed: 5 requests for 50 posts, ~20 seconds                        │
│  Needs: Session cookies + fb_dtsg + doc_id tokens                   │
│                                                                     │
│                                                                     │
│  COMBINED OUTPUT (saved to backend/data/{username}_complete.json)   │
│  ┌─────────────────────────────────────────────┐                    │
│  │ Profile: followers, following, posts_count,  │                    │
│  │          bio, profile_pic, verified           │                    │
│  │                                               │                    │
│  │ 50 Posts: exact likes, comments, type,        │                    │
│  │           partnerships, mentions, images       │                    │
│  │                                               │                    │
│  │ Metrics: engagement rate, avg likes/comments, │                    │
│  │          posts/week, partnership %, mentions   │                    │
│  └─────────────────────────────────────────────┘                    │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Two Methods?

| Data Point | Method 1 (HTTP) | Method 2 (GraphQL) |
|---|---|---|
| Followers count | Yes (from meta tags) | No |
| Bio | Yes | No |
| Post likes (exact) | No | Yes (6,606,787) |
| Post comments (exact) | No | Yes (133,180) |
| Post type (Post/Reel) | No | Yes |
| Brand partnership | No | Yes |
| User tags/mentions | No | Yes |

Neither method alone gives us everything. Combined, we get a complete picture.

---

## Analytics & Insights

From 50 posts, we compute and display:

| Metric | Description | Example |
|---|---|---|
| Engagement Rate | (likes+comments) / followers * 100 | 0.9% |
| Avg Likes | Total likes / posts analyzed | 5.93M |
| Avg Comments | Total comments / posts analyzed | 91.1K |
| Posts per Week | Posts count / time span in weeks | 3.7 |
| Partnership Posts | Count of `is_paid_partnership: true` | 6 (12%) |
| Posts with Mentions | Posts that tag other accounts | 39 of 50 |
| Post Type Split | Post vs Reel breakdown | 38 Posts, 12 Reels |
| Total Engagements | Sum of all likes + comments | 301M |

---

## Quick Start

```bash
# 1. Scrape data (one-time, runs ~25 seconds for 50 posts)
cd scraper
node scrape.js cristiano 50

# 2. Start backend API
cd ../backend
npm install
npm start                    # http://localhost:3001

# 3. Start frontend dashboard
cd ../frontend
npm install
npm run dev                  # http://localhost:5173
```

### API Endpoints

```
GET /api/profiles                              # List all scraped profiles
GET /api/profile/:username                     # Profile + metrics
GET /api/profile/:username/posts               # Paginated posts
    ?sort=date|likes|comments
    &size=8
    &offset=0
```

---

## Scalability Analysis

### Current Approach: What It Can Handle

```
┌──────────────────────────────────────────────────────────┐
│  CURRENT CAPACITY (2-3 Instagram accounts)               │
│                                                          │
│  Per influencer:                                         │
│    • 1 HTTP request (profile)     ~1 sec                 │
│    • 5 GraphQL requests (posts)   ~20 sec (with delays)  │
│    • Total: ~25 seconds per influencer                   │
│                                                          │
│  Scale:                                                  │
│    • 100 influencers   →  ~40 minutes                    │
│    • 500 influencers   →  ~3.5 hours                     │
│    • 1,000 influencers →  ~7 hours                       │
│                                                          │
│  Daily updates (only check for new posts):               │
│    • 1 HTTP + 1 GraphQL per influencer                   │
│    • 1,000 influencers → ~1 hour                         │
│                                                          │
│  Suitable for: MVP, pilot with select brands,            │
│  demo with curated influencer list                       │
└──────────────────────────────────────────────────────────┘
```

### Scaling Beyond 1,000 Influencers

```
┌──────────────────────────────────────────────────────────────────┐
│  SCALING STAGES                                                  │
│                                                                  │
│  STAGE 1: Current (1-1,000 influencers)                          │
│  ──────────────────────────────────────                          │
│  • Scraper: Single Node.js script                                │
│  • Storage: JSON files                                           │
│  • Backend: Express serving JSON                                 │
│  • Accounts: 2-3 Instagram sessions                              │
│  • Infra: Single machine                                         │
│                                                                  │
│                         │                                        │
│                         ▼                                        │
│                                                                  │
│  STAGE 2: Growth (1,000-10,000 influencers)                      │
│  ──────────────────────────────────────────                      │
│  • Scraper: Queue-based (BullMQ + Redis)                         │
│  • Storage: MongoDB (replace JSON files)                         │
│  • Backend: Express + MongoDB queries                            │
│  • Accounts: 5-10 Instagram sessions                             │
│  • Add: Proxy rotation (residential proxies)                     │
│  • Add: Incremental scraping (only new posts)                    │
│  • Add: Cron-based scheduling                                    │
│  • Infra: 2-3 servers                                            │
│                                                                  │
│                         │                                        │
│                         ▼                                        │
│                                                                  │
│  STAGE 3: Scale (10,000-100,000 influencers)                     │
│  ──────────────────────────────────────────                      │
│  • Buy bulk data from providers (BrightData, Modash)             │
│  • Influencer sign-up with Instagram OAuth (Graph API)           │
│  • Own scraper only for gap-filling / on-demand deep scrape      │
│  • Storage: MongoDB with proper indexing                         │
│  • Add: CDN for cached images                                    │
│  • Add: Monitoring & alerting (rate limits, session health)      │
│  • Infra: Kubernetes / managed cloud                             │
│                                                                  │
│                         │                                        │
│                         ▼                                        │
│                                                                  │
│  STAGE 4: Production (100,000+ influencers)                      │
│  ──────────────────────────────────────────                      │
│  • Primary: Third-party data providers (catalog)                 │
│  • Secondary: Instagram Graph API (signed-up influencers)        │
│  • Tertiary: Own scraper (targeted deep scrapes only)            │
│  • Storage: MongoDB + Redis cache + S3 for images                │
│  • Infra: Multi-region cloud deployment                          │
│                                                                  │
│  This is how Favikon-scale platforms work:                        │
│  Buy the catalog, get OAuth for signed users, scrape the gaps.   │
└──────────────────────────────────────────────────────────────────┘
```

### Three Data Acquisition Methods Compared

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  METHOD A: Own Scraping (what we built)                          │
│  ┌────────────────────────────────────────────────────────┐      │
│  │ How:     HTTP profile scrape + GraphQL API for posts    │      │
│  │ Cost:    Free (just Instagram accounts + server)        │      │
│  │ Scale:   Up to ~10,000 influencers                      │      │
│  │ Quality: Exact counts, real-time data                   │      │
│  │ Risk:    Account bans, rate limits, token expiry         │      │
│  │ Best for: MVP, targeted scraping, gap-filling           │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                  │
│  METHOD B: Third-Party Data Providers                            │
│  ┌────────────────────────────────────────────────────────┐      │
│  │ How:     API call to BrightData/Modash/Phyllo           │      │
│  │ Cost:    Pay per profile ($0.01-$0.10 per record)       │      │
│  │ Scale:   Millions of profiles                           │      │
│  │ Quality: Good, may be slightly delayed                  │      │
│  │ Risk:    Vendor dependency, cost at scale               │      │
│  │ Best for: Building the catalog of all influencers       │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                  │
│  METHOD C: Instagram Official API (OAuth)                        │
│  ┌────────────────────────────────────────────────────────┐      │
│  │ How:     Influencer connects account via OAuth          │      │
│  │ Cost:    Free (Meta Graph API)                          │      │
│  │ Scale:   Unlimited (for connected accounts)             │      │
│  │ Quality: Best (audience demographics, stories, etc.)    │      │
│  │ Risk:    Influencer must opt in                         │      │
│  │ Best for: Deep analytics for signed-up influencers      │      │
│  └────────────────────────────────────────────────────────┘      │
│                                                                  │
│  PRODUCTION STRATEGY: Use all three together                     │
│  ─────────────────────────────────────────────                   │
│  Layer 1: Data provider → catalog of all influencers             │
│  Layer 2: OAuth API → deep data for signed-up influencers        │
│  Layer 3: Own scraper → on-demand for specific profiles          │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## Session Configuration

The scraper requires valid Instagram session tokens. These are configured at the top of `scraper/scrape.js`:

| Token | Where to Find | Lifespan |
|---|---|---|
| `sessionid` | Cookie from browser (DevTools > Application > Cookies) | ~90 days |
| `csrftoken` | Cookie from browser | Changes periodically |
| `fb_dtsg` | Network tab > any graphql/query request > Payload | Per page load |
| `lsd` | Network tab > Payload | Per page load |
| `doc_id` | Network tab > Payload > `doc_id` field | Until Instagram deploys |
| `jazoest` | Network tab > Payload | Per session |

To update tokens:
1. Open `instagram.com` in Chrome while logged in
2. Open DevTools > Network tab
3. Navigate to any profile and find a `graphql/query` request
4. Copy the values from the Payload tab into `CONFIG` in `scrape.js`

---

## Data Quality: What We Get

### From Cristiano Ronaldo (@cristiano) — 50 posts scraped:

```
Profile Data (Method 1 - HTTP):
  Followers:      671,000,000
  Following:      641
  Total Posts:    4,007
  Bio:            Available
  Profile Pic:    Available
  Verified:       Yes

Post Analytics (Method 2 - GraphQL API):
  Posts Analyzed:  50
  Post Types:     38 Posts, 12 Reels
  Posts/Week:     3.7

  Avg Likes:      5,928,163 (exact, not rounded)
  Avg Comments:   91,112 (exact)
  Engagement:     0.9%

  Partnerships:   6 of 50 (12%)
  With Mentions:  39 of 50

  Top Post:       29.5M likes (DRPzD6oCLFZ)
```

### What We Don't Get (and Why)

| Data Point | Available? | Notes |
|---|---|---|
| View counts (Posts) | No | Instagram only provides views for Reels. Favikon estimates these. |
| View counts (Reels) | Partially | Available in API but returns null in timeline query. Needs separate call. |
| Audience demographics | No | Requires Instagram Graph API (OAuth). |
| Story performance | No | Requires Instagram Graph API (OAuth). |
| Share/save counts | No | Instagram doesn't expose these publicly. |
| Exact follower count | Approximate | Meta tags give rounded numbers (671M vs 670,843,217). |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Scraper | Node.js (native `https` module, no dependencies) |
| Backend | Express.js 5, CORS |
| Frontend | React 19, Vite 7, Tailwind CSS v4 |
| Data Store | JSON files (upgradeable to MongoDB) |
| Styling | Tailwind CSS with Inter font |
