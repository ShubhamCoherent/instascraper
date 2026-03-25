import { useState, useEffect } from 'react';
import ProfileHeader from './components/ProfileHeader';
import PlatformTabs from './components/PlatformTabs';
import StatsGrid from './components/StatsGrid';
import PostsGrid from './components/PostsGrid';

const API_BASE = '/api';

// Proxy Instagram CDN images through our backend to avoid referrer blocking
const proxyImg = (url) => {
  if (!url) return '';
  if (url.includes('instagram') || url.includes('fbcdn')) {
    return `${API_BASE}/image-proxy?url=${encodeURIComponent(url)}`;
  }
  return url;
};

function App() {
  const [profile, setProfile] = useState(null);
  const [posts, setPosts] = useState([]);
  const [totalPosts, setTotalPosts] = useState(0);
  const [activeTab, setActiveTab] = useState('instagram');
  const [sortBy, setSortBy] = useState('date');
  const [page, setPage] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const perPage = 8;
  const username = 'cristiano';

  // Load profile data
  useEffect(() => {
    fetchProfile();
  }, []);

  // Load posts when sort or page changes
  useEffect(() => {
    if (profile) {
      fetchPosts();
    }
  }, [sortBy, page, profile]);

  const fetchProfile = async () => {
    try {
      const res = await fetch(`${API_BASE}/profile/${username}`);
      if (!res.ok) throw new Error('Profile not found');
      const data = await res.json();
      setProfile(data);
      setLoading(false);
    } catch (err) {
      setError(err.message);
      setLoading(false);
    }
  };

  const fetchPosts = async () => {
    try {
      const res = await fetch(
        `${API_BASE}/profile/${username}/posts?sort=${sortBy}&size=${perPage}&offset=${page * perPage}`
      );
      if (!res.ok) throw new Error('Failed to load posts');
      const data = await res.json();
      setPosts(data.posts);
      setTotalPosts(data.total);
    } catch (err) {
      console.error('Error loading posts:', err);
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-2 border-violet-600 border-t-transparent mx-auto mb-4"></div>
          <p className="text-gray-500">Loading influencer data...</p>
        </div>
      </div>
    );
  }

  if (error || !profile) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center max-w-md px-6">
          <div className="w-16 h-16 bg-amber-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg className="w-8 h-8 text-amber-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4.5c-.77-.833-2.694-.833-3.464 0L3.34 16.5c-.77.833.192 2.5 1.732 2.5z" />
            </svg>
          </div>
          <h2 className="text-xl font-bold text-gray-800 mb-2">No Data Available</h2>
          <p className="text-gray-500 mb-6 text-sm">
            Run the scraper first, then start the backend server.
          </p>
          <div className="bg-gray-900 rounded-lg p-4 text-left">
            <code className="text-sm text-green-400 font-mono">
              cd scraper && node scrape.js<br />
              cd ../backend && npm start
            </code>
          </div>
        </div>
      </div>
    );
  }

  const totalPages = Math.ceil(totalPosts / perPage);

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Profile Header */}
      <div className="max-w-6xl mx-auto px-4 pt-6">
        <ProfileHeader profile={profile} proxyImg={proxyImg} />
      </div>

      {/* Platform Tabs */}
      <div className="max-w-6xl mx-auto px-4 mt-6">
        <div className="bg-white rounded-t-xl shadow-sm border border-gray-100 border-b-0">
          <PlatformTabs
            activeTab={activeTab}
            onTabChange={setActiveTab}
            profile={profile}
          />
        </div>
      </div>

      {/* Main Content */}
      <main className="max-w-6xl mx-auto px-4 pb-12">
        <div className="bg-white rounded-b-xl shadow-sm border border-gray-100 border-t-0 p-6 space-y-10">
          {/* Stats */}
          <StatsGrid
            profile={profile}
            metrics={profile.engagement_metrics}
          />

          {/* Posts */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-bold text-gray-900">
                Posts
                <span className="text-sm font-normal text-gray-400 ml-2">({totalPosts} total)</span>
              </h3>
              <div className="flex items-center gap-3">
                <select
                  value={sortBy}
                  onChange={(e) => { setSortBy(e.target.value); setPage(0); }}
                  className="text-sm border border-gray-200 rounded-lg px-3 py-1.5 text-gray-600 bg-white"
                >
                  <option value="date">Most recent</option>
                  <option value="likes">Most liked</option>
                  <option value="comments">Most commented</option>
                </select>
              </div>
            </div>

            <PostsGrid
              posts={posts}
              username={profile.username}
              profilePic={profile.profile_pic}
              proxyImg={proxyImg}
            />

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex items-center justify-center gap-3 mt-6">
                <button
                  onClick={() => setPage(Math.max(0, page - 1))}
                  disabled={page === 0}
                  className="px-3 py-1.5 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  Previous
                </button>
                <span className="text-sm text-gray-500">
                  Page {page + 1} of {totalPages}
                </span>
                <button
                  onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
                  disabled={page >= totalPages - 1}
                  className="px-3 py-1.5 text-sm rounded-lg border border-gray-200 hover:bg-gray-50 disabled:opacity-30 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
