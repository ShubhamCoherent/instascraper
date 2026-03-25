import ChartStatCard from './ChartStatCard';
import { formatNumber } from '../utils/formatNumber';

function StatCard({ title, value, subtitle, highlight }) {
  return (
    <div className={`bg-white rounded-xl border p-5 hover:shadow-md transition-shadow ${highlight ? 'border-violet-200 bg-violet-50/30' : 'border-gray-200'}`}>
      <div className="text-sm font-medium text-gray-500 mb-2">{title}</div>
      <div className="flex items-end justify-between mb-1">
        <span className="text-2xl font-bold text-gray-900">{value}</span>
      </div>
      {subtitle && (
        <div className="text-xs text-gray-400 mt-1">{subtitle}</div>
      )}
    </div>
  );
}

function StatsGrid({ profile, metrics }) {
  if (!metrics) return null;

  const monthlyStats = profile.monthly_stats || [];
  const partnerships = profile.partnership_details || [];
  const totalEngagements = (metrics.total_likes || 0) + (metrics.total_comments || 0);
  const postTypes = profile.post_types || {};

  return (
    <div>
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-bold text-gray-900">Stats</h3>
        <span className="text-xs text-gray-400">Based on {metrics.posts_analyzed} posts</span>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
        <ChartStatCard
          title="Engagements"
          value={formatNumber(totalEngagements)}
          secondaryLabel="Avg per post"
          secondaryValue={formatNumber(metrics.avg_engagement)}
          data={monthlyStats}
          dataKey="total_engagements"
          medianValue={metrics.median_engagements}
        />

        <ChartStatCard
          title="Engagement Rate"
          value={metrics.engagement_rate > 0 ? `${metrics.engagement_rate}%` : 'N/A'}
          secondaryLabel="Median"
          secondaryValue={`${metrics.median_engagement_rate || 0}%`}
          data={monthlyStats}
          dataKey="avg_engagement_rate"
          medianValue={metrics.median_engagement_rate}
        />

        <ChartStatCard
          title="Posts"
          value={metrics.posts_analyzed}
          secondaryLabel="Avg per week"
          secondaryValue={metrics.posts_per_week || '0'}
          data={monthlyStats}
          dataKey="post_count"
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
        <ChartStatCard
          title="Likes"
          value={formatNumber(metrics.total_likes)}
          secondaryLabel="Avg per post"
          secondaryValue={formatNumber(metrics.avg_likes)}
          data={monthlyStats}
          dataKey="total_likes"
          medianValue={metrics.median_likes}
        />

        <ChartStatCard
          title="Comments"
          value={formatNumber(metrics.total_comments)}
          secondaryLabel="Avg per post"
          secondaryValue={formatNumber(metrics.avg_comments)}
          data={monthlyStats}
          dataKey="total_comments"
          medianValue={metrics.median_comments}
        />
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          title="Posts/Week"
          value={metrics.posts_per_week || '0'}
          subtitle={Object.entries(postTypes).map(([k, v]) => `${v} ${k}s`).join(', ')}
        />

        {metrics.total_views > 0 && (
          <StatCard
            title="Avg Views"
            value={formatNumber(metrics.avg_views)}
            subtitle={`Total: ${formatNumber(metrics.total_views)}`}
          />
        )}

        <StatCard
          title="Partnerships"
          value={`${metrics.partnership_posts || 0}`}
          subtitle={`${metrics.partnership_percentage || 0}% of posts are sponsored`}
          highlight={metrics.partnership_posts > 0}
        />

        <StatCard
          title="Mentions"
          value={`${metrics.posts_with_mentions || 0}`}
          subtitle={`of ${metrics.posts_analyzed} posts tag other accounts`}
        />
      </div>

      {/* Brand Partnerships */}
      {partnerships.length > 0 && (
        <div className="mt-6">
          <h3 className="text-lg font-bold text-gray-900 mb-4">Brand Partnerships</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {partnerships.map((p) => (
              <div key={p.brand} className="bg-white rounded-xl border border-gray-200 p-4 hover:shadow-md transition-shadow">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <div className="w-8 h-8 rounded-full bg-violet-100 flex items-center justify-center">
                      <span className="text-sm font-bold text-violet-600">
                        {p.brand.charAt(0).toUpperCase()}
                      </span>
                    </div>
                    <div>
                      <div className="text-sm font-semibold text-gray-900">@{p.brand}</div>
                      <div className="text-xs text-gray-400">{p.post_count} {p.post_count === 1 ? 'post' : 'posts'}</div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm font-bold text-gray-900">{formatNumber(p.avg_engagement)}</div>
                    <div className="text-xs text-gray-400">total engagement</div>
                  </div>
                </div>

                <div className="flex items-center gap-4 text-xs text-gray-500 mb-3">
                  <span className="flex items-center gap-1">
                    <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z" />
                    </svg>
                    {formatNumber(p.avg_likes)} avg
                  </span>
                  <span className="flex items-center gap-1">
                    <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
                    </svg>
                    {formatNumber(p.total_comments)} total
                  </span>
                </div>

                {p.hashtags.length > 0 && (
                  <div className="flex flex-wrap gap-1.5">
                    {p.hashtags.map((tag) => (
                      <span key={tag} className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded-full">
                        {tag}
                      </span>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default StatsGrid;
