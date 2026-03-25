import { formatNumber } from '../utils/formatNumber';

const formatDateBadge = (timestamp) => {
  const date = new Date(timestamp * 1000);
  const day = date.getDate();
  const month = date.toLocaleDateString('en-US', { month: 'short' });
  const year = date.getFullYear().toString().slice(-2);
  return `${day} ${month} ${year}`;
};

function PostsGrid({ posts, username, profilePic, proxyImg }) {
  if (!posts || posts.length === 0) {
    return (
      <div className="text-center py-12 text-gray-500">
        <p className="text-lg">No posts available</p>
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {posts.map((post, index) => (
        <a
          key={post.media_id || post.code || index}
          href={post.url}
          target="_blank"
          rel="noopener noreferrer"
          className="bg-white rounded-xl border border-gray-200 overflow-hidden hover:shadow-md transition-all group"
        >
          {/* Card Header */}
          <div className="flex items-center justify-between px-3 py-2.5 border-b border-gray-100">
            <div className="flex items-center gap-2">
              {profilePic && (
                <img
                  src={proxyImg(profilePic)}
                  alt={username}
                  className="w-6 h-6 rounded-full object-cover"
                />
              )}
              <span className="text-sm font-medium text-gray-700">@{username}</span>
            </div>
            <div className="flex items-center gap-1.5">
              <svg className="w-3.5 h-3.5 text-pink-500" viewBox="0 0 24 24" fill="currentColor">
                <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069z"/>
              </svg>
              <span className="text-xs text-gray-500">{post.post_type || 'Post'}</span>
              {post.is_paid_partnership && (
                <span className="text-xs bg-amber-100 text-amber-700 px-1.5 py-0.5 rounded font-medium">Paid</span>
              )}
            </div>
          </div>

          {/* Image with date badge */}
          <div className="relative aspect-square overflow-hidden bg-gray-100">
            {post.image_url ? (
              <img
                src={proxyImg(post.image_url)}
                alt={`Post ${post.code}`}
                className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                loading="lazy"
              />
            ) : (
              <div className="w-full h-full flex items-center justify-center text-gray-300">
                <svg className="w-16 h-16" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1} d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
                </svg>
              </div>
            )}
            {post.timestamp && (
              <div className="absolute bottom-2 right-2 bg-black/60 text-white text-xs px-2 py-1 rounded">
                {formatDateBadge(post.timestamp)}
              </div>
            )}
            {post.carousel_count && (
              <div className="absolute top-2 right-2 bg-black/60 text-white text-xs px-2 py-1 rounded">
                1/{post.carousel_count}
              </div>
            )}
          </div>

          {/* Caption */}
          {post.caption && (
            <div className="px-3 py-2">
              <p className="text-sm text-gray-700 line-clamp-2">{post.caption}</p>
            </div>
          )}

          {/* Engagement Footer */}
          <div className="flex items-center gap-4 px-3 py-2.5 border-t border-gray-100 text-gray-500">
            {post.views > 0 && (
              <div className="flex items-center gap-1 text-xs">
                <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <span>{formatNumber(post.views)}</span>
              </div>
            )}
            <div className="flex items-center gap-1 text-xs">
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z" />
              </svg>
              <span>{formatNumber(post.likes)}</span>
            </div>
            <div className="flex items-center gap-1 text-xs">
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
              </svg>
              <span>{formatNumber(post.comments)}</span>
            </div>
            {post.engagement_rate > 0 && (
              <div className="ml-auto text-xs text-violet-600 font-medium">
                {post.engagement_rate.toFixed(2)}%
              </div>
            )}
          </div>
        </a>
      ))}
    </div>
  );
}

export default PostsGrid;
