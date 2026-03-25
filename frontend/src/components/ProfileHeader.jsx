import { formatNumber } from '../utils/formatNumber';

function ProfileHeader({ profile, proxyImg }) {
  const cleanName = (() => {
    const raw = profile.full_name || profile.username;
    const match = raw.match(/^(.+?)\s*\(@/);
    return match ? match[1].trim() : raw;
  })();

  return (
    <div className="bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden">
      <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
        <h1 className="text-xl font-bold text-gray-900">Profile Overview</h1>
        {profile.scraped_at && (
          <span className="text-xs text-gray-400">
            Last updated: {new Date(profile.scraped_at).toLocaleDateString('en-US', {
              day: 'numeric', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit'
            })}
          </span>
        )}
      </div>

      <div className="bg-gradient-to-b from-violet-50/80 to-white px-6 py-8">
        <div className="flex flex-col items-center text-center">
          {profile.profile_pic && (
            <img
              src={proxyImg(profile.profile_pic)}
              alt={profile.username}
              className="w-28 h-28 rounded-full object-cover border-4 border-white shadow-lg mb-4"
            />
          )}

          <div className="flex items-center gap-2 mb-1">
            <h2 className="text-2xl font-bold text-gray-900">{cleanName}</h2>
            {profile.is_verified && (
              <svg className="w-6 h-6 text-blue-500" viewBox="0 0 24 24" fill="currentColor">
                <path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10 10-4.5 10-10S17.5 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z"/>
              </svg>
            )}
            <svg className="w-5 h-5 text-pink-500 ml-1" viewBox="0 0 24 24" fill="currentColor">
              <path d="M12 2.163c3.204 0 3.584.012 4.85.07 3.252.148 4.771 1.691 4.919 4.919.058 1.265.069 1.645.069 4.849 0 3.205-.012 3.584-.069 4.849-.149 3.225-1.664 4.771-4.919 4.919-1.266.058-1.644.07-4.85.07-3.204 0-3.584-.012-4.849-.07-3.26-.149-4.771-1.699-4.919-4.92-.058-1.265-.07-1.644-.07-4.849 0-3.204.013-3.583.07-4.849.149-3.227 1.664-4.771 4.919-4.919 1.266-.057 1.645-.069 4.849-.069z"/>
            </svg>
          </div>

          <p className="text-sm text-gray-500 mb-2">@{profile.username}</p>

          {profile.bio && (
            <p className="text-sm text-gray-600 max-w-lg mb-4 leading-relaxed">{profile.bio}</p>
          )}

          <div className="flex items-center gap-8 mt-2">
            {profile.followers > 0 && (
              <div className="text-center">
                <div className="text-2xl font-bold text-gray-900">{formatNumber(profile.followers)}</div>
                <div className="text-xs text-gray-500 uppercase tracking-wide mt-0.5">Followers</div>
              </div>
            )}
            {profile.following > 0 && (
              <div className="text-center">
                <div className="text-2xl font-bold text-gray-900">{formatNumber(profile.following)}</div>
                <div className="text-xs text-gray-500 uppercase tracking-wide mt-0.5">Following</div>
              </div>
            )}
            {profile.posts_count > 0 && (
              <div className="text-center">
                <div className="text-2xl font-bold text-gray-900">{formatNumber(profile.posts_count)}</div>
                <div className="text-xs text-gray-500 uppercase tracking-wide mt-0.5">Posts</div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default ProfileHeader;
