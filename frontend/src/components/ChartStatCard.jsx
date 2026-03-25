import { LineChart, Line, XAxis, ReferenceLine, ResponsiveContainer, Tooltip } from 'recharts';
import { formatNumber } from '../utils/formatNumber';

function ChartStatCard({ title, value, secondaryLabel, secondaryValue, data, dataKey, medianValue, color = '#7c3aed' }) {
  return (
    <div className="bg-white rounded-xl border border-gray-200 p-5 hover:shadow-md transition-shadow">
      <div className="text-sm font-medium text-gray-500 mb-1">{title}</div>

      <div className="flex items-end justify-between mb-3">
        <span className="text-2xl font-bold text-gray-900">{value}</span>
        {secondaryLabel && (
          <span className="text-xs text-gray-400 text-right">
            {secondaryLabel}: <span className="font-semibold text-gray-600">{secondaryValue}</span>
          </span>
        )}
      </div>

      {data && data.length > 1 && (
        <div className="h-20">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 5, right: 5, bottom: 0, left: 5 }}>
              <XAxis
                dataKey="label"
                tick={{ fontSize: 10, fill: '#9ca3af' }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e5e7eb', boxShadow: '0 2px 8px rgba(0,0,0,0.08)' }}
                formatter={(val) => [formatNumber(val), title]}
                labelStyle={{ fontSize: 11, color: '#6b7280', marginBottom: 2 }}
              />
              <Line
                type="monotone"
                dataKey={dataKey}
                stroke={color}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 3, fill: color }}
              />
              {medianValue !== undefined && medianValue > 0 && (
                <ReferenceLine
                  y={medianValue}
                  stroke="#d1d5db"
                  strokeDasharray="4 4"
                  strokeWidth={1}
                  label={{ value: 'Median', position: 'right', fontSize: 9, fill: '#9ca3af' }}
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}

export default ChartStatCard;
