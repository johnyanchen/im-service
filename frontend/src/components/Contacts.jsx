export default function Contacts({ users, userId, onStartChat, getColor }) {
  return (
    <div className="flex-1 overflow-y-auto">
      <div className="px-4 py-3 border-b border-gray-100">
        <p className="text-xs text-gray-400">所有用户 · {users.length} 人</p>
      </div>
      {users.filter(u => u.id !== userId).map(u => (
        <div
          key={u.id}
          className="flex items-center gap-3 px-4 py-3 hover:bg-gray-50 cursor-pointer transition"
          onClick={() => onStartChat(u.id)}
        >
          <div className="w-10 h-10 rounded-full flex items-center justify-center text-white text-sm font-bold" style={{ background: getColor(u.username) }}>
            {u.username[0]?.toUpperCase()}
          </div>
          <div className="flex-1">
            <span className="text-sm font-medium text-gray-800">{u.username}</span>
            <p className="text-xs text-gray-400">ID: {u.id}</p>
          </div>
          <button className="text-xs px-3 py-1.5 rounded-full bg-blue-50 text-blue-500 hover:bg-blue-100 transition">
            发消息
          </button>
        </div>
      ))}
      {users.filter(u => u.id !== userId).length === 0 && (
        <div className="text-center text-gray-400 text-sm mt-12">暂无其他用户</div>
      )}
    </div>
  )
}
