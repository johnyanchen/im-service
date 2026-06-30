export default function Sidebar({ conversations, currentConvId, onSelect, getColor }) {
  return (
    <div className="flex-1 overflow-y-auto">
      {conversations.length === 0 && (
        <div className="text-center text-gray-500 text-sm mt-12">暂无会话</div>
      )}
      {conversations.map(c => (
        <div
          key={c.conversation_id}
          onClick={() => onSelect(c.conversation_id)}
          className={`flex items-center gap-3 px-3 py-3 cursor-pointer transition-colors ${c.conversation_id === currentConvId ? 'bg-[#c9c9c9]' : 'hover:bg-[#d8d8d8]'}`}
        >
          <div className="w-10 h-10 rounded-md flex items-center justify-center text-white text-sm font-bold shrink-0" style={{ background: getColor(c.name) }}>
            {c.type === 'group' ? '群' : (c.name || '?')[0]?.toUpperCase()}
          </div>
          <div className="flex-1 min-w-0">
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-900 truncate">{c.name}</span>
            </div>
            {c.last_msg_content && (
              <p className="text-xs text-gray-500 truncate mt-0.5">
                {c.last_msg_from ? `${c.last_msg_from}: ` : ''}{c.last_msg_content}
              </p>
            )}
          </div>
          {c.unread_count > 0 && (
            <span className="px-1.5 py-0.5 bg-[#f55c5c] text-white text-[10px] rounded-full min-w-[18px] text-center">{c.unread_count}</span>
          )}
        </div>
      ))}
    </div>
  )
}
