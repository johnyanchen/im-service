import { useState, useEffect } from 'react'

export default function CreateGroupModal({ auth, getColor, onClose, onCreate }) {
  const [name, setName] = useState('')
  const [friends, setFriends] = useState([])
  const [selected, setSelected] = useState(new Set())
  const [keyword, setKeyword] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState('')

  const headers = { 'Content-Type': 'application/json', 'Authorization': `Bearer ${auth.token}` }

  useEffect(() => {
    fetch('/api/friends', { headers })
      .then(r => r.json())
      .then(d => setFriends(d.friends || []))
      .catch(() => setError('加载好友列表失败'))
  }, [])

  const toggle = (id) => {
    setSelected(prev => {
      const next = new Set(prev)
      next.has(id) ? next.delete(id) : next.add(id)
      return next
    })
  }

  const filtered = friends.filter(f =>
    f.username.toLowerCase().includes(keyword.trim().toLowerCase())
  )

  const submit = async () => {
    if (!name.trim()) { setError('请输入群名'); return }
    if (selected.size === 0) { setError('请至少选择一位成员'); return }
    setSubmitting(true)
    setError('')
    try {
      const res = await fetch('/api/conversations/group', {
        method: 'POST', headers,
        body: JSON.stringify({ name: name.trim(), member_ids: [...selected] }),
      })
      const data = await res.json()
      if (data.error) { setError(data.error); setSubmitting(false); return }
      onCreate(data.conversationId || data.conversation_id)
    } catch (e) {
      setError('创建失败，请重试')
      setSubmitting(false)
    }
  }

  return (
    <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50" onClick={onClose}>
      <div className="w-[380px] max-h-[80vh] bg-white rounded-lg shadow-xl flex flex-col overflow-hidden" onClick={e => e.stopPropagation()}>
        {/* 标题 */}
        <div className="px-5 py-3 border-b border-[#ececec] flex items-center justify-between">
          <h3 className="text-sm font-medium text-gray-900">发起群聊</h3>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 text-lg leading-none">×</button>
        </div>

        {/* 群名 */}
        <div className="px-5 py-3 border-b border-[#ececec]">
          <label className="text-xs text-gray-500 block mb-1.5">群名称</label>
          <input
            className="w-full px-3 py-2 rounded-md bg-white outline-none text-sm border border-[#e0e0e0] focus:border-[#07c160] transition"
            placeholder="给群组起个名字"
            value={name}
            onChange={e => setName(e.target.value)}
            maxLength={64}
            autoFocus
          />
        </div>

        {/* 成员选择 */}
        <div className="px-5 pt-3 pb-1 flex items-center justify-between">
          <span className="text-xs text-gray-500">选择成员 · 已选 {selected.size}</span>
        </div>
        <div className="px-5 pb-2">
          <input
            className="w-full px-3 py-1.5 rounded-md bg-[#f5f5f5] outline-none text-xs border border-transparent focus:border-[#07c160] transition"
            placeholder="搜索好友"
            value={keyword}
            onChange={e => setKeyword(e.target.value)}
          />
        </div>
        <div className="flex-1 overflow-y-auto px-2 min-h-[120px]">
          {filtered.length === 0 && (
            <div className="text-center text-gray-400 text-xs mt-8">
              {friends.length === 0 ? '还没有好友，先去添加好友吧' : '没有匹配的好友'}
            </div>
          )}
          {filtered.map(u => {
            const checked = selected.has(u.id)
            return (
              <div key={u.id} onClick={() => toggle(u.id)}
                className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-[#f5f5f5] transition">
                <span className={`w-4 h-4 rounded flex items-center justify-center border shrink-0 transition ${checked ? 'bg-[#07c160] border-[#07c160]' : 'border-gray-300'}`}>
                  {checked && (
                    <svg className="w-3 h-3 text-white" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M16.7 5.3a1 1 0 010 1.4l-7.5 7.5a1 1 0 01-1.4 0L3.3 9.7a1 1 0 011.4-1.4l3.1 3.1 6.8-6.8a1 1 0 011.4 0z" clipRule="evenodd"/></svg>
                  )}
                </span>
                <div className="w-8 h-8 rounded-md flex items-center justify-center text-white text-xs font-bold shrink-0" style={{ background: getColor(u.username) }}>
                  {u.username[0]?.toUpperCase()}
                </div>
                <span className="text-sm text-gray-900 truncate">{u.username}</span>
              </div>
            )
          })}
        </div>

        {/* 底部 */}
        <div className="px-5 py-3 border-t border-[#ececec]">
          {error && <p className="text-[11px] text-[#f55c5c] mb-2">{error}</p>}
          <div className="flex gap-2 justify-end">
            <button onClick={onClose} className="px-4 py-1.5 rounded-md bg-gray-100 text-gray-600 text-sm hover:bg-gray-200 transition">取消</button>
            <button onClick={submit} disabled={submitting}
              className="px-4 py-1.5 rounded-md bg-[#07c160] text-white text-sm hover:bg-[#06ae56] transition disabled:opacity-50">
              {submitting ? '创建中...' : `创建${selected.size > 0 ? ` (${selected.size})` : ''}`}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
