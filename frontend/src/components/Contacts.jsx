import { useState, useEffect } from 'react'

export default function Contacts({ auth, getColor, onStartChat, friendTick, onFriendDeleted }) {
  const [friends, setFriends] = useState([])
  const [requests, setRequests] = useState([])
  const [myCode, setMyCode] = useState('')
  const [inputCode, setInputCode] = useState('')
  const [addMsg, setAddMsg] = useState('')
  const [selected, setSelected] = useState(null)

  const headers = { 'Content-Type': 'application/json', 'Authorization': `Bearer ${auth.token}` }

  const loadFriends = async () => {
    const res = await fetch('/api/friends', { headers })
    const data = await res.json()
    setFriends(data.friends || [])
  }

  const loadRequests = async () => {
    const res = await fetch('/api/friends/requests', { headers })
    const data = await res.json()
    setRequests(data.requests || [])
  }

  const loadInviteCode = async () => {
    const res = await fetch('/api/invite-code', { headers })
    const data = await res.json()
    setMyCode(data.code || '')
  }

  useEffect(() => { loadFriends(); loadRequests(); loadInviteCode() }, [])

  // 收到好友事件（friendTick 变化）时重新拉取申请与好友列表。跳过首次挂载，避免与上面的初始加载重复。
  useEffect(() => {
    if (friendTick === undefined || friendTick === 0) return
    loadRequests(); loadFriends()
  }, [friendTick])

  const refreshCode = async () => {
    const res = await fetch('/api/invite-code/refresh', { method: 'POST', headers })
    const data = await res.json()
    setMyCode(data.code || '')
  }

  const copyCode = () => {
    navigator.clipboard.writeText(myCode)
    setAddMsg('已复制')
    setTimeout(() => setAddMsg(''), 2000)
  }

  const addByCode = async () => {
    if (!inputCode.trim()) return
    const res = await fetch('/api/friends/add-by-code', { method: 'POST', headers, body: JSON.stringify({ code: inputCode.trim() }) })
    const data = await res.json()
    if (data.error) { setAddMsg(data.error) }
    else { setAddMsg(`已向 ${data.username} 发送好友申请`); setInputCode('') }
    setTimeout(() => setAddMsg(''), 3000)
  }

  const handleRequest = async (id, accept) => {
    await fetch('/api/friends/handle', { method: 'POST', headers, body: JSON.stringify({ request_id: id, accept }) })
    loadRequests()
    if (accept) loadFriends()
  }

  const startChat = async (friend) => {
    // 查是否已有单聊会话；有则用真 cid，没有则进入草稿会话（cid=0，靠 peer_id 发首条）
    const res = await fetch('/api/conversations/dm', { method: 'POST', headers, body: JSON.stringify({ peer_id: friend.id }) })
    const data = await res.json()
    const cid = data.conversationId || data.conversation_id || 0
    if (onStartChat) onStartChat(cid, friend.username, friend.id)
  }

  const deleteFriend = async (friend) => {
    if (!confirm(`删除好友「${friend.username}」？将同时删除与 TA 的聊天会话和记录。`)) return
    const res = await fetch('/api/friends/delete', { method: 'POST', headers, body: JSON.stringify({ friend_id: friend.id }) })
    const data = await res.json().catch(() => ({}))
    if (data.error) { setAddMsg(data.error); setTimeout(() => setAddMsg(''), 3000); return }
    setFriends(prev => prev.filter(u => u.id !== friend.id))
    setSelected(null)
    if (onFriendDeleted) onFriendDeleted(friend) // 通知外层从会话列表移除对应 dm
  }

  return (
    <div className="flex h-full">
      {/* 左栏：平铺列表 */}
      <div className="w-64 border-r border-[#d6d6d6] flex flex-col h-full overflow-y-auto">
        {/* 邀请码区域 */}
        <div className="px-4 py-3 bg-[#ededed] border-b border-[#d6d6d6]">
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-500">我的邀请码:</span>
            <span className="font-mono font-bold text-sm tracking-wider text-gray-900">{myCode}</span>
            <button onClick={copyCode} className="text-[10px] px-1.5 py-0.5 bg-[#07c160] text-white rounded hover:bg-[#06ae56]">复制</button>
            <button onClick={refreshCode} className="text-[10px] px-1.5 py-0.5 bg-gray-200 text-gray-600 rounded hover:bg-gray-300">刷新</button>
          </div>
          <div className="flex gap-1.5 mt-2">
            <input
              className="flex-1 px-2 py-1.5 rounded bg-white outline-none text-xs border border-[#e0e0e0] focus:border-[#07c160] transition uppercase tracking-wider font-mono"
              placeholder="输入好友邀请码"
              value={inputCode}
              onChange={e => setInputCode(e.target.value.toUpperCase())}
              maxLength={6}
              onKeyDown={e => e.key === 'Enter' && addByCode()}
            />
            <button onClick={addByCode} className="px-2.5 py-1.5 bg-[#07c160] text-white rounded text-xs hover:bg-[#06ae56]">添加</button>
          </div>
          {addMsg && <p className="text-[10px] text-gray-600 mt-1">{addMsg}</p>}
        </div>

        {/* 好友申请 */}
        {requests.length > 0 && (
          <>
            <div className="px-4 py-2 bg-[#ededed]">
              <span className="text-xs text-gray-500">好友申请 · {requests.length}</span>
            </div>
            {requests.map(r => {
              const name = r.fromUsername || r.from_username || '?'
              const isPending = (r.status || '') === 'pending'
              return (
                <div key={`req-${r.id}`} onClick={() => setSelected({ type: 'request', data: r })}
                  className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors border-b border-[#ececec] ${selected?.type === 'request' && selected.data.id === r.id ? 'bg-[#c9c9c9]' : 'bg-white hover:bg-[#d8d8d8]'}`}>
                  <div className="relative">
                    <div className="w-10 h-10 rounded-md flex items-center justify-center text-white text-sm font-bold" style={{ background: getColor(name) }}>
                      {name[0]?.toUpperCase()}
                    </div>
                    {isPending && <span className="absolute -top-1 -right-1 w-3 h-3 bg-[#f55c5c] rounded-full"></span>}
                  </div>
                  <div className="flex-1 min-w-0">
                    <span className="text-sm text-gray-900 truncate block">{name}</span>
                    <span className="text-xs text-gray-500">{isPending ? '待处理' : '已处理'}</span>
                  </div>
                </div>
              )
            })}
          </>
        )}

        {/* 好友列表 */}
        <div className="px-4 py-2 bg-[#ededed]">
          <span className="text-xs text-gray-500">好友 · {friends.length}</span>
        </div>
        {friends.map(u => (
          <div key={u.id} onClick={() => setSelected({ type: 'friend', data: u })}
            className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors border-b border-[#ececec] ${selected?.type === 'friend' && selected.data.id === u.id ? 'bg-[#c9c9c9]' : 'bg-white hover:bg-[#d8d8d8]'}`}>
            <div className="w-10 h-10 rounded-md flex items-center justify-center text-white text-sm font-bold" style={{ background: getColor(u.username) }}>
              {u.username[0]?.toUpperCase()}
            </div>
            <span className="text-sm text-gray-900">{u.username}</span>
          </div>
        ))}
        {friends.length === 0 && requests.length === 0 && (
          <div className="text-center text-gray-500 text-sm mt-12">还没有好友</div>
        )}
      </div>

      {/* 右栏：详情 */}
      <div className="flex-1 flex flex-col h-full bg-[#f5f5f5]">
        {selected?.type === 'friend' && (
          <div className="flex-1 flex flex-col items-center justify-center">
            <div className="w-20 h-20 rounded-lg flex items-center justify-center text-white text-2xl font-bold mb-4"
              style={{ background: getColor(selected.data.username) }}>
              {selected.data.username[0]?.toUpperCase()}
            </div>
            <h3 className="text-lg font-medium text-gray-900">{selected.data.username}</h3>
            <p className="text-sm text-gray-500 mt-2">已是好友</p>
            <div className="mt-5 flex gap-3">
              <button
                onClick={() => startChat(selected.data)}
                className="px-6 py-2 bg-[#07c160] text-white rounded-md text-sm hover:bg-[#06ae56] transition flex items-center gap-2">
                <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/></svg>
                发消息
              </button>
              <button
                onClick={() => deleteFriend(selected.data)}
                className="px-6 py-2 bg-white text-[#f55c5c] border border-[#f0d0d0] rounded-md text-sm hover:bg-[#fdeeee] transition">
                删除好友
              </button>
            </div>
          </div>
        )}

        {selected?.type === 'request' && (() => {
          const name = selected.data.fromUsername || selected.data.from_username || '?'
          const isPending = (selected.data.status || '') === 'pending'
          return (
            <div className="flex-1 flex flex-col items-center justify-center">
              <div className="w-20 h-20 rounded-lg flex items-center justify-center text-white text-2xl font-bold mb-4"
                style={{ background: getColor(name) }}>
                {name[0]?.toUpperCase()}
              </div>
              <h3 className="text-lg font-medium text-gray-900">{name}</h3>
              {isPending ? (
                <div className="mt-4 flex gap-3">
                  <button onClick={() => handleRequest(selected.data.id, true)} className="px-6 py-2 bg-[#07c160] text-white rounded-md text-sm hover:bg-[#06ae56] transition">接受</button>
                  <button onClick={() => handleRequest(selected.data.id, false)} className="px-6 py-2 bg-gray-200 text-gray-600 rounded-md text-sm hover:bg-gray-300 transition">拒绝</button>
                </div>
              ) : (
                <p className="text-sm text-gray-500 mt-2">已处理</p>
              )}
            </div>
          )
        })()}

        {!selected && (
          <div className="flex-1 flex items-center justify-center text-gray-400 text-sm">选择一个联系人查看详情</div>
        )}
      </div>
    </div>
  )
}
