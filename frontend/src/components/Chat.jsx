import { useState, useEffect, useRef, useCallback } from 'react'
import Sidebar from './Sidebar'
import Contacts from './Contacts'
import Messages from './Messages'

const COLORS = ['#FF6B6B','#4ECDC4','#45B7D1','#96CEB4','#FFEAA7','#DDA0DD','#98D8C8','#F7DC6F','#BB8FCE','#85C1E9']

function getColor(name) {
  let hash = 0
  for (let i = 0; i < (name||'').length; i++) hash = name.charCodeAt(i) + ((hash << 5) - hash)
  return COLORS[Math.abs(hash) % COLORS.length]
}

export default function Chat({ auth, onLogout }) {
  const [tab, setTab] = useState('chat') // 'chat' | 'contacts'
  const [conversations, setConversations] = useState([])
  const [currentConvId, setCurrentConvId] = useState(null)
  const currentConvIdRef = useRef(null)
  const [messagesByConv, setMessagesByConv] = useState({})
  const [users, setUsers] = useState([])
  const [input, setInput] = useState('')
  const wsRef = useRef(null)
  const [online, setOnline] = useState(true)

  const authHeaders = { 'Content-Type': 'application/json', 'Authorization': `Bearer ${auth.token}` }

  const doSync = async (lastSyncAt = 0) => {
    try {
      const res = await fetch(`/api/sync?last_sync_at=${lastSyncAt}`, { headers: authHeaders })
      const data = await res.json()
      if (data.conversations) {
        setConversations(prev => {
          const updated = [...prev]
          data.conversations.forEach(c => {
            const cid = c.conversationId || c.conversation_id
            const conv = { conversation_id: cid, unread_count: c.unreadCount ?? c.unread_count ?? 0,
              name: c.name || `会话 #${cid}`, type: c.type || 'dm', last_msg_content: c.lastMsgContent || c.last_msg_content || '',
              last_msg_from: c.lastMsgFrom || c.last_msg_from || '' }
            const idx = updated.findIndex(x => x.conversation_id === cid)
            if (idx >= 0) Object.assign(updated[idx], conv)
            else updated.push(conv)
          })
          return updated
        })
      }
      if (data.messages) {
        setMessagesByConv(prev => {
          const next = { ...prev }
          data.messages.forEach(m => {
            const cid = m.conversationId || m.conversation_id
            if (!next[cid]) next[cid] = []
            if (!next[cid].find(x => x.id === m.id)) {
              next[cid] = [...next[cid], { id: m.id, from: m.fromUsername || m.from_username,
                fromId: m.fromId || m.from_id, content: m.content, time: m.createdAt || m.created_at }]
            }
          })
          return next
        })
      }
    } catch (e) {
      console.error('sync failed:', e)
    }
  }

  const handleMessage = useCallback((e) => {
    const msg = JSON.parse(e.data)
    if (msg.type === 'new_message') {
      const cid = msg.conversation_id
      const isSelf = msg.from_id === auth.userId
      setMessagesByConv(prev => {
        const msgs = prev[cid] ? [...prev[cid]] : []
        msgs.push({ id: msg.message_id, from: msg.from_username, fromId: msg.from_id, content: msg.content, time: msg.created_at })
        return { ...prev, [cid]: msgs }
      })
      const isViewing = cid === currentConvIdRef.current
      if (!isSelf && isViewing) {
        fetch(`/api/conversations/${cid}/read`, { method: 'POST', headers: authHeaders, body: JSON.stringify({ msg_id: msg.message_id }) })
      }
      setConversations(prev => {
        const exists = prev.some(x => x.conversation_id === cid)
        if (!exists) {
          return [...prev, { conversation_id: cid, unread_count: isSelf ? 0 : 1, name: msg.conversation_name || `会话 #${cid}`, type: msg.conversation_type || 'dm', last_msg_content: msg.content, last_msg_from: msg.from_username }]
        }
        return prev.map(c => {
          if (c.conversation_id !== cid) return c
          const unread = (isSelf || isViewing) ? 0 : (c.unread_count + 1)
          return { ...c, last_msg_content: msg.content, last_msg_from: msg.from_username, unread_count: unread }
        })
      })
    }
  }, [auth.token])

  useEffect(() => {
    const ws = new WebSocket(`ws://${location.host}/ws`)
    wsRef.current = ws
    ws.onopen = () => {
      ws.send(JSON.stringify({ token: auth.token }))
      setOnline(true)
      doSync()
    }
    ws.onmessage = handleMessage
    ws.onclose = (e) => {
      setOnline(false)
      if (e.code === 4001) {
        alert('你的账号在其他设备登录，当前已下线')
        onLogout()
      }
    }
    return () => ws.close()
  }, [auth.token, handleMessage])

  const sendMsg = async () => {
    if (!input.trim() || !currentConvId) return
    await fetch('/api/messages', { method: 'POST', headers: authHeaders, body: JSON.stringify({ conversation_id: currentConvId, content: input }) })
    setInput('')
  }

  const selectConv = (id) => {
    setCurrentConvId(id)
    currentConvIdRef.current = id
    setTab('chat')
    setConversations(prev => prev.map(c => c.conversation_id === id ? { ...c, unread_count: 0 } : c))
    const msgs = messagesByConv[id]
    if (msgs && msgs.length > 0) {
      const lastId = msgs[msgs.length - 1].id
      fetch(`/api/conversations/${id}/read`, { method: 'POST', headers: authHeaders, body: JSON.stringify({ msg_id: lastId }) })
    }
  }

  const startChat = async (peerId) => {
    const res = await fetch('/api/conversations/dm', { method: 'POST', headers: authHeaders, body: JSON.stringify({ peer_id: peerId }) })
    const data = await res.json()
    if (data.conversationId || data.conversation_id) {
      setCurrentConvId(data.conversationId || data.conversation_id)
      setTab('chat')
      doSync()
    }
  }

  const createGroup = async () => {
    const name = prompt('群组名称:')
    if (!name) return
    const ids = prompt('成员 ID（逗号分隔）:')
    if (!ids) return
    const res = await fetch('/api/conversations/group', { method: 'POST', headers: authHeaders, body: JSON.stringify({ name, member_ids: ids.split(',').map(Number) }) })
    const data = await res.json()
    if (data.conversationId || data.conversation_id) {
      setCurrentConvId(data.conversationId || data.conversation_id)
      setTab('chat')
      doSync()
    }
  }

  const switchToContacts = async () => {
    setTab('contacts')
    try {
      const res = await fetch('/api/users', {
        headers: { 'Authorization': `Bearer ${auth.token}` },
      })
      const data = await res.json()
      if (data.users) setUsers(data.users)
    } catch (e) {
      console.error('fetch users failed:', e)
    }
  }

  const currentConv = conversations.find(c => c.conversation_id === currentConvId)
  const msgs = messagesByConv[currentConvId] || []

  return (
    <div className="flex h-screen bg-gray-50">
      <div className="w-72 border-r border-gray-200 bg-white flex flex-col h-screen">
        {/* 顶部用户信息 */}
        <div className="px-4 py-4 border-b border-gray-100">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-full flex items-center justify-center text-white text-xs font-bold" style={{ background: getColor(auth.username) }}>
              {auth.username[0]?.toUpperCase()}
            </div>
            <span className="text-sm font-medium text-gray-800">{auth.username}</span>
            <span className={`w-2 h-2 rounded-full ${online ? 'bg-green-400' : 'bg-gray-300'}`}></span>
            <button onClick={onLogout} className="ml-auto text-xs text-gray-400 hover:text-red-500 transition">退出</button>
          </div>
        </div>

        {/* Tab 切换 */}
        <div className="flex border-b border-gray-100">
          <button onClick={() => setTab('chat')} className={`flex-1 py-2.5 text-xs font-medium transition ${tab === 'chat' ? 'text-blue-500 border-b-2 border-blue-500' : 'text-gray-400 hover:text-gray-600'}`}>
            💬 会话
          </button>
          <button onClick={switchToContacts} className={`flex-1 py-2.5 text-xs font-medium transition ${tab === 'contacts' ? 'text-blue-500 border-b-2 border-blue-500' : 'text-gray-400 hover:text-gray-600'}`}>
            👥 联系人
          </button>
        </div>

        {/* Tab 内容 */}
        {tab === 'chat' ? (
          <>
            <div className="px-4 py-2">
              <button onClick={createGroup} className="w-full text-xs py-1.5 rounded-lg bg-gray-100 text-gray-600 hover:bg-gray-200 transition">+ 新建群聊</button>
            </div>
            <Sidebar
              conversations={conversations}
              currentConvId={currentConvId}
              onSelect={selectConv}
              getColor={getColor}
            />
          </>
        ) : (
          <Contacts users={users} userId={auth.userId} onStartChat={startChat} getColor={getColor} />
        )}
      </div>

      {/* 右侧聊天区 */}
      <div className="flex-1 flex flex-col">
        {currentConv ? (
          <>
            <div className="px-6 py-4 border-b border-gray-200 bg-white/80 backdrop-blur-sm">
              <h2 className="font-semibold text-gray-800">{currentConv.name}</h2>
              <p className="text-xs text-gray-400">{currentConv.type === 'group' ? '群聊' : '私聊'}</p>
            </div>
            <Messages msgs={msgs} userId={auth.userId} getColor={getColor} />
            <div className="px-4 py-3 border-t border-gray-200 bg-white">
              <div className="flex gap-2">
                <input
                  className="flex-1 px-4 py-2.5 rounded-full bg-gray-100 outline-none text-sm"
                  placeholder="输入消息..."
                  value={input}
                  onChange={e => setInput(e.target.value)}
                  onKeyDown={e => e.key === 'Enter' && sendMsg()}
                />
                <button onClick={sendMsg} className="px-5 py-2.5 bg-blue-500 text-white rounded-full text-sm font-medium hover:bg-blue-600 transition">
                  发送
                </button>
              </div>
            </div>
          </>
        ) : (
          <div className="flex-1 flex items-center justify-center text-gray-400 text-sm">
            选择一个会话开始聊天
          </div>
        )}
      </div>
    </div>
  )
}
