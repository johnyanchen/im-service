import { useState, useEffect, useRef, useCallback } from 'react'
import Sidebar from './Sidebar'
import Contacts from './Contacts'
import Messages from './Messages'
import CreateGroupModal from './CreateGroupModal'

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
  const [input, setInput] = useState('')
  const wsRef = useRef(null)
  const [online, setOnline] = useState(true)
  const [loadingMsgs, setLoadingMsgs] = useState(false)
  const [noMoreMsgs, setNoMoreMsgs] = useState({}) // {convId: true}
  const [showCreateGroup, setShowCreateGroup] = useState(false)

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
    } catch (e) {
      console.error('sync failed:', e)
    }
  }

  // Load messages for a conversation (initial or load-more)
  const loadMessages = async (convId, beforeId = 0) => {
    setLoadingMsgs(true)
    try {
      let url = `/api/conversations/${convId}/messages?limit=30`
      if (beforeId > 0) url += `&before_id=${beforeId}`
      const res = await fetch(url, { headers: authHeaders })
      const data = await res.json()
      const msgs = (data.messages || []).map(m => ({
        id: m.id, from: m.fromUsername || m.from_username,
        fromId: m.fromId || m.from_id, content: m.content, time: m.createdAt || m.created_at
      }))
      if (msgs.length < 30) {
        setNoMoreMsgs(prev => ({ ...prev, [convId]: true }))
      }
      setMessagesByConv(prev => {
        const existing = prev[convId] || []
        const existingIds = new Set(existing.map(m => m.id))
        if (beforeId > 0) {
          // prepend older messages
          const newMsgs = msgs.filter(m => !existingIds.has(m.id))
          return { ...prev, [convId]: [...newMsgs, ...existing] }
        } else {
          // initial load: merge with any messages already received via push
          const pushOnly = existing.filter(m => !msgs.find(x => x.id === m.id))
          return { ...prev, [convId]: [...msgs, ...pushOnly] }
        }
      })
    } catch (e) {
      console.error('load messages failed:', e)
    }
    setLoadingMsgs(false)
  }

  const loadMoreMessages = (convId) => {
    if (loadingMsgs || noMoreMsgs[convId]) return
    const msgs = messagesByConv[convId] || []
    if (msgs.length === 0) return
    const oldestId = msgs[0].id
    loadMessages(convId, oldestId)
  }

  const handleMessage = useCallback((e) => {
    const msg = JSON.parse(e.data)
    if (msg.type === 'conv_created') {
      const cid = msg.conversation_id
      setConversations(prev => {
        if (prev.some(x => x.conversation_id === cid)) return prev
        return [...prev, {
          conversation_id: cid, unread_count: 0,
          name: msg.conversation_name || `会话 #${cid}`,
          type: msg.conversation_type || 'dm',
          last_msg_content: '', last_msg_from: '',
        }]
      })
      return
    }
    if (msg.type === 'new_message') {
      const cid = msg.conversation_id
      const isSelf = msg.from_id === auth.userId
      setMessagesByConv(prev => {
        const msgs = prev[cid] ? [...prev[cid]] : []
        if (!msgs.find(m => m.id === msg.message_id)) {
          msgs.push({ id: msg.message_id, from: msg.from_username, fromId: msg.from_id, content: msg.content, time: msg.created_at })
        }
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
    setConversations(prev => prev.map(c => c.conversation_id === id ? { ...c, unread_count: 0 } : c))
    // Load messages if not already loaded
    if (!messagesByConv[id] || messagesByConv[id].length === 0) {
      loadMessages(id)
    }
    const msgs = messagesByConv[id]
    if (msgs && msgs.length > 0) {
      const lastId = msgs[msgs.length - 1].id
      fetch(`/api/conversations/${id}/read`, { method: 'POST', headers: authHeaders, body: JSON.stringify({ msg_id: lastId }) })
    }
  }

  const handleGroupCreated = (cid) => {
    setShowCreateGroup(false)
    if (cid) {
      // 会话本身会通过 WS 的 conv_created 事件推下来加入列表；
      // 这里只负责把创建者直接切到新会话。
      setCurrentConvId(cid)
      currentConvIdRef.current = cid
    }
  }

  const startChatWith = (cid, peerName) => {
    if (!cid) return
    // 若列表里还没有（WS 事件可能稍后到），先乐观插入一条，避免空窗
    setConversations(prev => {
      if (prev.some(c => c.conversation_id === cid)) return prev
      return [...prev, { conversation_id: cid, unread_count: 0, name: peerName || `会话 #${cid}`, type: 'dm', last_msg_content: '', last_msg_from: '' }]
    })
    setTab('chat')
    selectConv(cid)
  }

  const currentConv = conversations.find(c => c.conversation_id === currentConvId)
  const msgs = messagesByConv[currentConvId] || []

  return (
    <div className="flex h-screen">
      {/* 最左侧导航栏 */}
      <div className="w-14 bg-[#2e2e2e] flex flex-col items-center py-4 gap-4">
        <div className="flex flex-col items-center gap-1 group relative">
          <div className="w-9 h-9 rounded-md flex items-center justify-center text-white text-xs font-bold" style={{ background: getColor(auth.username) }}>
            {auth.username[0]?.toUpperCase()}
          </div>
          <span className="text-[10px] text-gray-400 max-w-[48px] truncate leading-tight" title={auth.username}>{auth.username}</span>
          {/* hover 显示完整用户名 */}
          <div className="absolute left-12 top-0 z-50 hidden group-hover:block whitespace-nowrap bg-[#1a1a1a] text-white text-xs px-2 py-1 rounded shadow-lg">
            {auth.username}
          </div>
        </div>
        <div className="flex-1 flex flex-col items-center gap-2 mt-4">
          <button onClick={() => setTab('chat')} className={`w-10 h-10 rounded-lg flex items-center justify-center transition ${tab === 'chat' ? 'bg-[#464646]' : 'hover:bg-[#3a3a3a]'}`} title="聊天">
            <svg className="w-5 h-5 text-gray-300" fill="currentColor" viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/></svg>
          </button>
          <button onClick={() => setTab('contacts')} className={`w-10 h-10 rounded-lg flex items-center justify-center transition ${tab === 'contacts' ? 'bg-[#464646]' : 'hover:bg-[#3a3a3a]'}`} title="通讯录">
            <svg className="w-5 h-5 text-gray-300" fill="currentColor" viewBox="0 0 24 24"><path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z"/></svg>
          </button>
        </div>
        <div className="flex flex-col items-center gap-2">
          <span className={`w-2 h-2 rounded-full ${online ? 'bg-[#07c160]' : 'bg-gray-500'}`}></span>
          <button onClick={onLogout} className="w-10 h-10 rounded-lg flex items-center justify-center hover:bg-[#3a3a3a] transition" title="退出">
            <svg className="w-4 h-4 text-gray-400" fill="currentColor" viewBox="0 0 24 24"><path d="M17 7l-1.41 1.41L18.17 11H8v2h10.17l-2.58 2.58L17 17l5-5zM4 5h8V3H4c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h8v-2H4V5z"/></svg>
          </button>
        </div>
      </div>

      {/* 聊天模式 */}
      {tab === 'chat' && (
        <>
          {/* 会话列表 */}
          <div className="w-64 border-r border-[#d6d6d6] bg-[#ededed] flex flex-col">
            <div className="px-3 py-2">
              <button onClick={() => setShowCreateGroup(true)} className="w-full text-xs py-1.5 rounded-md bg-white text-gray-600 hover:bg-gray-100 transition border border-[#e0e0e0]">+ 发起群聊</button>
            </div>
            <Sidebar conversations={conversations} currentConvId={currentConvId} onSelect={selectConv} getColor={getColor} />
          </div>
          {/* 消息区 */}
          <div className="flex-1 flex flex-col bg-[#f0ece3]">
            {currentConv ? (
              <>
                <div className="px-5 py-3 border-b border-[#d6d6d6] bg-[#ededed]">
                  <h2 className="text-sm font-medium text-gray-800">{currentConv.name}</h2>
                </div>
                <Messages msgs={msgs} userId={auth.userId} getColor={getColor} onLoadMore={() => loadMoreMessages(currentConvId)} loading={loadingMsgs} noMore={noMoreMsgs[currentConvId]} />
                <div className="px-4 py-3 border-t border-[#d6d6d6] bg-[#ededed]">
                  <div className="flex gap-2 items-center">
                    <input className="flex-1 px-3 py-2 rounded-md bg-white outline-none text-sm border border-[#e0e0e0] focus:border-[#07c160] transition" placeholder="输入消息..." value={input} onChange={e => setInput(e.target.value)} onKeyDown={e => e.key === 'Enter' && sendMsg()} />
                    <button onClick={sendMsg} className="px-4 py-2 bg-[#07c160] text-white rounded-md text-sm hover:bg-[#06ae56] transition">发送</button>
                  </div>
                </div>
              </>
            ) : (
              <div className="flex-1 flex items-center justify-center text-gray-500 text-sm bg-[#ededed]">选择一个聊天开始</div>
            )}
          </div>
        </>
      )}

      {/* 联系人模式 */}
      {tab === 'contacts' && (
        <div className="flex-1 bg-[#ededed]">
          <Contacts auth={auth} getColor={getColor} onStartChat={startChatWith} />
        </div>
      )}

      {/* 发起群聊弹窗 */}
      {showCreateGroup && (
        <CreateGroupModal
          auth={auth}
          getColor={getColor}
          onClose={() => setShowCreateGroup(false)}
          onCreate={handleGroupCreated}
        />
      )}
    </div>
  )
}