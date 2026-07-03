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
  const [currentDraftPeer, setCurrentDraftPeer] = useState(null) // 选中的草稿会话对应的 peer_id
  const currentConvIdRef = useRef(null)
  const tabRef = useRef('chat') // 供 WS 回调读取当前 tab，避免闭包过期
  // 每个会话「已计入未读的最大 message_id」水位。sync 与推送共用它做去重计数：
  // 只有 message_id 超过水位的消息才 +1，保证 sync/推送乱序或重复到达都不会多算漏算。
  const unreadHiRef = useRef({}) // { [convId]: maxCountedMsgId }
  const [messagesByConv, setMessagesByConv] = useState({})
  const [input, setInput] = useState('')
  const [sendError, setSendError] = useState('') // 发送失败提示（如"对方不是你的好友"）
  const wsRef = useRef(null)
  const [online, setOnline] = useState(true)
  const [loadingMsgs, setLoadingMsgs] = useState(false)
  const [noMoreMsgs, setNoMoreMsgs] = useState({}) // {convId: true}
  const [showCreateGroup, setShowCreateGroup] = useState(false)
  const [friendTick, setFriendTick] = useState(0) // 收到好友事件时 +1，通知 Contacts 重新拉取
  const [friendUnread, setFriendUnread] = useState(0) // 未在联系人 tab 时累积的新申请数，用于红点

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
            const serverUnread = c.unreadCount ?? c.unread_count ?? 0
            const serverLastMsgId = c.lastMsgId ?? c.last_msg_id ?? 0
            // sync 的 unread 是「截至 serverLastMsgId 的权威未读」。之后靠推送在此基础上 +1，
            // 因此把水位推进到 serverLastMsgId：超过它的推送才会被计数，避免与 sync 重复。
            // 若某条更新的消息已被推送先行计数（水位已更高），保留较高水位，别回退。
            const idx = updated.findIndex(x => x.conversation_id === cid)
            const prevHi = unreadHiRef.current[cid] || 0
            // sync 已经把 serverLastMsgId 及之前的都算进 serverUnread，
            // 但如果推送已经领先（prevHi > serverLastMsgId），说明有 sync 尚未反映的新消息，
            // 那部分未读要在 serverUnread 之外额外保留，用当前显示值兜底取 max。
            const conv = {
              conversation_id: cid,
              name: c.name || `会话 #${cid}`, type: c.type || 'dm',
              last_msg_content: c.lastMsgContent || c.last_msg_content || '',
              last_msg_from: c.lastMsgFrom || c.last_msg_from || '',
            }
            if (idx >= 0) {
              const cur = updated[idx].unread_count || 0
              // 推送已领先于 sync 快照：以现有显示值为准（它含 sync 之后到达的消息）；
              // 否则用服务端权威值。取 max 双保险，杜绝红点被 sync 抹回。
              conv.unread_count = prevHi > serverLastMsgId ? Math.max(cur, serverUnread) : serverUnread
              Object.assign(updated[idx], conv)
            } else {
              conv.unread_count = serverUnread
              updated.push(conv)
            }
            if (serverLastMsgId > prevHi) unreadHiRef.current[cid] = serverLastMsgId
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
          // initial load: 直接替换，以服务端为准（水位过滤后的结果）
          return { ...prev, [convId]: msgs }
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
    if (msg.type === 'friend_event') {
      // 好友申请/同意/拒绝：触发 Contacts 重新拉取申请/好友列表。
      setFriendTick(t => t + 1)
      // 收到新申请且不在联系人 tab 时，累积红点。
      if (msg.action === 'request' && tabRef.current !== 'contacts') {
        setFriendUnread(n => n + 1)
      }
      return
    }
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
      // 是否该为这条消息 +1：只有它超过本会话未读水位、且不是自己发的、且没在看该会话时才计数。
      // 超过水位就把水位推进到这条 id，杜绝同一条消息被 sync 和推送重复计数。
      const prevHi = unreadHiRef.current[cid] || 0
      const isNewForUnread = msg.message_id > prevHi
      if (isNewForUnread) unreadHiRef.current[cid] = msg.message_id
      // 正在看或自己发的，视为已读，推进已读水位到这条，避免之后 sync 又把它算成未读。
      if (isSelf || isViewing) {
        if (msg.message_id > (unreadHiRef.current[cid] || 0)) unreadHiRef.current[cid] = msg.message_id
      }
      setConversations(prev => {
        const exists = prev.some(x => x.conversation_id === cid)
        const shouldBump = isNewForUnread && !isSelf && !isViewing
        if (!exists) {
          return [...prev, { conversation_id: cid, unread_count: shouldBump ? 1 : 0, name: msg.conversation_name || `会话 #${cid}`, type: msg.conversation_type || 'dm', last_msg_content: msg.content, last_msg_from: msg.from_username }]
        }
        return prev.map(c => {
          if (c.conversation_id !== cid) return c
          const unread = (isSelf || isViewing) ? 0 : (shouldBump ? c.unread_count + 1 : c.unread_count)
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
    if (!input.trim() || !currentConv) return
    const text = input
    setInput('')
    setSendError('')
    // 草稿单聊会话（cid=0）：带 peer_id 让后端惰性建会话；否则带 conversation_id
    const body = currentConv.draft
      ? { peer_id: currentConv.peer_id, content: text }
      : { conversation_id: currentConv.conversation_id, content: text }
    let res, data
    try {
      res = await fetch('/api/messages', { method: 'POST', headers: authHeaders, body: JSON.stringify(body) })
      data = await res.json().catch(() => ({}))
    } catch {
      setInput(text) // 网络错误：回填输入，别丢用户已打的字
      setSendError('发送失败，请检查网络')
      return
    }
    // 后端拒绝（如删好友后不再是好友）：显示错误、回填输入，不上屏。
    if (!res.ok || data.error) {
      setInput(text)
      setSendError(data.error || '发送失败')
      return
    }
    const realCid = data.conversationId || data.conversation_id
    const msgId = data.messageId || data.message_id
    const createdAt = data.createdAt || data.created_at
    // 发送者不再收到自己消息的 WS 自推，改由这里用 HTTP 响应本地上屏。
    if (realCid && msgId) {
      const mine = { id: msgId, from: auth.username, fromId: auth.userId, content: text, time: createdAt }
      setMessagesByConv(prev => {
        const list = prev[realCid] ? [...prev[realCid]] : []
        if (!list.find(m => m.id === msgId)) list.push(mine)
        return { ...prev, [realCid]: list }
      })
      // 自己发的即已读，推进未读水位，避免随后的 sync 把它算成未读。
      if (msgId > (unreadHiRef.current[realCid] || 0)) unreadHiRef.current[realCid] = msgId
      setConversations(prev => prev.map(c =>
        c.conversation_id === realCid
          ? { ...c, last_msg_content: text, last_msg_from: auth.username, unread_count: 0 }
          : c
      ))
    }
    // 首条发完，后端回填真实 cid：把草稿窗口换成真会话
    if (currentConv.draft && realCid) {
      setConversations(prev => prev.map(c =>
        (c.draft && c.peer_id === currentConv.peer_id)
          ? { ...c, conversation_id: realCid, draft: false }
          : c
      ))
      setMessagesByConv(prev => {
        if (!prev[0]) return prev
        // 合并草稿窗口(cid=0)下已上屏的消息到真会话，去重。
        const merged = [...(prev[realCid] || [])]
        for (const m of prev[0]) if (!merged.find(x => x.id === m.id)) merged.push(m)
        const next = { ...prev, [realCid]: merged }
        delete next[0]
        return next
      })
      setCurrentConvId(realCid)
      currentConvIdRef.current = realCid
      setCurrentDraftPeer(null)
    }
  }

  const selectConv = (id) => {
    setCurrentConvId(id)
    currentConvIdRef.current = id
    setCurrentDraftPeer(null)
    setConversations(prev => prev.map(c => c.conversation_id === id ? { ...c, unread_count: 0 } : c))
    if (!id) return // 草稿会话（id=0）无历史可拉    // Load messages if not already loaded
    loadMessages(id)
    const msgs = messagesByConv[id]
    if (msgs && msgs.length > 0) {
      const lastId = msgs[msgs.length - 1].id
      // 点开即已读：推进未读水位到最后一条，避免随后的 sync 又把它们算成未读。
      if (lastId > (unreadHiRef.current[id] || 0)) unreadHiRef.current[id] = lastId
      fetch(`/api/conversations/${id}/read`, { method: 'POST', headers: authHeaders, body: JSON.stringify({ msg_id: lastId }) })
    }
  }

  // Sidebar 点击：草稿会话选中 peer，真会话走 selectConv
  const onSelectConv = (c) => {
    if (c.draft) {
      setCurrentConvId(0)
      currentConvIdRef.current = 0
      setCurrentDraftPeer(c.peer_id)
    } else {
      selectConv(c.conversation_id)
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

  const startChatWith = (cid, peerName, peerId) => {
    setTab('chat')
    tabRef.current = 'chat'
    if (cid) {
      // 已有真会话：正常插入并选中。记录 peer_id/peerName 便于删好友时本地移除。
      setConversations(prev => {
        if (prev.some(c => c.conversation_id === cid)) return prev
        return [...prev, { conversation_id: cid, peer_id: peerId, unread_count: 0, name: peerName || `会话 #${cid}`, type: 'dm', last_msg_content: '', last_msg_from: '' }]
      })
      selectConv(cid)
      return
    }
    // 草稿会话：本地态，cid=0，还没落库。发首条时用 peer_id 惰性建会话。
    // 同一 peer 已有草稿则复用，避免重复。
    setConversations(prev => {
      const existing = prev.find(c => c.draft && c.peer_id === peerId)
      if (existing) return prev
      return [...prev, { conversation_id: 0, draft: true, peer_id: peerId, unread_count: 0, name: peerName || '新会话', type: 'dm', last_msg_content: '', last_msg_from: '' }]
    })
    setCurrentConvId(0)
    currentConvIdRef.current = 0
    setCurrentDraftPeer(peerId)
  }

  // 删好友后：从会话列表移除与该好友的 dm 会话（本地态；后端已删发起方视图行）。
  // 优先按 peer_id 匹配，回退按用户名匹配（sync 下来的 dm 会话名就是对方用户名）。
  const handleFriendDeleted = (friend) => {
    setConversations(prev => prev.filter(c => {
      if (c.type !== 'dm') return true
      const byPeer = c.peer_id && friend.id && c.peer_id === friend.id
      const byName = c.name && c.name === friend.username
      return !(byPeer || byName)
    }))
    setCurrentConvId(cur => {
      const removed = conversations.find(c => c.type === 'dm' && ((c.peer_id && c.peer_id === friend.id) || c.name === friend.username))
      if (removed && removed.conversation_id === cur) {
        currentConvIdRef.current = null
        return null
      }
      return cur
    })
  }

  const currentConv = currentConvId
    ? conversations.find(c => c.conversation_id === currentConvId)
    : conversations.find(c => c.draft && c.peer_id === currentDraftPeer)
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
          <button onClick={() => { setTab('chat'); tabRef.current = 'chat' }} className={`w-10 h-10 rounded-lg flex items-center justify-center transition ${tab === 'chat' ? 'bg-[#464646]' : 'hover:bg-[#3a3a3a]'}`} title="聊天">
            <svg className="w-5 h-5 text-gray-300" fill="currentColor" viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2zm0 14H6l-2 2V4h16v12z"/></svg>
          </button>
          <button onClick={() => { setTab('contacts'); tabRef.current = 'contacts'; setFriendUnread(0) }} className={`relative w-10 h-10 rounded-lg flex items-center justify-center transition ${tab === 'contacts' ? 'bg-[#464646]' : 'hover:bg-[#3a3a3a]'}`} title="通讯录">
            <svg className="w-5 h-5 text-gray-300" fill="currentColor" viewBox="0 0 24 24"><path d="M12 12c2.21 0 4-1.79 4-4s-1.79-4-4-4-4 1.79-4 4 1.79 4 4 4zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z"/></svg>
            {friendUnread > 0 && <span className="absolute -top-0.5 -right-0.5 min-w-[16px] h-4 px-1 bg-[#f55c5c] text-white text-[10px] leading-4 text-center rounded-full">{friendUnread > 99 ? '99+' : friendUnread}</span>}
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
            <Sidebar conversations={conversations} currentConvId={currentConvId} currentDraftPeer={currentDraftPeer} onSelect={onSelectConv} getColor={getColor} />
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
                  {sendError && (
                    <div className="flex items-center gap-2 mb-2 px-3 py-2 rounded-md bg-[#fdeeee] border border-[#f5d0d0] text-[#d94a4a] text-xs">
                      <svg className="w-3.5 h-3.5 shrink-0" fill="currentColor" viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-2h2v2zm0-4h-2V7h2v6z"/></svg>
                      <span className="flex-1">{sendError}</span>
                      <button onClick={() => setSendError('')} className="shrink-0 text-[#d94a4a] hover:text-[#b83636] leading-none" title="关闭">
                        <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 24 24"><path d="M19 6.41L17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z"/></svg>
                      </button>
                    </div>
                  )}
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
          <Contacts auth={auth} getColor={getColor} onStartChat={startChatWith} friendTick={friendTick} onFriendDeleted={handleFriendDeleted} />
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