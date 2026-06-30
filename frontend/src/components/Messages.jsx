import { useEffect, useRef } from 'react'

function formatTime(ts) {
  if (!ts) return ''
  const d = new Date(ts)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
}

export default function Messages({ msgs, userId, getColor, onLoadMore, loading, noMore }) {
  const bottomRef = useRef(null)
  const containerRef = useRef(null)
  const prevMsgsLen = useRef(0)

  useEffect(() => {
    // Auto-scroll to bottom only when new messages arrive at the end
    if (msgs.length > prevMsgsLen.current) {
      const isNewAtBottom = prevMsgsLen.current === 0 || msgs.length - prevMsgsLen.current <= 2
      if (isNewAtBottom) {
        bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
      }
    }
    prevMsgsLen.current = msgs.length
  }, [msgs])

  const handleScroll = () => {
    const el = containerRef.current
    if (!el || loading || noMore) return
    if (el.scrollTop < 50) {
      onLoadMore?.()
    }
  }

  return (
    <div ref={containerRef} onScroll={handleScroll} className="flex-1 overflow-y-auto px-4 py-4 space-y-3 bg-[#f0ece3]">
      {loading && <div className="text-center text-xs text-gray-400 py-2">加载中...</div>}
      {noMore && msgs.length > 0 && <div className="text-center text-xs text-gray-400 py-2">没有更多消息了</div>}
      {msgs.map((m, i) => {
        const isMine = m.fromId === userId
        const showTime = i === 0 || (m.time && msgs[i-1]?.time && m.time - msgs[i-1].time > 300000)
        return (
          <div key={m.id || i}>
            {showTime && m.time && (
              <div className="text-center text-[11px] text-gray-500 my-3">{formatTime(m.time)}</div>
            )}
            <div className={`flex items-start gap-2 ${isMine ? 'flex-row-reverse' : ''}`}>
              <div className="w-9 h-9 rounded-md flex items-center justify-center text-white text-xs font-bold shrink-0 mt-0.5" style={{ background: getColor(isMine ? 'me' : m.from) }}>
                {isMine ? '我' : (m.from || '?')[0]?.toUpperCase()}
              </div>
              <div className={`max-w-[65%] flex flex-col ${isMine ? 'items-end' : 'items-start'}`}>
                {!isMine && <span className="text-[11px] text-gray-500 mb-1 ml-1">{m.from}</span>}
                <div className={`relative px-3 py-2 text-sm leading-relaxed rounded-md ${isMine ? 'bg-[#95ec69] text-gray-900' : 'bg-white text-gray-900'}`}>
                  {m.content}
                  <span className={`absolute top-3 w-0 h-0 border-[6px] ${isMine ? '-right-2.5 border-l-[#95ec69] border-r-0 border-t-transparent border-b-transparent' : '-left-2.5 border-r-white border-l-0 border-t-transparent border-b-transparent'}`}></span>
                </div>
              </div>
            </div>
          </div>
        )
      })}
      <div ref={bottomRef} />
    </div>
  )
}
