import { useEffect, useRef } from 'react'

function formatTime(ts) {
  if (!ts) return ''
  const d = new Date(ts)
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`
}

export default function Messages({ msgs, userId, getColor }) {
  const bottomRef = useRef(null)

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [msgs])

  return (
    <div className="flex-1 overflow-y-auto px-6 py-4 space-y-3">
      {msgs.map((m, i) => {
        const isMine = m.fromId === userId
        const showTime = i === 0 || (m.time && msgs[i-1]?.time && m.time - msgs[i-1].time > 300000)
        return (
          <div key={m.id || i}>
            {showTime && m.time && (
              <div className="text-center text-[11px] text-gray-400 my-3">{formatTime(m.time)}</div>
            )}
            <div className={`flex items-end gap-2 ${isMine ? 'flex-row-reverse' : ''}`}>
              {!isMine && (
                <div className="w-7 h-7 rounded-full flex items-center justify-center text-white text-[10px] font-bold shrink-0" style={{ background: getColor(m.from) }}>
                  {(m.from || '?')[0]?.toUpperCase()}
                </div>
              )}
              <div className={`max-w-[70%] ${isMine ? 'items-end' : 'items-start'} flex flex-col`}>
                {!isMine && <span className="text-[11px] text-gray-400 mb-0.5 ml-1">{m.from}</span>}
                <div className={`px-3.5 py-2 rounded-2xl text-sm leading-relaxed ${isMine ? 'bg-blue-500 text-white rounded-br-sm' : 'bg-gray-200 text-gray-800 rounded-bl-sm'}`}>
                  {m.content}
                </div>
                <span className="text-[10px] text-gray-300 mt-0.5 mx-1">{formatTime(m.time)}</span>
              </div>
            </div>
          </div>
        )
      })}
      <div ref={bottomRef} />
    </div>
  )
}
