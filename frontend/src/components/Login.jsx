import { useState } from 'react'

export default function Login({ onAuth }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')

  const submit = async (endpoint) => {
    setError('')
    const res = await fetch(`/api/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    })
    const data = await res.json()
    if (data.token) onAuth({ token: data.token, userId: data.user_id, username })
    else setError(data.error || '操作失败')
  }

  return (
    <div className="flex items-center justify-center h-screen bg-[#111111]">
      <div className="w-80 p-8">
        <div className="flex justify-center mb-8">
          <div className="w-16 h-16 rounded-2xl bg-[#07c160] flex items-center justify-center">
            <svg className="w-9 h-9 text-white" viewBox="0 0 24 24" fill="currentColor">
              <path d="M8.5 13.5a1.5 1.5 0 100-3 1.5 1.5 0 000 3zM15.5 13.5a1.5 1.5 0 100-3 1.5 1.5 0 000 3zM12 2C6.477 2 2 6.477 2 12c0 2.05.62 3.955 1.68 5.54L2 22l4.46-1.68A9.94 9.94 0 0012 22c5.523 0 10-4.477 10-10S17.523 2 12 2z"/>
            </svg>
          </div>
        </div>
        <h1 className="text-xl font-medium text-center mb-8 text-white">微信</h1>
        <input
          className="w-full px-0 py-3 mb-4 bg-transparent border-b border-gray-700 outline-none text-sm text-white placeholder-gray-500 focus:border-[#07c160] transition"
          placeholder="请输入用户名"
          value={username}
          onChange={e => setUsername(e.target.value)}
        />
        <input
          className="w-full px-0 py-3 mb-6 bg-transparent border-b border-gray-700 outline-none text-sm text-white placeholder-gray-500 focus:border-[#07c160] transition"
          type="password"
          placeholder="请输入密码"
          value={password}
          onChange={e => setPassword(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && submit('login')}
        />
        {error && <p className="text-red-400 text-xs mb-3 text-center">{error}</p>}
        <button onClick={() => submit('login')} className="w-full py-3 bg-[#07c160] text-white rounded-md text-sm font-medium hover:bg-[#06ae56] transition mb-3">登录</button>
        <button onClick={() => submit('register')} className="w-full py-3 bg-transparent text-gray-400 border border-gray-700 rounded-md text-sm hover:border-gray-500 transition">注册</button>
      </div>
    </div>
  )
}
