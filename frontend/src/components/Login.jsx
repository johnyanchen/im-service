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
    <div className="flex items-center justify-center h-screen bg-gray-50">
      <div className="w-80 p-8 bg-white rounded-2xl shadow-sm">
        <h1 className="text-2xl font-semibold text-center mb-6">IM Service</h1>
        <input
          className="w-full px-4 py-3 mb-3 rounded-xl bg-gray-100 outline-none text-sm"
          placeholder="用户名"
          value={username}
          onChange={e => setUsername(e.target.value)}
        />
        <input
          className="w-full px-4 py-3 mb-4 rounded-xl bg-gray-100 outline-none text-sm"
          type="password"
          placeholder="密码"
          value={password}
          onChange={e => setPassword(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && submit('login')}
        />
        {error && <p className="text-red-500 text-xs mb-3 text-center">{error}</p>}
        <div className="flex gap-2">
          <button onClick={() => submit('login')} className="flex-1 py-3 bg-blue-500 text-white rounded-xl text-sm font-medium hover:bg-blue-600 transition">登录</button>
          <button onClick={() => submit('register')} className="flex-1 py-3 bg-gray-200 text-gray-700 rounded-xl text-sm font-medium hover:bg-gray-300 transition">注册</button>
        </div>
      </div>
    </div>
  )
}
