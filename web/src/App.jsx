import { BrowserRouter, Routes, Route } from 'react-router-dom'
import FundList from './pages/FundList.jsx'
import FundDetail from './pages/FundDetail.jsx'
import MarketDashboard from './pages/MarketDashboard.jsx'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<FundList />} />
        <Route path="/fund/:id" element={<FundDetail />} />
        <Route path="/market" element={<MarketDashboard />} />
      </Routes>
    </BrowserRouter>
  )
}
