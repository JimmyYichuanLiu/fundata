import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout.jsx'
import FundList from './pages/FundList.jsx'
import FundDetail from './pages/FundDetail.jsx'
import MarketDashboard from './pages/MarketDashboard.jsx'
import FundComparison from './pages/FundComparison.jsx'
import BasisAnalysis from './pages/BasisAnalysis.jsx'
import CrudeOilComparison from './pages/CrudeOilComparison.jsx'

export default function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<FundList />} />
          <Route path="/fund/:id" element={<FundDetail />} />
          <Route path="/market" element={<MarketDashboard />} />
          <Route path="/compare" element={<FundComparison />} />
          <Route path="/basis" element={<BasisAnalysis />} />
          <Route path="/crude" element={<CrudeOilComparison />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  )
}
