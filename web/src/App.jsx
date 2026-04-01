import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout.jsx'
import FundList from './pages/FundList.jsx'
import FundDetail from './pages/FundDetail.jsx'
import MarketDashboard from './pages/MarketDashboard.jsx'
import FundComparison from './pages/FundComparison.jsx'
import ComparisonPage from './pages/ComparisonPage.jsx'
import BasisAnalysis from './pages/BasisAnalysis.jsx'
import CrudeOilComparison from './pages/CrudeOilComparison.jsx'
import CrudeDataTable from './pages/CrudeDataTable.jsx'
import NewsPage from './pages/NewsPage.jsx'
import { CompareProvider } from './context/CompareContext.jsx'

export default function App() {
  return (
    <BrowserRouter>
      <CompareProvider>
        <Layout>
          <Routes>
            <Route path="/" element={<FundList />} />
            <Route path="/fund/:id" element={<FundDetail />} />
            <Route path="/market" element={<MarketDashboard />} />
            <Route path="/compare" element={<FundComparison />} />
            <Route path="/compare/v2" element={<ComparisonPage />} />
            <Route path="/basis" element={<BasisAnalysis />} />
            <Route path="/crude" element={<CrudeOilComparison />} />
            <Route path="/crude/data" element={<CrudeDataTable />} />
            <Route path="/news" element={<NewsPage />} />
          </Routes>
        </Layout>
      </CompareProvider>
    </BrowserRouter>
  )
}
