import { BrowserRouter, Routes, Route, Navigate, useLocation } from 'react-router-dom'
import Layout from './components/Layout.jsx'
import FundList from './pages/FundList.jsx'
import FundDetail from './pages/FundDetail.jsx'
import NavDetailPage from './pages/NavDetailPage.jsx'
import MarketDashboard from './pages/MarketDashboard.jsx'
import FundComparison from './pages/FundComparison.jsx'
import ComparisonPage from './pages/ComparisonPage.jsx'
import BasisAnalysis from './pages/BasisAnalysis.jsx'
import PortfolioBuilderPage from './pages/PortfolioBuilderPage.jsx'
import PortfolioDetailPage from './pages/PortfolioDetailPage.jsx'
// import CrudeOilComparison from './pages/CrudeOilComparison.jsx'
// import CrudeDataTable from './pages/CrudeDataTable.jsx'
// import NewsPage from './pages/NewsPage.jsx'
import { CompareProvider } from './context/CompareContext.jsx'
import { AuthProvider } from './context/AuthContext.jsx'
import PortfolioListPage from './pages/PortfolioListPage.jsx'
import AdminPage from './pages/AdminPage.jsx'
import PageState from './components/PageState.jsx'

function ComparisonRedirect() { const location = useLocation(); return <Navigate to={'/compare' + location.search} replace /> }

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
      <CompareProvider>
        <Layout>
          <Routes>
            <Route path="/" element={<FundList />} />
            <Route path="/fund/:id" element={<FundDetail />} />
            <Route path="/fund/:id/nav" element={<NavDetailPage />} />
            <Route path="/market" element={<MarketDashboard />} />
            <Route path="/compare" element={<ComparisonPage />} />
            <Route path="/compare/v2" element={<ComparisonRedirect />} />
            <Route path="/portfolios" element={<PortfolioListPage />} />
            <Route path="/admin" element={<AdminPage />} />
            <Route path="*" element={<PageState title="页面不存在">请通过左侧导航进入研究页面。</PageState>} />
            <Route path="/portfolios/new" element={<PortfolioBuilderPage />} />
            <Route path="/portfolios/:id" element={<PortfolioDetailPage />} />
            <Route path="/basis" element={<BasisAnalysis />} />
            {/* <Route path="/crude" element={<CrudeOilComparison />} /> */}
            {/* <Route path="/crude/data" element={<CrudeDataTable />} /> */}
            {/* <Route path="/news" element={<NewsPage />} /> */}
          </Routes>
        </Layout>
      </CompareProvider>
      </AuthProvider>
    </BrowserRouter>
  )
}
